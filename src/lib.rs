use rusoto_s3::S3Client;
use rusoto_s3::{DeleteObjectsRequest, Delete, ObjectIdentifier};
pub use rusoto_core::region::Region;
use rusoto_core::RusotoError;
use rusoto_s3::S3 as S3Trait;
use rusoto_s3::{ListObjectsV2Request, ListObjectsV2Output};
use rusoto_s3::Object as S3Object;
use rusoto_s3::StreamingBody;
use log::{trace, debug, error};
use itertools::unfold;
use std::time::Duration;
use std::io::Read;
use std::error::Error;
use std::fmt;
use std::collections::HashMap;
//use std::hash::{Hash, Hasher};
use ensure::{Absent, Present, Ensure, Meet, External, ExternalState};
use ensure::CheckEnsureResult::*;
use either::Either;
use Either::*;
use problem::prelude::*;
use itertools::Itertools;

pub trait Captures1<'i> {}
impl<'i, T> Captures1<'i> for T {}

pub trait Captures2<'i> {}
impl<'i, T> Captures2<'i> for T {}

const DEFAULT_PART_SIZE: usize = 10 * 1024 * 1024; // note that max parts is 10k = 100_000 MiB

#[derive(Debug)]
pub enum S3SyncError {
    RusotoError(RusotoError<Box<dyn Error + 'static>>),
    IoError(std::io::Error),
    NoBodyError,
}

impl fmt::Display for S3SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3SyncError::RusotoError(err) => match err {
                RusotoError::Service(err) => write!(f, "AWS service-specific error occurred: {}", err),
                RusotoError::HttpDispatch(err) => write!(f, "AWS error occurred dispatching the HTTP request: {}", err),
                RusotoError::Credentials(err) => write!(f, "AWS error was encountered with credentials: {}", err),
                RusotoError::Validation(err) => write!(f, "AWS validation error occurred: {}", err),
                RusotoError::ParseError(err) => write!(f, "AWS error occurred parsing the response payload: {}", err),
                RusotoError::Unknown(err) => write!(f, "unknown AWS error occurred: {:?}", err),
            },
            S3SyncError::IoError(_) => write!(f, "local I/O error"),
            S3SyncError::NoBodyError => write!(f, "expected body but found none"),
        }
    }
}

impl Error for S3SyncError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            S3SyncError::RusotoError(_) => None,
            S3SyncError::IoError(err) => Some(err),
            S3SyncError::NoBodyError => None,
        }
    }
}

impl<T: Error + 'static> From<RusotoError<T>> for S3SyncError {
    fn from(err: RusotoError<T>) -> S3SyncError {
        match err {
            RusotoError::Service(err) => S3SyncError::RusotoError(RusotoError::Service(Box::new(err))),
            RusotoError::HttpDispatch(err) => S3SyncError::RusotoError(RusotoError::HttpDispatch(err)),
            RusotoError::Credentials(err) => S3SyncError::RusotoError(RusotoError::Credentials(err)),
            RusotoError::Validation(err) => S3SyncError::RusotoError(RusotoError::Validation(err)),
            RusotoError::ParseError(err) => S3SyncError::RusotoError(RusotoError::ParseError(err)),
            RusotoError::Unknown(err) => S3SyncError::RusotoError(RusotoError::Unknown(err)),
        }
    }
}

impl From<std::io::Error> for S3SyncError {
    fn from(err: std::io::Error) -> S3SyncError {
        S3SyncError::IoError(err)
    }
}

/// Represents object in a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object<'b> {
    bucket: &'b Present<Bucket>,
    key: String
}

//impl<'b> Hash for Object<'b> {
//    fn hash<H: Hasher>(&self, state: &mut H) {
//        self.key.hash(state)
//    }
//}

impl External for Object<'_> {}

impl<'b> Object<'b> {
    pub fn from_key(bucket: &Present<Bucket>, key: String) -> Object {
        Object {
            bucket,
            key
        }
    }

    fn from_s3_object(bucket: &Present<Bucket>, object: S3Object) -> Object {
        Object::from_key(bucket, object.key.expect("S3 object has no key!"))
    }

    pub fn bucket(&self) -> &Present<Bucket> {
        self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Represents S3 bucket.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Bucket {
    name: String
}

impl External for Bucket {}

impl Bucket {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<String> for Bucket {
    fn from(name: String) -> Bucket {
        Bucket {
            name
        }
    }
}

pub struct PaginationIter<RQ, RS, SSA, GSA, FF, E> where RQ: Clone, SSA: Fn(&mut RQ, String), GSA: Fn(&RS) -> Option<String>, FF: Fn(RQ) -> Result<RS, E> {
    request: RQ,
    // function that returns request parametrised to fetch next page
    set_start_after: SSA,
    get_start_after: GSA,
    fetch: FF,
    done: bool
}

impl<RQ, RS, SSA, GSA, FF, E> Iterator for PaginationIter<RQ, RS, SSA, GSA, FF, E> where RQ: Clone, SSA: Fn(&mut RQ, String), GSA: Fn(&RS) -> Option<String>, FF: Fn(RQ) -> Result<RS, E> {
    type Item = Result<RS, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None
        }
        Some((self.fetch)(self.request.clone()).map(|response| {
            if let Some(start_after) = (self.get_start_after)(&response) {
                (self.set_start_after)(&mut self.request, start_after);
            } else {
                self.done = true;
            }
            response
        }))
    }
}


#[derive(Debug)]
pub enum TransferStatus {
    Init,
    Progress,
    Done,
    Failed,
}

impl Default for TransferStatus {
    fn default() -> Self {
        TransferStatus::Init
    }
}

#[derive(Debug, Default)]
pub struct TransferProgress {
    /// Status of the transfer.
    pub status: TransferStatus,
    /// Number of buffers or parts transferred.
    pub buffers: u16,
    /// Number of bytes transferred since last progress.
    pub bytes: u64,
    /// Total number of bytes transferred.
    pub bytes_total: u64,
}

//TODO: Configurable timeouts
//TODO: Max upload size info

/// Wrapper of Rusoto S3 client that adds some high level imperative and declarative operations on
/// S3 buckets and objects.
pub struct S3 {
    client: S3Client,
    part_size: usize,
    on_upload_progress: Option<Box<dyn Fn(&TransferProgress)>>,
}

impl S3 {
    /// Crate new Rusoto based S3 client.
    pub fn new(region: Region, part_size: impl Into<Option<usize>>) -> S3 {
        S3 {
            client: S3Client::new(region),
            part_size: part_size.into().unwrap_or(DEFAULT_PART_SIZE),
            on_upload_progress: None,
        }
    }

    /// Maximum size of the multipart upload part.
    ///
    /// Useful to set up other I/O buffers accordingly.
    pub fn part_size(&self) -> usize {
        self.part_size
    }


    /// Set callback on body upload progress.
    pub fn on_upload_progress(&mut self, callback: impl Fn(&TransferProgress) + 'static) -> Option<Box<dyn Fn(&TransferProgress)>> {
        let ret = self.on_upload_progress.take();
        self.on_upload_progress = Some(Box::new(callback));
        ret
    }

    pub fn check_bucket_exists(&self, bucket: Bucket) -> Result<Either<Present<Bucket>, Absent<Bucket>>, S3SyncError> {
        use rusoto_s3::HeadBucketRequest;
        use rusoto_s3::HeadBucketError;
        use rusoto_core::request::BufferedHttpResponse;

        let res = self.client.head_bucket(HeadBucketRequest {
            bucket: bucket.name.clone(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(10)).sync();
        trace!("Head bucket response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(bucket))),
            Err(RusotoError::Service(HeadBucketError::NoSuchBucket(_))) => Ok(Right(Absent(bucket))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(bucket))),
            Err(err) => Err(err.into())
        }
    }

    pub fn check_object_exists<'s, 'b>(&'s self, object: Object<'b>) -> Result<Either<Present<Object<'b>>, Absent<Object<'b>>>, S3SyncError> {
        use rusoto_s3::HeadObjectRequest;
        use rusoto_s3::HeadObjectError;
        use rusoto_core::request::BufferedHttpResponse;

        let res = self.client.head_object(HeadObjectRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(10)).sync();
        trace!("Head response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(object))),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(Right(Absent(object))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(object))),
            Err(err) => Err(err.into())
        }
    }

    pub fn list_objects<'b, 's: 'b>(&'s self, bucket: &'b Present<Bucket>, prefix: String) -> impl Iterator<Item = Result<Present<Object<'b>>, S3SyncError>> + Captures1<'s> + Captures2<'b> {
        let client = &self.client;
        let pages = PaginationIter {
            request: ListObjectsV2Request {
                bucket: bucket.name().to_owned(),
                prefix: Some(prefix),
                .. Default::default()
            },
            set_start_after: |request: &mut ListObjectsV2Request, start_after| {
                request.start_after = Some(start_after);
            },
            get_start_after: |response: &ListObjectsV2Output| {
                response.contents.as_ref().and_then(|objects| objects.last().and_then(|last| last.key.as_ref().map(|r| r.clone())))
            },
            fetch: move |request: ListObjectsV2Request| {
                client.list_objects_v2(request).with_timeout(Duration::from_secs(10)).sync().map_err(Into::into)
            },
            done: false
        };

        pages.flat_map(move |response| {
            let mut error = None;
            let mut objects = None;
            match response {
                Err(err) => error = Some(err),
                Ok(output) => objects = output.contents.map(|objects| objects.into_iter()),
            }

            unfold((), move |_| {
                if let Some(error) = error.take() {
                    Some(Err(error))
                } else {
                    objects.as_mut().and_then(|obj| obj.next()).map(|o| Ok(Present(Object::from_s3_object(bucket, o))))
                }
            })
        })
    }

    pub fn get_object_body<'s, 'b>(&'s self, bucket: String, object: &'_ Present<Object<'b>>) -> Result<StreamingBody, S3SyncError> {
        use rusoto_s3::GetObjectRequest;
        self.client.get_object(GetObjectRequest {
            bucket,
            key: object.0.key.to_owned(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(300)).sync()
        .map_err(Into::into)
        .and_then(|output| output.body.ok_or(S3SyncError::NoBodyError))
    }

    //TODO: object does not need to be Absent to update it's body
    //TODO: Option<Options> to set content type, storage class ets
    pub fn put_object_body<'s, 'b>(&'s self, object: Absent<Object<'b>>, mut body: impl Read) -> Result<Present<Object<'b>>, S3SyncError> {
        use rusoto_s3::{CreateMultipartUploadRequest, UploadPartRequest, CompleteMultipartUploadRequest, AbortMultipartUploadRequest, CompletedMultipartUpload, CompletedPart};
        use rusoto_core::ByteStream;

        let upload_id = self.client.create_multipart_upload(CreateMultipartUploadRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            //TODO: content_type, storage_clase etc.
            .. Default::default()
        })
        .with_timeout(Duration::from_secs(300)).sync()?
        .upload_id.expect("no upload ID");

        debug!("Started multipart upload {:?}", upload_id);

        let mut completed_parts = Vec::new();
        let mut progress = TransferProgress::default();

        // Notify progress init
        self.on_upload_progress.as_ref().map(|c| c(&progress));

        let result = || -> Result<_, S3SyncError> {
            let body = &mut body;

            for part_number in 1u16.. {
                //TODO: streaming bufs
                let mut buf = Vec::with_capacity(self.part_size);
                let bytes = body.take(self.part_size as u64).read_to_end(&mut buf)?;

                // Don't create 0 byte parts on EoF
                if bytes == 0 {
                    break
                }

                //let body = ByteStream::new(stream::once(Ok(Bytes::from(buf))));
                let body = ByteStream::from(buf);

                debug!("Uploading part {} ({} bytes)", part_number, bytes);
                let result = self.client.upload_part(UploadPartRequest {
                    body: Some(body),
                    bucket: object.bucket.name.clone(),
                    key: object.key.clone(),
                    part_number: part_number as i64,
                    upload_id: upload_id.clone(),
                    .. Default::default()
                }).with_timeout(Duration::from_secs(300)).sync()?;

                completed_parts.push(CompletedPart {
                    e_tag: result.e_tag,
                    part_number: Some(part_number as i64),
                });

                progress.status = TransferStatus::Progress;
                progress.buffers += 1;
                progress.bytes = bytes as u64;
                progress.bytes_total += bytes as u64;

                // Notify with progress
                self.on_upload_progress.as_ref().map(|c| c(&progress));
            }

            // No parts uploaded
            if completed_parts.is_empty() {
                return Err(S3SyncError::NoBodyError)
            }

            debug!("Multipart upload {:?} complete", upload_id);
            self.client.complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: object.bucket.name.clone(),
                key: object.key.clone(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(completed_parts)
                }),
                .. Default::default()
            }).with_timeout(Duration::from_secs(300)).sync()?;

            // Notify it is done
            progress.status = TransferStatus::Done;
            self.on_upload_progress.as_ref().map(|c| c(&progress));

            Ok(Present(object.0.clone()))
        }();

        if let Err(err) = &result {
            error!("Aborting multipart upload {:?} due to error: {}", upload_id, Problem::from_error_message(err));
            self.client.abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: object.bucket.name.clone(),
                key: object.key.clone(),
                upload_id,
                .. Default::default()
            }).with_timeout(Duration::from_secs(300)).sync().ok_or_log_warn();

            // Notify it is has failed
            progress.status = TransferStatus::Failed;
            self.on_upload_progress.as_ref().map(|c| c(&progress));
        }

        result
    }

    //TODO: This should return iterator since we can be working with unbounded number of objects

    /// Deletes list of objects.
    ///
    /// Delete call does not fail if object does not exist and terefore this method can work with any
    /// `ExternalState` of the object.
    ///
    /// Note that objects can live in different buckets. In this case they will be deleted bucket
    /// by bucket.
    ///
    /// Note that returned results are per batch of delte operations.
    ///
    /// Returns `Err` if one of the batch delete calls failed.
    /// If `Ok` is returned all batch delete calls succedded but each individual object could have
    /// failed to be deleted.
    pub fn delete_objects<'b, 's: 'b>(&'s self, objects: impl IntoIterator<Item = impl ExternalState<Object<'b>>>) -> Result<Vec<Result<Absent<Object<'b>>, (Object<'b>, S3SyncError)>>, S3SyncError> {
        let mut objects = objects.into_iter().map(|o| o.invalidate_state()).peekable();

        std::iter::from_fn(move || {
            let current_bucket_name = if let Some(object) = objects.peek() {
                object.bucket.name.clone()
            } else {
                return None
            };

            Some(objects
                .peeking_take_while({
                    let current_bucket_name = current_bucket_name.clone();
                    move |object| object.bucket.name == current_bucket_name})
                .chunks(1000).into_iter().map(move |chunk| {
                    let mut objects = chunk
                        .into_iter()
                        .map(|o| (o.key.clone(), o))
                        .collect::<HashMap<_, _>>();

                    let object_ids: Vec<ObjectIdentifier> = objects.values().map(|object| ObjectIdentifier {
                        key: object.key.clone(),
                        .. Default::default()
                    }).collect();

                    debug!("Deleting batch of {} objects from S3 bucket '{}'", objects.len(), current_bucket_name);
                    let res = self.client.delete_objects(DeleteObjectsRequest {
                        bucket: current_bucket_name.clone(),
                        delete: Delete {
                            objects: object_ids,
                            .. Default::default()
                        }, .. Default::default()
                    }).with_timeout(Duration::from_secs(60)).sync()?;
                    trace!("Delete response: {:?}", res);

                    let mut failed_objects = Vec::new();

                    if let Some(errors) = res.errors {
                        for error in errors {
                            error!("Error deleting S3 object '{}': {}",
                                error.key.as_ref().map(|s| s.as_str()).unwrap_or("<None>"),
                                error.message.as_ref().map(|s| s.as_str()).unwrap_or("<None>"));

                            // Try the best to get failed objects out of OK objects along with error
                            // message
                            if let (Some(key), Some(error)) = (error.key, error.message) {
                                if let Some(object) = objects.remove(&key) {
                                    failed_objects.push((object, S3SyncError::RusotoError(RusotoError::Validation(error))));
                                }
                            }
                        }
                    }

                    // Result<Vec<Result<,>,>
                    Ok(objects.into_iter()
                        .map(|(_k, o)| Ok(Absent(o)))
                        .chain(failed_objects.into_iter().map(|oe| Err(oe)))
                        .collect::<Vec<Result<_, _>>>())
                })
                .collect::<Result<Vec<Vec<Result<_, _>>>, _>>()
                .map(|ok: Vec<_>| ok.into_iter().flatten().collect::<Vec<Result<_, _>>>()))
                // Option<Result<Vec<Result<,>,>>
        })
        .collect::<Result<Vec<_>,_>>()
        .map(|ok: Vec<_>| ok.into_iter().flatten().collect::<Vec<Result<_, _>>>())
    }

    /// Ensure that object is present in S3 bucket.
    ///
    /// It will call body function to obtain Read object from which the data will be uploaded to S3
    /// if object does not already exist there.
    pub fn object_present<'b, 's: 'b, R: Read + 's, F: FnOnce() -> Result<R, std::io::Error> + 's>(&'s self, object: Object<'b>, body: F) -> impl Ensure<Present<Object<'b>>, EnsureAction = impl Meet<Met = Present<Object<'b>>, Error = S3SyncError> + Captures1<'s> + Captures2<'b>> + Captures1<'s> + Captures2<'b> {
        move || {
            Ok(match self.check_object_exists(object)? {
                Left(present) => Met(present),
                Right(absent) => EnsureAction(move || {
                    self.put_object_body(absent, body()?)
                })
            })
        }
    }

    //TODO: object_absent?
}

#[cfg(feature = "test-s3")]
#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    fn s3_test_bucket() -> Bucket {
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set").into()
    }

    fn test_key() -> String {
        use std::time::SystemTime;
        format!("s3-sync-test/foo-{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros())
    }

    #[test]
    fn test_object_present() {
        use std::io::Cursor;

        let s3 = S3::new(Region::EuWest1, None);
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        s3.object_present(object, move || Ok(body)).ensure().or_failed_to("make object present");
    }

    #[test]
    fn test_object_present_empty_read() {
        use std::io::Cursor;

        let s3 = S3::new(Region::EuWest1, None);
        let body = Cursor::new(b"".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        assert_matches!(s3.object_present(object, move || Ok(body)).ensure(), Err(S3SyncError::NoBodyError));
    }

    #[test]
    fn test_object_present_progress() {
        use std::io::Cursor;

        let mut s3 = S3::new(Region::EuWest1, None);
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        //TODO: actually test state stuff
        s3.on_upload_progress(|_p| ());
        s3.object_present(object, move || Ok(body)).ensure().or_failed_to("make object present");
    }

    #[test]
    fn test_delete_objects() {
        let s3 = S3::new(Region::EuWest1, None);

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let objects = vec![
            Object::from_key(&bucket, "s3-sync-test/foo".to_owned()),
            Object::from_key(&bucket, test_key()),
            Object::from_key(&bucket, test_key()),
        ];

        s3.delete_objects(objects).or_failed_to("delete objects");
    }

    #[test]
    fn test_delete_objects_from_list() {
        let s3 = S3::new(Region::EuWest1, None);

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");

        let objects = s3.list_objects(&bucket, "s3-sync-test/baz".to_owned()).or_failed_to("get list of objects");
        s3.delete_objects(objects).or_failed_to("delete objects");
    }
}
