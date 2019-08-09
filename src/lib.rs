use rusoto_s3::S3Client;
use rusoto_s3::{DeleteObjectsRequest, Delete, ObjectIdentifier};
pub use rusoto_core::region::Region;
use rusoto_core::RusotoError;
use rusoto_s3::S3 as S3Trait;
use rusoto_s3::{ListObjectsV2Request, ListObjectsV2Output};
use rusoto_s3::Object as S3Object;
use rusoto_s3::StreamingBody;
use derive_more::{Display, FromStr};
use log::{debug, info, error};
use itertools::unfold;
use std::time::Duration;
use std::io::Read;
use std::error::Error;
use std::fmt;
use ensure::{Absent, Present, Ensure, Meet};
use ensure::CheckEnsureResult::*;
use either::Either;
use Either::*;
use problem::prelude::*;

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

#[derive(Debug, Display, FromStr, Clone)]
pub struct Object {
    pub key: String
}

impl Object {
    pub fn from_key(key: String) -> Object {
        Object { key }
    }
}

impl From<String> for Object {
    fn from(key: String) -> Object {
        Object::from_key(key)
    }
}

#[derive(Debug, Display, FromStr, Clone)]
pub struct Bucket {
    pub name: String
}

impl From<String> for Bucket {
    fn from(name: String) -> Bucket {
        Bucket {
            name
        }
    }
}

impl From<S3Object> for Object {
    fn from(object: S3Object) -> Object {
        Object::from_key(object.key.expect("S3 object has no key!"))
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
    /// Total number of bytes transferred.
    pub bytes: u64,
}

pub struct S3 {
    client: S3Client,
    part_size: usize,
    on_part_uploaded: Option<Box<dyn Fn(&TransferProgress)>>,
}

impl S3 {
    /// Crate new Rusoto based S3 client.
    pub fn new(region: Region, part_size: impl Into<Option<usize>>) -> S3 {
        S3 {
            client: S3Client::new(region),
            part_size: part_size.into().unwrap_or(DEFAULT_PART_SIZE),
            on_part_uploaded: None,
        }
    }

    /// Maximum size of the multipart upload part.
    ///
    /// Useful to set up other I/O buffers accordingly.
    pub fn part_size(&self) -> usize {
        self.part_size
    }


    /// Set callback on body upload progress.
    pub fn on_part_uploaded(&mut self, callback: Box<dyn Fn(&TransferProgress)>) -> Option<Box<dyn Fn(&TransferProgress)>> {
        let ret = self.on_part_uploaded.take();
        self.on_part_uploaded = Some(callback);
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
        debug!("Head bucket response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(bucket))),
            Err(RusotoError::Service(HeadBucketError::NoSuchBucket(_))) => Ok(Right(Absent(bucket))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(bucket))),
            Err(err) => Err(err.into())
        }
    }

    pub fn check_object_exists(&self, bucket: &Present<Bucket>, object: Object) -> Result<Either<Present<Object>, Absent<Object>>, S3SyncError> {
        use rusoto_s3::HeadObjectRequest;
        use rusoto_s3::HeadObjectError;
        use rusoto_core::request::BufferedHttpResponse;

        let res = self.client.head_object(HeadObjectRequest {
            bucket: bucket.0.name.clone(),
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(10)).sync();
        debug!("Head response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(object))),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(Right(Absent(object))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(object))),
            Err(err) => Err(err.into())
        }
    }

    pub fn list_prefix<'s>(&'s self, bucket: String, prefix: String) -> impl Iterator<Item = Result<Present<Object>, S3SyncError>> + 's {
        let client = &self.client;
        let pages = PaginationIter {
            request: ListObjectsV2Request {
                bucket,
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

        pages.flat_map(|response| {
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
                    objects.as_mut().and_then(|obj| obj.next()).map(|o| Ok(Present(Object::from(o))))
                }
            })
        })
    }

    pub fn get_object_body(&self, bucket: String, object: &Present<Object>) -> Result<StreamingBody, S3SyncError> {
        use rusoto_s3::GetObjectRequest;
        self.client.get_object(GetObjectRequest {
            bucket,
            key: object.0.key.to_owned(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(300)).sync()
        .map_err(Into::into)
        .and_then(|output| output.body.ok_or(S3SyncError::NoBodyError))
    }

    pub fn put_object_body(&self, bucket: &Present<Bucket>, object: Absent<Object>, mut body: impl Read) -> Result<Present<Object>, S3SyncError> {
        use rusoto_s3::{CreateMultipartUploadRequest, UploadPartRequest, CompleteMultipartUploadRequest, AbortMultipartUploadRequest, CompletedMultipartUpload, CompletedPart};
        use rusoto_core::ByteStream;

        let upload_id = self.client.create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.0.name.clone(),
            key: object.0.key.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            //TODO: content_type, storage_clase etc.
            .. Default::default()
        })
        .with_timeout(Duration::from_secs(300)).sync()?
        .upload_id.expect("no upload ID");

        info!("Started multipart upload {:?}", upload_id);

        let mut completed_parts = Vec::new();
        let mut progress = TransferProgress::default();

        // Notify progress init
        self.on_part_uploaded.as_ref().map(|c| c(&progress));

        let result = || -> Result<_, S3SyncError> {
            //TODO: configurable
            let body = &mut body;

            for part_number in 1u16.. {
                let mut buf = Vec::with_capacity(self.part_size);
                let bytes = body.take(self.part_size as u64).read_to_end(&mut buf)?;

                // Don't create 0 byte parts on EoF
                if bytes == 0 {
                    break
                }

                //let body = ByteStream::new(stream::once(Ok(Bytes::from(buf))));
                let body = ByteStream::from(buf);

                info!("Uploading part {} ({} bytes)", part_number, bytes);
                let result = self.client.upload_part(UploadPartRequest {
                    body: Some(body),
                    bucket: bucket.0.name.clone(),
                    key: object.0.key.clone(),
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
                progress.bytes += bytes as u64;

                // Notify with progress
                self.on_part_uploaded.as_ref().map(|c| c(&progress));
            }

            // Read did not return any data
            if completed_parts.is_empty() {
                return Err(S3SyncError::NoBodyError)
            }

            info!("Multipart upload {:?} complete", upload_id);
            self.client.complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: bucket.0.name.clone(),
                key: object.0.key.clone(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(completed_parts)
                }),
                .. Default::default()
            }).with_timeout(Duration::from_secs(300)).sync()?;

            // Notify it is done
            progress.status = TransferStatus::Done;
            self.on_part_uploaded.as_ref().map(|c| c(&progress));

            Ok(Present(object.0.clone()))
        }();

        if let Err(err) = &result {
            error!("Aborting multipart upload {:?} due to error: {}", upload_id, err);
            self.client.abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: bucket.0.name.clone(),
                key: object.0.key,
                upload_id,
                .. Default::default()
            }).with_timeout(Duration::from_secs(300)).sync().ok_or_log_warn();

            // Notify it is has failed
            progress.status = TransferStatus::Failed;
            self.on_part_uploaded.as_ref().map(|c| c(&progress));
        }

        result
    }

    pub fn delete_objects(&self, bucket: String, objects: Vec<Present<Object>>) -> Result<Vec<Absent<Object>>, S3SyncError> {
        objects.chunks(1000).map(|chunk| {
            info!("Deleting batch of {} objects from S3 bucket '{}'", chunk.len(), &bucket);
            let res = self.client.delete_objects(DeleteObjectsRequest {
                bucket: bucket.clone(),
                delete: Delete {
                    objects: chunk.into_iter().map(|object| ObjectIdentifier {
                        key: (object.0).key.clone(),
                        .. Default::default()
                    }).collect(),
                    .. Default::default()
                }, .. Default::default()
            }).with_timeout(Duration::from_secs(60)).sync()?;
            debug!("Delete response: {:?}", res);

            if let Some(errors) = res.errors {
                for error in errors {
                    error!("Error deleting S3 object '{}': {}",
                        error.key.as_ref().map(|s| s.as_str()).unwrap_or("<None>"),
                        error.message.as_ref().map(|s| s.as_str()).unwrap_or("<None>"));
                }
            }

            Ok(if let Some(deleted) = res.deleted {
                info!("Deleted {} objects", deleted.len());
                deleted.into_iter().filter_map(|deleted| deleted.key).collect::<Vec<_>>()
            } else {
                Vec::new()
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|chunks|
            chunks.into_iter()
            .flat_map(|chunk| chunk)
            .map(|key| Absent(Object::from_key(key)))
            .collect::<Vec<_>>())
    }

    /// Ensure that object is present in S3 bucket.
    ///
    /// It will call body function to obtain Read object from which the data will be uploaded to S3
    /// if object does not already exist there.
    pub fn object_present<'s, R: Read + 's, F: FnOnce() -> Result<R, std::io::Error> + 's>(&'s self, bucket: &'s Present<Bucket>, object: Object, body: F) -> impl Ensure<Present<Object>, EnsureAction = impl Meet<Met = Present<Object>, Error = S3SyncError> + 's> + 's {
        move || {
            Ok(match self.check_object_exists(&bucket, object.clone())? {
                Left(present) => Met(present),
                Right(absent) => EnsureAction(move || {
                    self.put_object_body(bucket, absent, body()?)
                })
            })
        }
    }

}

#[cfg(feature = "test-s3")]
#[cfg(test)]
mod tests {
    use super::*;

    pub fn s3_test_bucket() -> Bucket {
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set").into()
    }

    #[test]
    fn test_object_present() {
        use std::io::Cursor;

        let s3 = S3::new(Region::EuWest1, None);
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        s3.object_present(&bucket, "/s3-sync-test/foo".to_owned().into(), move || Ok(body)).ensure().or_failed_to("make object present");
    }

    #[test]
    fn test_object_present_empty_read() {
        use std::io::Cursor;

        let s3 = S3::new(Region::EuWest1, None);
        let body = Cursor::new(b"".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        s3.object_present(&bucket, "/s3-sync-test/foo".to_owned().into(), move || Ok(body)).ensure().or_failed_to("make object present");
    }
}
