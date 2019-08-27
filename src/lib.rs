/*!
High level synchronous S3 client.

This client wraps Rusoto S3 and provides the following features:
* check if bucket or object exists,
* list objects that match prefix as iterator that handles pagination transparently,
* put large objects via multipart API and follow progress via callback,
* delete single or any number of objects via bulk delete API,
* deffer execution using `ensure` crate for putting and deleting objects.

Example usage
=============

```rust
use s3_sync::{S3, Region, ObjectBodyMeta, Object};
use std::io::Cursor;
use std::io::Read;

let test_bucket = std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set");
let test_key = "foobar.test";

let s3 = S3::new(Region::default());

let bucket = s3.check_bucket_exists(test_bucket.into()).expect("check if bucket exists").left().expect("bucket does not exist");
let object = Object::from_key(&bucket, test_key.to_owned());

let body = Cursor::new(b"hello world".to_vec());
let object = s3.put_object(object, body, ObjectBodyMeta::default()).unwrap();

let mut body = Vec::new();
s3.get_body(&object).expect("object body").read_to_end(&mut body).unwrap();

assert_eq!(&body, b"hello world");
```
!*/
use rusoto_s3::S3Client;
use rusoto_s3::{DeleteObjectRequest, DeleteObjectsRequest, Delete, ObjectIdentifier};
pub use rusoto_core::region::Region;
use rusoto_core::RusotoError;
use rusoto_s3::S3 as S3Trait;
use rusoto_s3::{ListObjectsV2Request, ListObjectsV2Output};
use rusoto_s3::Object as S3Object;
use log::{trace, debug, error};
use itertools::unfold;
use std::time::Duration;
use std::io::Read;
use std::error::Error;
use std::fmt;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
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

const DELETE_BATCH_SIZE: usize = 1000; // how many objects to delete in single batch call
const MAX_MULTIPART_PARTS: usize = 10_000; // as defined by S3 API limit

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

impl External for Object<'_> {}

impl<'b> Object<'b> {
    /// Creates `Object` from present `Bucket` and given key `String`.
    pub fn from_key(bucket: &Present<Bucket>, key: String) -> Object {
        Object {
            bucket,
            key
        }
    }

    fn from_s3_object(bucket: &Present<Bucket>, object: S3Object) -> Object {
        Object::from_key(bucket, object.key.expect("S3 object has no key!"))
    }

    /// Gets objects bucket.
    pub fn bucket(&self) -> &Present<Bucket> {
        self.bucket
    }

    /// Gets object key.
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
    /// Gets bucket name.
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

struct PaginationIter<RQ, RS, SSA, GSA, FF, E> where RQ: Clone, SSA: Fn(&mut RQ, String), GSA: Fn(&RS) -> Option<String>, FF: Fn(RQ) -> Result<RS, E> {
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

/// Information about status of the ongoing transfer.
#[derive(Debug)]
pub enum TransferStatus {
    /// Initialization successful.
    Init,
    /// Transfer is ongoing.
    Progress(TransferStats),
    /// Transfer successfully complete.
    Done(TransferStats),
    /// There was an error.
    Failed(String),
}

impl Default for TransferStatus {
    fn default() -> Self {
        TransferStatus::Init
    }
}

impl TransferStatus {
    fn update(&mut self, stats: TransferStats) {
        match self {
            TransferStatus::Init => {
                *self = TransferStatus::Progress(stats);
            }
            TransferStatus::Progress(ref mut s) => {
                s.buffers += stats.buffers;
                s.bytes = stats.bytes;
                s.bytes_total += stats.bytes_total;
            },
            _ => panic!("TransferStats in bad state for .done(): {:?}", self),
        }
    }

    fn done(self) -> Self {
        match self {
            TransferStatus::Progress(stats) => TransferStatus::Done(stats),
            _ => panic!("TransferStats in bad state for .done(): {:?}", self),
        }
    }

    fn failed(self, err: String) -> Self {
        match self {
            TransferStatus::Init |
            TransferStatus::Progress(_) => TransferStatus::Failed(err),
            _ => panic!("TransferStats in bad state for .failed(): {:?}", self),
        }
    }
}

/// Information about transfer progress.
#[derive(Debug, Default)]
pub struct TransferStats {
    /// Number of buffers or parts transferred.
    pub buffers: u16,
    /// Number of bytes transferred since last progress.
    pub bytes: u64,
    /// Total number of bytes transferred.
    pub bytes_total: u64,
}

/// Meta information about object body.
#[derive(Debug)]
pub struct ObjectBodyMeta {
    /// A standard MIME type describing the format of the object data.
    pub content_type: String,
    /// Specifies presentational information for the object.
    pub content_disposition: Option<String>,
    /// The language the content is in.
    pub content_language: Option<String>,
}

impl Default for ObjectBodyMeta {
    fn default() -> ObjectBodyMeta {
        ObjectBodyMeta {
            content_type: "application/octet-stream".to_owned(),
            content_disposition: None,
            content_language: None,
        }
    }
}

/// Wrapper of Rusoto S3 client that adds some high level imperative and declarative operations on
/// S3 buckets and objects.
pub struct S3 {
    client: S3Client,
    on_upload_progress: Option<RefCell<Box<dyn FnMut(&TransferStatus)>>>,
    /// Size of multipart upload part.
    part_size: usize,
    /// Timeout for non data related operations.
    timeout: Duration,
    /// Timeout for data upload/download operations.
    data_timeout: Duration,
}

pub struct Settings {
    /// Size of multipart upload part.
    pub part_size: usize,
    /// Timeout for non data related operations.
    pub timeout: Duration,
    /// Timeout for data upload/download operations.
    pub data_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            part_size: 10 * 1024 * 1024, // note that max part count is 10k so we can upload up to 100_000 MiB
            timeout: Duration::from_secs(10),
            data_timeout: Duration::from_secs(300),
        }
    }
}

impl S3 {
    /// Creates new Rusoto based high level S3 client with default settings.
    pub fn new(region: Region) -> S3 {
        Self::new_with_settings(region, Settings::default())
    }

    /// Creates new Rusoto based high level S3 client with given settings.
    pub fn new_with_settings(region: Region, settings: Settings) -> S3 {
        S3 {
            client: S3Client::new(region),
            on_upload_progress: None,
            part_size: settings.part_size,
            timeout: settings.timeout,
            data_timeout: settings.data_timeout,
        }
    }

    /// Gets maximum size of the multipart upload part.
    ///
    /// Useful to set up other I/O buffers accordingly.
    pub fn part_size(&self) -> usize {
        self.part_size
    }

    /// Returns maximum size of data in bytes that S3 object can be put with.
    pub fn max_upload_size(&self) -> usize {
        self.part_size * MAX_MULTIPART_PARTS
    }

    /// Set callback on body upload progress.
    pub fn on_upload_progress(&mut self, callback: impl FnMut(&TransferStatus) + 'static) -> Option<Box<dyn FnMut(&TransferStatus)>> {
        let ret = self.on_upload_progress.take();
        self.on_upload_progress = Some(RefCell::new(Box::new(callback)));
        ret.map(|c| c.into_inner())
    }

    /// Calls `f` with `S3` client that has `on_upload_progress` set to `callback` and restores
    /// callback to previous state on return.
    pub fn with_on_upload_progress<O>(&mut self, callback: impl FnMut(&TransferStatus) + 'static, f: impl FnOnce(&mut Self) -> O) -> O {
        let old = self.on_upload_progress(callback);
        let ret = f(self);
        old.map(|callback| self.on_upload_progress(callback));
        ret
    }

    fn notify_upload_progress(&self, status: &TransferStatus) {
        self.on_upload_progress.as_ref().map(|c| {
            c.try_borrow_mut().expect("S3 upload_progress closure already borrowed mutable").as_mut()(status);
        });
    }

    /// Checks if given bucket exists.
    pub fn check_bucket_exists(&self, bucket: Bucket) -> Result<Either<Present<Bucket>, Absent<Bucket>>, S3SyncError> {
        use rusoto_s3::HeadBucketRequest;
        use rusoto_s3::HeadBucketError;
        use rusoto_core::request::BufferedHttpResponse;

        let res = self.client.head_bucket(HeadBucketRequest {
            bucket: bucket.name.clone(),
            .. Default::default()
        }).with_timeout(self.timeout).sync();
        trace!("Head bucket response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(bucket))),
            Err(RusotoError::Service(HeadBucketError::NoSuchBucket(_))) => Ok(Right(Absent(bucket))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(bucket))),
            Err(err) => Err(err.into())
        }
    }

    /// Checks if given object exists.
    pub fn check_object_exists<'s, 'b>(&'s self, object: Object<'b>) -> Result<Either<Present<Object<'b>>, Absent<Object<'b>>>, S3SyncError> {
        use rusoto_s3::HeadObjectRequest;
        use rusoto_s3::HeadObjectError;
        use rusoto_core::request::BufferedHttpResponse;

        let res = self.client.head_object(HeadObjectRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(self.timeout).sync();
        trace!("Head response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(object))),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(Right(Absent(object))),
            Err(RusotoError::Unknown(BufferedHttpResponse { status, .. })) if status.as_u16() == 404 => Ok(Right(Absent(object))),
            Err(err) => Err(err.into())
        }
    }

    /// Provides iterator of objects in existing bucket that have key of given prefix.
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
                client.list_objects_v2(request).with_timeout(self.timeout).sync().map_err(Into::into)
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

    /// Gets object body.
    pub fn get_body<'s, 'b>(&'s self, object: &'_ Present<Object<'b>>) -> Result<impl Read, S3SyncError> {
        use rusoto_s3::GetObjectRequest;
        self.client.get_object(GetObjectRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(self.data_timeout).sync()
        .map_err(Into::into)
        .and_then(|output| output.body.ok_or(S3SyncError::NoBodyError))
        .map(|body| body.into_blocking_read())
    }

    /// Puts object with given body using multipart API.
    ///
    /// If given existing object it will be overwritten.
    ///
    /// Use `.max_upload_size()` to find out how many bytes the body can have at maximum.
    /// Increase `part_size` to be able to upload more date (`max_upload_size = part_number *
    /// 10_000`).
    pub fn put_object<'s, 'b>(&'s self, object: impl ExternalState<Object<'b>>, mut body: impl Read, meta: ObjectBodyMeta) -> Result<Present<Object<'b>>, S3SyncError> {
        use rusoto_s3::{CreateMultipartUploadRequest, UploadPartRequest, CompleteMultipartUploadRequest, AbortMultipartUploadRequest, CompletedMultipartUpload, CompletedPart};
        use rusoto_core::ByteStream;

        // does not matter if object is present or absent
        let object = object.invalidate_state();
        let bucket_name = object.bucket.name.clone();
        let object_key = object.key.clone();

        let upload_id = self.client.create_multipart_upload(CreateMultipartUploadRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            content_type: Some(meta.content_type),
            content_disposition: meta.content_disposition,
            content_language: meta.content_language,
            .. Default::default()
        })
        .with_timeout(self.timeout).sync()?
        .upload_id.expect("no upload ID");

        debug!("Started multipart upload {:?}", upload_id);

        let mut completed_parts = Vec::new();
        let mut progress = TransferStatus::default();

        // Notify progress init
        self.notify_upload_progress(&progress);

        let result = || -> Result<_, S3SyncError> {
            let body = &mut body;

            for part_number in 1u16.. {
                // Note that S3 does not support chunked uploads of single part so we need to send
                // full part at once. Best thing to do here would be to use Bytes directly to avoid
                // allocation per part upload...
                // Need to allocate one byte more to avoid re-allocation as read_to_end needs to
                // have place for storage before it gets EoF.
                let mut buf = Vec::with_capacity(self.part_size + 1);
                let bytes = body.take(self.part_size as u64).read_to_end(&mut buf)?;

                // Don't create 0 byte parts on EoF
                if bytes == 0 {
                    break
                }

                debug!("Uploading part {} ({} bytes)", part_number, bytes);
                let result = self.client.upload_part(UploadPartRequest {
                    body: Some(ByteStream::from(buf)),
                    bucket: bucket_name.clone(),
                    key: object_key.clone(),
                    part_number: part_number as i64,
                    upload_id: upload_id.clone(),
                    .. Default::default()
                }).with_timeout(self.data_timeout).sync()?;

                completed_parts.push(CompletedPart {
                    e_tag: result.e_tag,
                    part_number: Some(part_number as i64),
                });

                progress.update(TransferStats {
                    buffers: 1,
                    bytes: bytes as u64,
                    bytes_total: bytes as u64,
                });

                // Notify with progress
                self.notify_upload_progress(&progress);
            }

            // No parts uploaded
            if completed_parts.is_empty() {
                return Err(S3SyncError::NoBodyError)
            }

            debug!("Multipart upload {:?} complete", upload_id);
            self.client.complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: bucket_name.clone(),
                key: object_key.clone(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(completed_parts)
                }),
                .. Default::default()
            }).with_timeout(self.timeout).sync()?;

            Ok(Present(object))
        }();

        if let Err(err) = &result {
            let err = Problem::from_error_message(err).to_string();
            error!("Aborting multipart upload {:?} due to error: {}", upload_id, err);
            self.client.abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: bucket_name,
                key: object_key,
                upload_id,
                .. Default::default()
            }).with_timeout(self.timeout).sync().ok_or_log_warn();

            // Notify it is has failed
            self.notify_upload_progress(&progress.failed(err));
        } else {
            // Notify it is done
            self.notify_upload_progress(&progress.done());
        }

        result
    }

    /// Deletes single object.
    ///
    /// Delete call does not fail if object does not exist and therefore this method can work with
    /// `Present`, `Absent` or just `Object` values.
    ///
    /// To delete many objects it is better performance wise to use `.delete_objects()` witch
    /// uses bulk delete API.
    pub fn delete_object<'b, 's: 'b>(&'s self, object: impl ExternalState<Object<'b>>) -> Result<Absent<Object<'b>>, S3SyncError> {
        let object = object.invalidate_state();

        debug!("Deleting object {:?} from S3 bucket {:?}", object.key, object.bucket.name);
        let res = self.client.delete_object(DeleteObjectRequest {
            bucket: object.bucket.name.clone(),
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(self.timeout).sync()?;
        trace!("Delete response: {:?}", res);

        Ok(Absent(object))
    }

    /// Deletes list of objects in streaming fashion using bulk delete API.
    ///
    /// Note that if returned iterator is not completely consumed not all items from the list may
    /// be processed.
    ///
    /// Delete call does not fail if object does not exist and therefore this method can work with
    /// `Present`, `Absent` or just `Object` values.
    ///
    /// Objects can live in different buckets but for best performance it is
    /// recommended to order the list by bucket so that biggest batches can be crated.
    ///
    /// Each returned item represent batch delete call to S3 API.
    /// Successful batch call will return `Ok` variant containing vector of results for each
    /// individual object delete operation as provided by S3.
    pub fn delete_objects<'b, 's: 'b>(&'s self, objects: impl IntoIterator<Item = impl ExternalState<Object<'b>>>) -> impl Iterator<Item = Result<Vec<Result<Absent<Object<'b>>, (Object<'b>, S3SyncError)>>, S3SyncError>> + Captures1<'s> + Captures2<'b> {
        objects
            .into_iter()
            .map(|o| o.invalidate_state())
            .peekable()
            .batching(|objects| {
                let current_bucket_name = if let Some(object) = objects.peek() {
                    object.bucket.name.clone()
                } else {
                    return None
                };

                Some((current_bucket_name.clone(),
                     objects
                        .peeking_take_while(move |object| object.bucket.name == current_bucket_name)
                        .take(DELETE_BATCH_SIZE)
                        .collect::<Vec<_>>()))
            })
            .map(move |(current_bucket_name, objects): (_, Vec<_>)| {
                debug!("Deleting batch of {} objects from S3 bucket {:?}", objects.len(), current_bucket_name);
                let res = self.client.delete_objects(DeleteObjectsRequest {
                    bucket: current_bucket_name.clone(),
                    delete: Delete {
                        objects: objects.iter().map(|object| ObjectIdentifier {
                            key: object.key.clone(),
                            .. Default::default()
                        }).collect::<Vec<_>>(),
                        .. Default::default()
                    }, .. Default::default()
                }).with_timeout(self.timeout).sync()?;
                trace!("Delete response: {:?}", res);

                let ok_objects =
                if let Some(deleted) = res.deleted {
                    debug!("Deleted {} objects", deleted.len());
                    deleted.into_iter().map(|deleted| {
                        if let Some(key) = deleted.key {
                            Ok(key)
                        } else {
                            Err(S3SyncError::RusotoError(RusotoError::Validation("got S3 delete object errors but no key or message information".to_owned())))
                        }
                    }).collect::<Result<HashSet<_>, _>>()?
                } else {
                    Default::default()
                };

                let mut failed_objects =
                if let Some(errors) = res.errors {
                    errors.into_iter().map(|error| {
                        error!("Error deleting S3 object {:?}: {}",
                            error.key.as_ref().map(|s| s.as_str()).unwrap_or("<None>"),
                            error.message.as_ref().map(|s| s.as_str()).unwrap_or("<None>"));

                        // Try the best to get failed objects out of OK objects along with error
                        // message
                        if let (Some(key), Some(error)) = (error.key, error.message) {
                            Ok((key, S3SyncError::RusotoError(RusotoError::Validation(error))))
                        } else {
                            Err(S3SyncError::RusotoError(RusotoError::Validation("got S3 delete object errors but no key or message information".to_owned())))
                        }
                    }).collect::<Result<HashMap<_, _>, _>>()?
                } else {
                    Default::default()
                };

                Ok(objects.into_iter().map(|o| {
                    if ok_objects.contains(&o.key) {
                        Ok(Absent(o))
                    } else if let Some(err) = failed_objects.remove(&o.key) {
                        Err((o, err))
                    } else {
                        Err((o, S3SyncError::RusotoError(RusotoError::Validation("S3 did not report this object as deleted or failed to be deleted".to_owned()))))
                    }
                }).collect::<Vec<_>>()) // Option<Result<Vec<Result<,>,>>
            })
    }

    /// Returns `Ensure` object that can be used to ensure that object is present in the S3 bucket.
    ///
    /// It will call body function to obtain `Read` object from which the data will be uploaded to S3
    /// and its meta data if object does not already exist there.
    ///
    /// Note that there can be a race condition between check if object exists and upload.
    pub fn object_present<'b, 's: 'b, R: Read + 's, F: FnOnce() -> Result<(R, ObjectBodyMeta), std::io::Error> + 's>(&'s self, object: Object<'b>, body: F) -> impl Ensure<Present<Object<'b>>, EnsureAction = impl Meet<Met = Present<Object<'b>>, Error = S3SyncError> + Captures1<'s> + Captures2<'b>> + Captures1<'s> + Captures2<'b> {
        move || {
            Ok(match self.check_object_exists(object)? {
                Left(present) => Met(present),
                Right(absent) => EnsureAction(move || {
                    let (body, meta) = body()?;
                    self.put_object(absent, body, meta)
                })
            })
        }
    }

    /// Returns `Ensure` object that can be used to ensure that object is absent in the S3 bucket.
    ///
    /// Note that there can be a race condition between check if object exists and delete operation.
    pub fn object_absent<'b, 's: 'b>(&'s self, object: Object<'b>) -> impl Ensure<Absent<Object<'b>>, EnsureAction = impl Meet<Met = Absent<Object<'b>>, Error = S3SyncError> + Captures1<'s> + Captures2<'b>> + Captures1<'s> + Captures2<'b> {
        move || {
            Ok(match self.check_object_exists(object)? {
                Right(absent) => Met(absent),
                Left(present) => EnsureAction(move || {
                    self.delete_object(present)
                })
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::io::Cursor;

    fn s3_test_bucket() -> Bucket {
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set").into()
    }

    fn test_key() -> String {
        use std::time::SystemTime;
        format!("s3-sync-test/foo-{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros())
    }

    #[test]
    fn test_get_body() {
        use std::io::Cursor;

        let s3 = S3::new(Region::default());
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        let object = s3.put_object(object, body, ObjectBodyMeta::default()).unwrap();

        let mut body = Vec::new();

        s3.get_body(&object).unwrap().read_to_end(&mut body).unwrap();

        assert_eq!(&body, b"hello world");
    }

    #[test]
    fn test_object_present() {
        use std::io::Cursor;

        let s3 = S3::new(Region::default());
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        let object = s3.object_present(object, move || Ok((body, ObjectBodyMeta::default()))).ensure().or_failed_to("make object present");

        assert!(s3.check_object_exists(object.invalidate_state()).unwrap().is_left());
    }

    #[test]
    fn test_object_absent() {
        use std::io::Cursor;

        let s3 = S3::new(Region::default());
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        let object = s3.put_object(object, body, ObjectBodyMeta::default()).unwrap().invalidate_state();

        let object = s3.object_absent(object).ensure().or_failed_to("make object absent");

        assert!(s3.check_object_exists(object.invalidate_state()).unwrap().is_right());
    }

    #[test]
    fn test_object_present_empty_read() {
        use std::io::Cursor;

        let s3 = S3::new(Region::default());
        let body = Cursor::new(b"".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        assert_matches!(s3.object_present(object, move || Ok((body, ObjectBodyMeta::default()))).ensure(), Err(S3SyncError::NoBodyError));
    }

    #[test]
    fn test_object_present_progress() {
        let mut s3 = S3::new(Region::default());
        let body = Cursor::new(b"hello world".to_vec());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");
        let object = Object::from_key(&bucket, test_key());

        let asserts: Vec<Box<dyn Fn(&TransferStatus)>> = vec![
            Box::new(|t| assert_matches!(t, TransferStatus::Init)),
            Box::new(|t| assert_matches!(t, TransferStatus::Progress(_))),
            Box::new(|t| assert_matches!(t, TransferStatus::Done(_))),
        ];

        let mut asserts = asserts.into_iter();

        s3.with_on_upload_progress(move |t| asserts.next().unwrap()(t), |s3| {
            s3.object_present(object, move || Ok((body, ObjectBodyMeta::default()))).ensure().or_failed_to("make object present");
        });
    }

    #[test]
    fn test_delete_objects_given() {
        let s3 = S3::new(Region::default());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");

        let objects = vec![
            Object::from_key(&bucket, "s3-sync-test/bar-1".to_owned()),
            Object::from_key(&bucket, "s3-sync-test/bar-2".to_owned()),
        ];

        for object in objects.clone() {
            s3.put_object(object, Cursor::new(b"foo bar".to_vec()), ObjectBodyMeta::default()).unwrap();
        }

        let ops = s3.delete_objects(objects).or_failed_to("delete objects").collect::<Vec<_>>();

        // one batch
        assert_eq!(ops.len(), 1);
        // two objects in the batch
        assert_eq!(ops[0].len(), 2);

        assert_matches!(&ops[0][0], Ok(Absent(Object { bucket: Present(Bucket { name }), key })) => {
                        assert_eq!(name, &s3_test_bucket().name);
                        assert_eq!(key, "s3-sync-test/bar-1")
        });

        assert_matches!(&ops[0][1], Ok(Absent(Object { bucket: Present(Bucket { name }), key })) => {
                        assert_eq!(name, &s3_test_bucket().name);
                        assert_eq!(key, "s3-sync-test/bar-2")
        });
    }

    #[test]
    fn test_delete_objects_from_list() {
        let s3 = S3::new(Region::default());

        let bucket = s3.check_bucket_exists(s3_test_bucket()).or_failed_to("check if bucket exists").left().expect("bucket does not exist");

        let objects = vec![
            Object::from_key(&bucket, "s3-sync-test/baz-1".to_owned()),
            Object::from_key(&bucket, "s3-sync-test/baz-2".to_owned()),
            Object::from_key(&bucket, "s3-sync-test/bax".to_owned()),
        ];

        for object in objects {
            s3.put_object(object, Cursor::new(b"foo bar".to_vec()), ObjectBodyMeta::default()).unwrap();
        }

        let objects = s3.list_objects(&bucket, "s3-sync-test/baz".to_owned()).or_failed_to("get list of objects");
        let ops = s3.delete_objects(objects).or_failed_to("delete objects").collect::<Vec<_>>();

        // one batch
        assert_eq!(ops.len(), 1);
        // two objects in the batch
        assert_eq!(ops[0].len(), 2);

        assert_matches!(&ops[0][0], Ok(Absent(Object { bucket: Present(Bucket { name }), key })) => {
                        assert_eq!(name, &s3_test_bucket().name);
                        assert_eq!(key, "s3-sync-test/baz-1")
        });

        assert_matches!(&ops[0][1], Ok(Absent(Object { bucket: Present(Bucket { name }), key })) => {
                        assert_eq!(name, &s3_test_bucket().name);
                        assert_eq!(key, "s3-sync-test/baz-2")
        });
    }
}
