use rusoto_s3::S3Client;
use rusoto_s3::{DeleteObjectsRequest, Delete, ObjectIdentifier};
use rusoto_core::region::Region;
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
use ensure::{Absent, Present, Ensure, Meet};
use ensure::CheckEnsureResult::*;
use either::Either;
use Either::*;

#[derive(Debug)]
pub enum S3SyncError {
    RusotoError(Box<dyn Error>),
    IoError(std::io::Error),
    NoBodyError,
}

impl<T: Error + 'static> From<RusotoError<T>> for S3SyncError {
    fn from(err: RusotoError<T>) -> S3SyncError {
        S3SyncError::RusotoError(Box::new(err))
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

impl From<S3Object> for Object {
    fn from(object: S3Object) -> Object {
        Object::from_key(object.key.expect("S3 object has no key!"))
    }
}

// pub fn objects_absent(bucket: String, objects: Vec<impl Into<Object>>) -> 
// impl Promise<Met = Vec<Absent<Object>>, Meet = impl Meet<Met = (Vec<Absent<Object>>, Vec<Absent<Object>>)>> {
//     move || {
//         let s3 = S3::new();
//         let mut existing_objects = Vec::new();
//         let mut non_existing_objects = Vec::new();

//         for object in objects.into_iter().map(Into::into) {
//             let key = object.key.to_owned();
//             in_context_of_with(|| format!("checking if object with key '{}' exists in bucket '{}'", key, bucket.clone()), || {
//                 s3.check_object_exists(bucket.clone(), object).map(|res| match res {
//                     Left(Present) => existing_objects.push(Present),
//                     Right(non_existing) => non_existing_objects.push(non_existing),
//                 })
//             })?
//         }

//         Ok(if existing_objects.is_empty() {
//             Left(non_existing_objects)
//         } else {
//             Right(move || {
//                 let deleted = s3.delete_objects(bucket, existing_objects)?;
//                 Ok((deleted, non_existing_objects))
//             })
//         })
//     }
// }

pub fn object_present<'s>(s3: &'s S3, bucket: String, object: Object, body: impl Read + 's) -> impl Ensure<Present<Object>, EnsureAction = impl Meet<Met = Present<Object>, Error = S3SyncError> + 's> + 's {
    move || {
        Ok(match s3.check_object_exists(bucket.clone(), object.clone())? {
            Left(present) => Met(present),
            Right(absent) => EnsureAction(move || {
                s3.put_object_body(bucket, absent, body)
            })
        })
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

pub struct S3 {
    client: S3Client
}

impl S3 {
    // https://rusoto.github.io/rusoto/rusoto_s3/index.html
    pub fn new() -> S3 {
        S3 {
            client: S3Client::new(Region::EuWest1)
        }
    }

    pub fn check_object_exists(&self, bucket: String, object: Object) -> Result<Either<Present<Object>, Absent<Object>>, S3SyncError> {
        use rusoto_s3::HeadObjectRequest;
        use rusoto_s3::HeadObjectError;

        let res = self.client.head_object(HeadObjectRequest {
            bucket,
            key: object.key.clone(),
            .. Default::default()
        }).with_timeout(Duration::from_secs(10)).sync();
        debug!("Head response: {:?}", res);

        match res {
            Ok(_) => Ok(Left(Present(object))),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(Right(Absent(object))),
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

    pub fn put_object_body(&self, bucket: String, object: Absent<Object>, mut body: impl Read) -> Result<Present<Object>, S3SyncError> {
        use rusoto_s3::{CreateMultipartUploadRequest, UploadPartRequest, CompleteMultipartUploadRequest};
        use rusoto_core::ByteStream;
        use futures::stream;
        use bytes::Bytes;

        let upload_id = self.client.create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: object.0.key.clone(),
            //TODO: content_type, storage_clase etc.
            .. Default::default()
        })
        .with_timeout(Duration::from_secs(300)).sync()?
        .upload_id.expect("no upload ID");

        //TODO: configurable
        let chunk_size = 10 * 1024 * 1024; // note that max parts is 10k = 100_000 MiB
        let body = &mut body;

        for part_number in 0.. {
            let mut buf = Vec::new();
            body.take(chunk_size).read_to_end(&mut buf)?;
            let body = ByteStream::new(stream::once(Ok(Bytes::from(buf))));

            self.client.upload_part(UploadPartRequest {
                body: Some(body),
                bucket: bucket.clone(),
                key: object.0.key.clone(),
                part_number,
                upload_id: upload_id.clone(),
                .. Default::default()
            }).with_timeout(Duration::from_secs(300)).sync()?;
        }

        self.client.complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket,
            key: object.0.key.clone(),
            upload_id,
            .. Default::default()
        }).with_timeout(Duration::from_secs(300)).sync()?;

        Ok(Present(object.0))
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
