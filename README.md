[![Latest Version]][crates.io] [![Documentation]][docs.rs] ![License]

High level synchronous S3 Rust client library.

This client wraps Rusoto S3 and provides the following features:

* check if bucket or object exists,
* list objects that match prefix as iterator that handles pagination transparently,
* put large objects via multipart API and follow progress via a callback,
* delete single or multiple objects via bulk delete API,
* deffer execution using `ensure` crate for putting and deleting objects.

# Example usage

```rust
use s3_sync::{S3, Region, ObjectBodyMeta, BucketKey, Bucket};
use std::io::Cursor;
use std::io::Read;

let test_bucket = std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set");
let test_key = "foobar.test";

let s3 = S3::default();

let bucket = s3.check_bucket_exists(Bucket::from_name(test_bucket)).expect("check if bucket exists")
  .left().expect("bucket does not exist");
let bucket_key = BucketKey::from_string(&bucket, test_key);

let body = Cursor::new(b"hello world".to_vec());
let object = s3.put_object(bucket_key, body, ObjectBodyMeta::default()).unwrap();

let mut body = Vec::new();
s3.get_body(&object).expect("object body").read_to_end(&mut body).unwrap();

assert_eq!(&body, b"hello world");
```

[crates.io]: https://crates.io/crates/s3-sync
[Latest Version]: https://img.shields.io/crates/v/s3-sync.svg
[Documentation]: https://docs.rs/s3-sync/badge.svg
[docs.rs]: https://docs.rs/s3-sync
[License]: https://img.shields.io/crates/l/s3-sync.svg
