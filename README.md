[![Latest Version]][crates.io] [![Documentation]][docs.rs] ![License]
High level synchronous S3 Rust client library.

This client wraps Rusoto S3 and provides the following features:
* check if bucket or object exists,
* list objects that match prefix as iterator that handles pagination transparently,
* put large objects via multipart API and follow progress via callback,
* delete single or any number of objects via bulk delete API,
* deffer execution using `ensure` crate for putting and deleting objects.

Example usage
=============

```rust
use s3_sync::{S3, Region, ObjectBodyMeta, Object, Bucket};
use std::io::Cursor;
use std::io::Read;

let test_bucket = std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set");
let test_key = "foobar.test";

let s3 = S3::new(Region::default());

let bucket = s3.check_bucket_exists(Bucket::from_name(test_bucket)).expect("check if bucket exists").left().expect("bucket does not exist");
let object = Object::from_key(&bucket, test_key.to_owned());

let body = Cursor::new(b"hello world".to_vec());
let object = s3.put_object(object, body, ObjectBodyMeta::default()).unwrap();

let mut body = Vec::new();
s3.get_body(&object).expect("object body").read_to_end(&mut body).unwrap();

assert_eq!(&body, b"hello world");
```

[crates.io]: https://crates.io/crates/multiline
[Latest Version]: https://img.shields.io/crates/v/multiline.svg
[Documentation]: https://docs.rs/multiline/badge.svg
[docs.rs]: https://docs.rs/multiline
[License]: https://img.shields.io/crates/l/multiline.svg
