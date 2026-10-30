use crate::{ports::ObjectSigner, CoreResult};

#[derive(Debug, Default)]
pub struct FakeSigner;

impl ObjectSigner for FakeSigner {
    fn presign_put(&self, key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Ok(format!("put:{key}"))
    }

    fn presign_get(&self, key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Ok(format!("get:{key}"))
    }
}
