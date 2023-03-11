use crate::prelude::*;

/// Stub for Rio on non-linux envs
#[derive(Clone, Debug)]
pub struct Rio;

impl Rio {
    pub(crate) async fn write_at(&self, _: &StdFile, _: &[u8], _: u64) -> IOResult<usize> {
        self.default_behaviour().await
    }

    pub(crate) async fn read_at(&self, _: &StdFile, _: &[u8], _: u64) -> IOResult<usize> {
        self.default_behaviour().await
    }

    pub(crate) async fn fsync(&self, _: &StdFile) -> IOResult<()> {
        self.default_behaviour().await
    }

    async fn default_behaviour<T>(&self) -> IOResult<T> {
        panic!("Async IO not supported");
    }
}
