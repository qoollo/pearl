use super::prelude::*;
pub(crate) use crate::record::Record;
pub use crate::BloomConfig;

impl Record {
    pub(crate) fn migrate(self, source: u32, target: u32) -> AnyResult<Self> {
        match (source, target) {
            (source, target) if source >= target => Ok(self),
            (0, 1) => self.mirgate_v0_to_v1(),
            (source, target) => Err(Error::unsupported_migration(source, target).into()),
        }
    }

    pub(crate) fn mirgate_v0_to_v1(mut self) -> AnyResult<Self> {
        self.header = self.header.with_reversed_key_bytes()?;
        Ok(self)
    }
}

/// Migrate blob version
pub fn migrate_blob(
    input: &Path,
    output: &Path,
    validate_every: usize,
    target_version: u32,
) -> AnyResult<()> {
    process_blob_with(
        &input,
        &output,
        validate_every,
        |record, version| record.migrate(version, target_version),
        false,
    )?;
    Ok(())
}
