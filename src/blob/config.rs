use std::ops::Index;

use super::IndexConfig;

#[derivce(Debug, Clone)]
pub struct BlobConfig {
    pub(super) index: IndexConfig,
    pub(super) validate_data_during_index_regen: bool,
}


impl BlobConfig {
    pub fn new(index: IndexConfig, validate_data_during_index_regen: bool) -> Self {
        Self {
            index,
            validate_data_during_index_regen
        }
    }

    pub fn index(&self) -> &IndexConfig {
        &self.index
    }
    pub fn validate_data_during_index_regen(&self) -> bool {
        self.validate_data_during_index_regen
    }
}