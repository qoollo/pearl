use super::IndexConfig;

#[derive(Debug, Clone)]
pub struct BlobConfig {
    pub(super) index: IndexConfig,
    pub(super) validate_data_during_index_regen: bool,
}


#[allow(dead_code)]
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