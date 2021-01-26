use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    work_dir: Option<PathBuf>,
    create_work_dir: bool,
    max_blob_size: Option<u64>,
    max_data_in_blob: Option<u64>,
    blob_file_name_prefix: Option<String>,
    update_interval_ms: u64,
    allow_duplicates: bool,
    filter: Option<BloomConfig>,
}

// Getters
impl Config {
    #[inline]
    pub fn work_dir(&self) -> Option<&Path> {
        self.work_dir.as_ref().map(AsRef::as_ref)
    }

    #[inline]
    pub const fn max_blob_size(&self) -> Option<u64> {
        self.max_blob_size
    }

    #[inline]
    pub const fn max_data_in_blob(&self) -> Option<u64> {
        self.max_data_in_blob
    }

    #[inline]
    pub fn blob_file_name_prefix(&self) -> Option<&str> {
        self.blob_file_name_prefix.as_ref().map(AsRef::as_ref)
    }

    #[inline]
    pub const fn update_interval_ms(&self) -> u64 {
        self.update_interval_ms
    }

    #[inline]
    pub const fn allow_duplicates(&self) -> bool {
        self.allow_duplicates
    }

    #[inline]
    pub fn filter(&self) -> Option<BloomConfig> {
        self.filter.clone()
    }

    #[inline]
    pub fn create_work_dir(&self) -> bool {
        self.create_work_dir
    }
}

//Setters
impl Config {
    pub fn set_work_dir(&mut self, path: PathBuf) {
        self.work_dir = Some(path);
    }

    pub fn set_max_blob_size(&mut self, max_blob_size: u64) {
        self.max_blob_size = Some(max_blob_size);
    }

    pub fn set_max_data_in_blob(&mut self, max_data_in_blob: u64) {
        self.max_data_in_blob = Some(max_data_in_blob);
    }

    pub fn set_blob_file_name_prefix(&mut self, blob_file_name_prefix: String) {
        self.blob_file_name_prefix = Some(blob_file_name_prefix);
    }

    pub fn set_allow_duplicates(&mut self, allow_duplicates: bool) {
        self.allow_duplicates = allow_duplicates;
    }

    pub fn set_filter(&mut self, filter: BloomConfig) {
        self.filter = Some(filter);
    }

    pub fn set_create_work_dir(&mut self, create: bool) {
        self.create_work_dir = create;
    }
}

// Impl Traits
impl Default for Config {
    fn default() -> Self {
        Self {
            work_dir: None,
            create_work_dir: true,
            max_blob_size: None,
            max_data_in_blob: None,
            blob_file_name_prefix: None,
            update_interval_ms: 100,
            allow_duplicates: false,
            filter: None,
        }
    }
}
