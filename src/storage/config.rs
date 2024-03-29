use std::time::Duration;

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    work_dir: Option<PathBuf>,
    create_work_dir: bool,
    max_blob_size: Option<u64>,
    max_data_in_blob: Option<u64>,
    blob_file_name_prefix: Option<String>,
    debounce_interval_ms: u64,
    allow_duplicates: bool,
    ignore_corrupted: bool,
    index: IndexConfig,
    dump_sem: Arc<Semaphore>,
    corrupted_dir_name: String,
    bloom_filter_group_size: usize,
    deferred_min_time: Duration,
    deferred_max_time: Duration,
    validate_data_during_index_regen: bool,
    max_dirty_bytes_before_sync: u64
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
    pub const fn debounce_interval_ms(&self) -> u64 {
        self.debounce_interval_ms
    }

    #[inline]
    pub const fn allow_duplicates(&self) -> bool {
        self.allow_duplicates
    }

    #[inline]
    pub const fn ignore_corrupted(&self) -> bool {
        self.ignore_corrupted
    }

    #[inline]
    pub fn corrupted_dir_name(&self) -> &str {
        self.corrupted_dir_name.as_str()
    }

    #[inline]
    pub fn index(&self) -> &IndexConfig {
        &self.index
    }

    #[inline]
    pub fn blob(&self) -> BlobConfig {
        BlobConfig::new(self.index().clone(), self.validate_data_during_index_regen())
    }

    #[inline]
    pub fn create_work_dir(&self) -> bool {
        self.create_work_dir
    }

    #[inline]
    pub fn dump_sem(&self) -> Arc<Semaphore> {
        self.dump_sem.clone()
    }

    pub fn bloom_filter_group_size(&self) -> usize {
        self.bloom_filter_group_size
    }

    pub fn deferred_min_time(&self) -> Duration {
        self.deferred_min_time
    }

    pub fn deferred_max_time(&self) -> Duration {
        self.deferred_max_time
    }

    pub fn validate_data_during_index_regen(&self) -> bool {
        self.validate_data_during_index_regen
    }

    pub fn max_dirty_bytes_before_sync(&self) -> u64 {
        self.max_dirty_bytes_before_sync
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

    pub fn set_ignore_corrupted(&mut self, ignore_corrupted: bool) {
        self.ignore_corrupted = ignore_corrupted;
    }

    pub fn set_corrupted_dir_name(&mut self, name: String) {
        self.corrupted_dir_name = name;
    }

    pub fn set_index(&mut self, index: IndexConfig) {
        self.index = index
    }

    pub fn set_create_work_dir(&mut self, create: bool) {
        self.create_work_dir = create;
    }

    pub fn set_dump_sem(&mut self, dump_sem: Arc<Semaphore>) {
        self.dump_sem = dump_sem
    }

    pub fn set_bloom_filter_group_size(&mut self, bloom_filter_group_size: usize) {
        self.bloom_filter_group_size = bloom_filter_group_size
    }

    pub fn set_deferred_index_dump_times(&mut self, min: Duration, max: Duration) {
        self.deferred_min_time = min;
        self.deferred_max_time = max;
    }

    pub fn set_validate_data_during_index_regen(&mut self, value: bool) {
        self.validate_data_during_index_regen = value;
    }

    pub fn set_max_dirty_bytes_before_sync(&mut self, value: u64) {
        self.max_dirty_bytes_before_sync = value;
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
            debounce_interval_ms: 200,
            allow_duplicates: false,
            ignore_corrupted: false,
            index: Default::default(),
            dump_sem: Arc::new(Semaphore::new(1)),
            corrupted_dir_name: "corrupted".into(),
            bloom_filter_group_size: 8,
            deferred_min_time: Duration::from_secs(60),
            deferred_max_time: Duration::from_secs(180),
            validate_data_during_index_regen: false,
            max_dirty_bytes_before_sync: 32 * 1024 * 1024
        }
    }
}
