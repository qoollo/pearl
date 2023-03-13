use std::time::Duration;

use super::prelude::*;

/// Is used to initialize a `Storage`.
///
/// Required params:
///  - [`work_dir`] - where `Storage` will keep blob and index files
///  - [`max_blob_size`] - upper limit of blob file size
///  - [`max_data_in_blob`] - maximum number of records in one blob
///  - [`blob_file_name_prefix`] - prefix in blob file name pattern: {prefix}.{id}.{ext}
///  - [`key_size`] - const key size in bytes
///
/// # Example
/// ```no_run
/// use pearl::{Builder, Storage, ArrayKey};
///
/// let storage: Storage<ArrayKey<8>> = Builder::new()
///         .blob_file_name_prefix("benchmark")
///         .max_blob_size(10_000_000)
///         .max_data_in_blob(1_000)
///         .work_dir(std::env::temp_dir().join("pearl_benchmark"))
///         .build()
///         .unwrap();
/// ```
///
/// [`work_dir`]: struct.Builder.html#method.work_dir
/// [`max_blob_size`]: struct.Builder.html#method.max_blob_size
/// [`max_data_in_blob`]: struct.Builder.html#method.max_data_in_blob
/// [`blob_file_name_prefix`]: struct.Builder.html#method.blob_file_name_prefix
/// [`key_size`]: struct.Builder.html#method.key_size
#[derive(Default, Debug)]
pub struct Builder {
    config: Config,
    iodriver: IODriver,
}

const MAX_POSSIBLE_DATA_IN_BLOB: u64 = u32::MAX as u64;

impl Builder {
    /// Create new uninitialized `Builder`
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates `Storage` based on given configuration,
    /// returns error if not all params are set.
    /// # Errors
    /// Return error if some of the required params is missed or wrong
    pub fn build<K>(self) -> Result<Storage<K>>
    where
        for<'a> K: Key<'a> + 'static,
    {
        let mut error_params = String::new();
        if self.config.work_dir().is_none() {
            error_params.push_str("> work_dir\n");
        } else if self.config.max_data_in_blob().is_none() {
            error_params.push_str("> max_data_in_blob\n");
        } else if self.config.max_data_in_blob().unwrap() > MAX_POSSIBLE_DATA_IN_BLOB {
            error_params.push_str(&format!(
                "> max_data_in_blob must be less than {}",
                MAX_POSSIBLE_DATA_IN_BLOB
            ));
        } else if self.config.max_blob_size().is_none() {
            error_params.push_str("> max_blob_size\n");
        } else if self.config.blob_file_name_prefix().is_none() {
            error_params.push_str("> blob_file_name_prefix\n");
        }
        if error_params.is_empty() {
            Ok(Storage::new(self.config, self.iodriver))
        } else {
            error!("{}", error_params);
            Err(Error::uninitialized().into())
        }
    }

    /// Sets working directory. If path doesn't exists, Storage will try to create it
    /// at initialization stage.
    pub fn work_dir<S: Into<PathBuf>>(mut self, work_dir: S) -> Self {
        let path: PathBuf = work_dir.into();
        debug!("work dir set to: {}", path.display());
        self.config.set_work_dir(path);
        self
    }

    /// Sets directory name for corrupted files. If path doesn't exists, Storage will try to create it
    /// at initialization stage.
    pub fn corrupted_dir_name(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        debug!("corrupted dir name set to: {}", name);
        self.config.set_corrupted_dir_name(name);
        self
    }

    /// Sets blob file size approximate limit. When the file size exceeds it,
    /// active blob update is activated.
    /// Must be greater than zero
    #[must_use]
    pub fn max_blob_size(mut self, max_blob_size: u64) -> Self {
        if max_blob_size > 0 {
            self.config.set_max_blob_size(max_blob_size);
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// Limits max number of records in a single blob.
    /// Must be greater than zero
    #[must_use]
    pub fn max_data_in_blob(mut self, max_data_in_blob: u64) -> Self {
        if max_data_in_blob > 0 {
            self.config.set_max_data_in_blob(max_data_in_blob);
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// Sets blob file name prefix, e.g. if prefix set to `hellopearl`,
    /// files will be named as `hellopearl.[N].blob`.
    /// Where N - index number of file
    /// If the prefix is empty, param won't be set.
    pub fn blob_file_name_prefix<U: Into<String>>(mut self, blob_file_name_prefix: U) -> Self {
        let prefix = blob_file_name_prefix.into();
        if prefix.is_empty() {
            error!("passed empty file prefix, not set");
        } else {
            self.config.set_blob_file_name_prefix(prefix);
        }
        self
    }

    /// Disables check existence of the record on write.
    /// Writing becomes faster because there is no additional disk access
    /// for searching for duplicates.
    #[must_use]
    pub fn allow_duplicates(mut self) -> Self {
        self.config.set_allow_duplicates(true);
        self
    }

    /// Ignores blobs with corrupted header for index file and continues work with normal
    /// ones (also leaves error logs for bad blobs).
    /// It's worth noticing that index file maybe corrupted likely in case of irregular
    /// stop of the storage: either the disk disconnected or server crashed. In these cases blob
    /// file is likely either corrupted, so there is no a try to restore indices from blob file.
    pub fn ignore_corrupted(mut self) -> Self {
        self.config.set_ignore_corrupted(true);
        self
    }

    /// Sets custom bloom filter config, if not set, use default values.
    #[must_use]
    pub fn set_filter_config(mut self, config: BloomConfig) -> Self {
        let mut index_config = self.config.index().clone();
        index_config.bloom_config = Some(config);
        self.config.set_index(index_config);
        self
    }

    /// Enables or disables data checksum validation during index regeneration
    #[must_use]
    pub fn set_validate_data_during_index_regen(mut self, value: bool) -> Self {
        self.config.set_validate_data_during_index_regen(value);
        self
    }

    /// [Optional]
    /// Sets whether to create work directory if its missing on storage initialization.
    /// Default value is `true`
    #[must_use]
    pub fn create_work_dir(mut self, create: bool) -> Self {
        self.config.set_create_work_dir(create);
        self
    }

    /// Sets semaphore for index dumping on blob change.
    /// Parallel saving of indexes onto the disk will be limited by this semaphore.
    /// This can prevent disk overusage in systems with multiple pearls.
    pub fn set_dump_sem(mut self, dump_sem: Arc<Semaphore>) -> Self {
        self.config.set_dump_sem(dump_sem);
        self
    }

    /// Sets bloom filter group size
    pub fn set_bloom_filter_group_size(mut self, size: usize) -> Self {
        self.config.set_bloom_filter_group_size(size);
        self
    }

    /// Set min and max waiting time for deferred index dump
    pub fn set_deferred_index_dump_times(mut self, min: Duration, max: Duration) -> Self {
        self.config.set_deferred_index_dump_times(min, max);
        self
    }
}
