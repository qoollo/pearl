use super::core::{Config, Error, Result, Storage};
use std::path::PathBuf;

/// `Builder` used for initializing a `Storage`.
/// Required params:
///  - [`work_dir`] - where `Storage` will keep blob and index files
///  - [`max_blob_size`] - upper limit of blob file size
///  - [`max_data_in_blob`] - maximum number of records in one blob
///  - [`blob_file_name_prefix`] - blob file name pattern: {prefix}.{id}.{ext}
///  - [`key_size`] - const key size in bytes
///         
/// # Example
/// ```
/// let storage = Builder::new()
///         .blob_file_name_prefix("benchmark")
///         .max_blob_size(max_blob_size)
///         .max_data_in_blob(max_data_in_blob)
///         .work_dir(tmp_dir.join("pearl_benchmark"))
///         .key_size(8)
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
}

impl Builder {
    /// Create new unitialized `Builder`
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates `Storage` based on given configuration,
    /// returns error if not all params are set.
    pub fn build<K>(self) -> Result<Storage<K>> {
        if self.config.blob_file_name_prefix.is_none()
            || self.config.max_data_in_blob.is_none()
            || self.config.max_blob_size.is_none()
            || self.config.blob_file_name_prefix.is_none()
            || self.config.key_size.is_none()
        {
            Err(Error::Uninitialized)
        } else {
            Ok(Storage::new(self.config))
        }
    }

    /// # Description
    /// Sets working directory. If path doesn't exists, Storage will try to create it
    /// at initialization stage.
    pub fn work_dir<S: Into<PathBuf>>(mut self, work_dir: S) -> Self {
        debug!("set work dir");
        let path: PathBuf = work_dir.into();
        debug!("work dir set to: {}", path.display());
        self.config.work_dir = Some(path);
        self
    }

    /// # Description
    /// Sets blob file size approximate limit. When the file size exceeds it,
    /// active blob update is activated.
    /// Must be greater than zero
    pub fn max_blob_size(mut self, max_blob_size: u64) -> Self {
        if max_blob_size > 0 {
            self.config.max_blob_size = Some(max_blob_size);
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// # Description
    /// Limits max number of records in a single blob.
    /// Must be greater than zero
    pub fn max_data_in_blob(mut self, max_data_in_blob: u64) -> Self {
        if max_data_in_blob > 0 {
            self.config.max_data_in_blob = Some(max_data_in_blob);
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// # Description
    /// Sets blob file name prefix, e.g. if prefix set to `hellopearl`,
    /// files will be named as `hellopearl.[N].blob`.
    /// Where N - index number of file
    /// If the prefix is empty, param won't be set.
    pub fn blob_file_name_prefix<U: Into<String>>(mut self, blob_file_name_prefix: U) -> Self {
        let prefix = blob_file_name_prefix.into();
        if prefix.is_empty() {
            error!("passed empty file prefix, not set");
        } else {
            self.config.blob_file_name_prefix = Some(prefix);
        }
        self
    }

    /// # Description
    /// Sets key size limit
    pub fn key_size(mut self, key_size: u16) -> Self {
        self.config.key_size = if key_size > 0 { Some(key_size) } else { None };
        self
    }
}
