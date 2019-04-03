use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use crate::{
    blob::{self, Blob, ReadFuture, WriteFuture},
    record::Record,
};

const BLOB_FILE_EXTENSION: &str = "blob";
const LOCK_FILE: &str = "pearl.lock";

/// # Description
/// A specialized Result type
type Result<T> = std::result::Result<T, Error>;

/// # Description
/// Used to create a storage, configure it and manage
/// `K` - type of storage key, must be [Sized](https://doc.rust-lang.org/std/marker/trait.Sized.html)
/// # Examples
/// ```no-run
/// use pearl::{Storage, Builder};
///
/// let mut storage = Builder::new().build::<u32>();
/// storage.init().unwrap();
/// ```
///
#[derive(Debug)]
pub struct Storage {
    config: Config,
    active_blob: Option<Box<Blob>>,
    blobs: Vec<Blob>,
    lock_file: Option<File>,
    next_blob_id: usize,
}

impl Storage {
    /// Creates a new instance of a storage with u32 key
    /// # Examples
    ///
    /// ```no-run
    /// use pearl::{Storage, Builder};
    ///
    /// let mut storage = Builder::new().build::<u32>();
    /// storage.init().unwrap();
    /// ```
    ///
    /// Storage works in dir provided to builder. If dir not exist,
    /// creates it, otherwise tries init dir as existing storage.
    pub fn init(&mut self) -> Result<()> {
        // @TODO implement work dir validation
        self.prepare_work_dir()?;
        let wd = Path::new(self.config.work_dir.as_ref().ok_or(Error::Unitialized)?);
        let files_in_work_dir: Vec<_> = fs::read_dir(wd)
            .map_err(Error::IO)?
            .filter_map(|res_dir_entry| res_dir_entry.map_err(|e| error!("{}", e)).ok())
            .collect();
        if files_in_work_dir
            .iter()
            .map(|file| file.file_name().as_os_str().to_str().unwrap().to_owned())
            .find(|name| name.ends_with(BLOB_FILE_EXTENSION))
            .is_none()
        {
            debug!("working dir is unitialized, starting empty storage");
            self.init_new().unwrap(); // @TODO handle unwrap explicitly
        } else {
            debug!("working dir contains files, try init existing");
            trace!("ls:");
            files_in_work_dir
                .iter()
                .for_each(|name| trace!("{}", name.file_name().as_os_str().to_str().unwrap())); // @TODO handle unwrap explicitly
            self.init_from_existing(files_in_work_dir).unwrap(); // @TODO handle unwrap explicitly
        }
        Ok(())
    }

    /// # Description
    /// Writes bytes `value` to active blob
    /// If active blob reaches it limit, create new and close old
    /// Returns number of bytes, written to blob
    /// # Examples

    // @TODO specify more useful error type
    pub fn write(&mut self, record: &mut Record) -> Result<WriteFuture> {
        if self.is_active_blob_full(record.full_len())? {
            let next = self.next_blob_name()?;
            let new_active = Blob::open_new(next).map_err(Error::BlobError)?.boxed();
            // @TODO process unwrap explicitly
            let old_active = self.active_blob.replace(new_active).unwrap();
            self.blobs.push(*old_active);
        }
        // @TODO process unwrap explicitly
        self.active_blob
            .as_mut()
            .unwrap()
            .write(record)
            .map_err(Error::BlobError)
    }

    fn max_id(&self) -> Option<usize> {
        let active_blob_id = self.active_blob.as_ref().map(|blob| blob.id());
        let blobs_max_id = self.blobs.iter().max_by_key(|blob| blob.id()).map(Blob::id);
        active_blob_id.max(blobs_max_id)
    }

    fn next_blob_name(&self) -> Result<blob::FileName> {
        Ok(blob::FileName::new(
            self.config
                .blob_file_name_prefix
                .as_ref()
                .ok_or(Error::Unitialized)?
                .to_owned(),
            self.next_blob_id,
            BLOB_FILE_EXTENSION.to_owned(),
            self.config
                .work_dir
                .as_ref()
                .ok_or(Error::Unitialized)?
                .to_owned(),
        ))
    }
}

impl Storage {
    /// # Description
    /// Reads data with given key to `Vec<u8>`, if error ocured or there are no
    /// records with matching key, returns `Err(_)`
    // @TODO specify more useful error type
    pub fn read<K>(&mut self, key: &K) -> Result<ReadFuture>
    where
        K: AsRef<[u8]> + Ord,
    {
        if let Some(fut) = self
            .active_blob
            .as_ref()
            .and_then(|active_blob| active_blob.read(&key).ok())
        {
            Ok(fut)
        } else {
            self.blobs
                .iter()
                .find_map(|blob| blob.read(&key).ok())
                .ok_or(Error::RecordNotFound)
        }
    }

    /// # Description
    /// Closes all file descriptors
    /// # Examples

    // @TODO specify more useful error type
    pub fn close(&mut self) -> Result<()> {
        // @TODO implement
        Ok(())
    }

    /// # Description
    /// Blobs count contains closed blobs and one active, if is some.
    /// # Examples
    /// ```no-run
    /// use pearl::Builder;
    /// // key type f64
    /// let mut storage = Builder::new().work_dir("/tmp/pearl/").build::<f64>();
    /// storage.init();
    /// assert_eq!(storage.blobs_count(), 1);
    /// ```
    pub fn blobs_count(&self) -> usize {
        self.blobs.len() + if self.active_blob.is_some() { 1 } else { 0 }
    }

    /// # Description
    /// Returns active blob file path, if active blob unitialized - None
    pub fn active_blob_path(&self) -> Option<PathBuf> {
        Some(self.active_blob.as_ref()?.path())
    }

    fn prepare_work_dir(&mut self) -> Result<()> {
        let path = Path::new(self.config.work_dir.as_ref().unwrap()); // @TODO handle unwrap explicitly
        if !path.exists() {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path).map_err(Error::IO)?;
        } else {
            debug!("work dir exists: {}", path.display());
        }
        let lock_file = path.join(LOCK_FILE);
        debug!("try to open lock file: {}", lock_file.display());
        self.lock_file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_file)
                .map_err(Error::IO)?,
        );
        Ok(())
    }

    fn init_new(&mut self) -> Result<()> {
        let next = self.next_blob_name()?;
        self.active_blob = Some(Blob::open_new(next).map_err(Error::InitActiveBlob)?.boxed());
        debug!(
            "created new active blob: {}",
            self.active_blob.as_ref().unwrap().id()
        );
        Ok(())
    }

    fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
        let mut max_blob_file_index: Option<usize> = None;
        let mut active_blob = None;
        let temp_blobs = files
            .iter()
            .map(DirEntry::path)
            .filter(|path| path.is_file())
            .filter(|path| {
                path.extension()
                    .map(|os_str| os_str.to_str().unwrap())
                    .unwrap_or("")
                    == BLOB_FILE_EXTENSION
            })
            .filter_map(|path| {
                let blob = Blob::from_file(path.clone()).unwrap();
                blob.check_data_consistency()
                    .map_err(|e| error!("Check data consistency failed: {:?}", e))
                    .ok()?;
                let id = blob.id();
                if let Some(i) = max_blob_file_index.as_mut() {
                    if id > *i {
                        *i = id;
                        let temp_blob = active_blob.take()?;
                        active_blob = Some(blob);
                        Some(temp_blob)
                    } else {
                        Some(blob)
                    }
                } else {
                    max_blob_file_index = Some(id);
                    active_blob = Some(blob);
                    None
                }
            })
            .collect();
        self.active_blob = Some(Box::new(active_blob.take().ok_or(Error::ActiveBlobNotSet)?));
        self.blobs = temp_blobs;
        self.blobs.sort_by_key(Blob::id);
        self.next_blob_id = self.max_id().map(|i| i + 1).unwrap_or(0);
        Ok(())
    }

    // @TODO handle unwrap explicitly
    fn is_active_blob_full(&self, next_record_size: u64) -> Result<bool> {
        Ok(self
            .active_blob
            .as_ref()
            .ok_or(Error::ActiveBlobNotSet)?
            .file_size()
            .map_err(Error::BlobError)?
            + next_record_size
            > self.config.max_blob_size.unwrap()
            || self.active_blob.as_ref().unwrap().records_count().unwrap() as u64
                >= self.config.max_data_in_blob.unwrap())
    }
}

#[derive(Debug)]
pub enum Error {
    ActiveBlobNotSet,
    InitActiveBlob(blob::Error),
    BlobError(blob::Error),
    WrongConfig,
    Unitialized,
    IO(io::Error),
    RecordNotFound,
}

/// `Builder` used for initializing a `Storage`.
/// Examples
#[derive(Default, Debug)]
pub struct Builder {
    config: Config,
}

impl<'a> Builder {
    /// Initializes the `Builder` with defaults
    /// Examples
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates `Storage` based on given configuration
    /// Examples
    pub fn build(self) -> Result<Storage> {
        if self.config.blob_file_name_prefix.is_none()
            || self.config.max_data_in_blob.is_none()
            || self.config.max_blob_size.is_none()
            || self.config.blob_file_name_prefix.is_none()
        {
            Err(Error::Unitialized)
        } else {
            Ok(Storage {
                config: self.config,
                active_blob: None,
                blobs: Vec::new(),
                lock_file: None,
                next_blob_id: 0,
            })
        }
    }

    /// # Description
    /// Sets a string with work dir as prefix for blob naming.
    /// If path not exists, Storage will try to create at initialization stage.
    /// # Examples
    /// ```no-run
    /// let builder = Builder::new().work_dir("/tmp/pearl/");
    /// ```
    pub fn work_dir<S: Into<PathBuf>>(mut self, work_dir: S) -> Self {
        debug!("set work dir");
        let path: PathBuf = work_dir.into();
        info!("work dir set to: {}", path.display());
        self.config.work_dir = Some(path);
        self
    }

    /// # Description
    /// Sets blob file max size
    /// Must be greater than zero
    pub fn max_blob_size(mut self, max_blob_size: u64) -> Self {
        if max_blob_size > 0 {
            self.config.max_blob_size = Some(max_blob_size);
            info!(
                "maximum blob size set to: {}",
                self.config.max_blob_size.unwrap()
            );
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// # Description
    /// Sets max number of records in single blob
    /// Must be greater than zero
    pub fn max_data_in_blob(mut self, max_data_in_blob: u64) -> Self {
        if max_data_in_blob > 0 {
            self.config.max_data_in_blob = Some(max_data_in_blob);
            info!(
                "max number of records in blob set to: {}",
                self.config.max_data_in_blob.unwrap()
            );
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }

    /// # Description
    /// Sets blob file name prefix, e.g. if prefix set to `hellopearl`,
    /// files will be named as `hellopearl.[N].blob`.
    /// Where N - index number of file
    /// Must be not empty
    pub fn blob_file_name_prefix<U: Into<String>>(mut self, blob_file_name_prefix: U) -> Self {
        let prefix = blob_file_name_prefix.into();
        if !prefix.is_empty() {
            self.config.blob_file_name_prefix = Some(prefix);
            info!(
                "blob file format: {}.{{}}.blob",
                self.config.blob_file_name_prefix.as_ref().unwrap()
            );
        } else {
            error!("passed empty file prefix, not set");
        }
        self
    }
}

/// Description
/// Examples
#[derive(Debug)]
struct Config {
    work_dir: Option<PathBuf>,
    max_blob_size: Option<u64>,
    max_data_in_blob: Option<u64>,
    blob_file_name_prefix: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            work_dir: None,
            max_blob_size: None,
            max_data_in_blob: None,
            blob_file_name_prefix: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fs, Builder, OpenOptions};
    use std::{
        env,
        fs::{create_dir_all, set_permissions},
    };

    const RO_DIR_NAME: &str = "pearl_test_readonly/";
    const FILE_NAME: &str = "pearl_test.file";
    const WORK_DIR: &str = "pearl_test/";

    #[test]
    fn set_work_dir() {
        let path = env::temp_dir().join(WORK_DIR);
        create_dir_all(&path).unwrap();
        let builder = Builder::new().work_dir(&path);
        assert!(builder.config.work_dir.is_some());
        fs::remove_dir(path).unwrap();
    }

    #[test]
    fn set_readonly_work_dir() {
        let path = env::temp_dir().join(RO_DIR_NAME);
        create_dir_all(&path).unwrap();
        let mut perm = path.metadata().unwrap().permissions();
        perm.set_readonly(true);
        set_permissions(&path, perm).unwrap();
        let mut storage = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000u64)
            .max_data_in_blob(1_000)
            .build()
            .unwrap();
        assert!(storage.init().is_err());
        fs::remove_dir(path).unwrap();
    }

    #[test]
    fn set_file_as_work_dir() {
        use std::io::Write;

        let path = env::temp_dir().join(FILE_NAME);
        create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.flush().unwrap();
        let mut storage = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000)
            .max_data_in_blob(1_000)
            .build()
            .unwrap();
        assert!(storage.init().is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn set_zero_max_data_in_blob() {
        let builder = Builder::new().max_data_in_blob(0);
        assert!(builder.config.max_data_in_blob.is_none());
    }

    #[test]
    fn set_zero_max_blob_size() {
        let builder = Builder::new().max_blob_size(0);
        assert!(builder.config.max_blob_size.is_none());
    }
    #[test]
    fn set_empty_prefix() {
        let builder = Builder::new().blob_file_name_prefix("");
        assert!(builder.config.blob_file_name_prefix.is_none());
    }
}
