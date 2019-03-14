use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use crate::{
    blob::Blob,
    record::Record,
};

const LOCK_FILE: &str = "pearl.lock";

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
pub struct Storage<K> {
    initialized: Option<()>,
    config: Config,
    active_blob: Option<Box<Blob<K>>>,
    blobs: Vec<Blob<K>>,
    lock_file: Option<File>,
}

impl<K> Storage<K>
where
    K: Default,
{
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
    pub fn init(&mut self) -> io::Result<()> {
        // @TODO implement work dir validation
        self.prepare_work_dir()?;
        let wd = Path::new(self.config.work_dir.as_ref().unwrap()); // @TODO handle unwrap explicitly
        let files_in_work_dir: Vec<_> = fs::read_dir(wd)?
            .map(std::result::Result::unwrap) // @TODO handle unwrap explicitly
            .collect();
        if files_in_work_dir.is_empty() {
            debug!("working dir is empty, starting empty storage");
            self.init_new().unwrap(); // @TODO handle unwrap explicitly
        } else {
            debug!("working dir contains files, try init existing");
            trace!("ls:");
            files_in_work_dir
                .iter()
                .for_each(|name| trace!("{}", name.file_name().as_os_str().to_str().unwrap())); // @TODO handle unwrap explicitly
            self.init_from_existing(files_in_work_dir).unwrap(); // @TODO handle unwrap explicitly
        }
        self.initialized = Some(());
        Ok(())
    }

    /// # Description
    /// Writes bytes `value` to active blob
    /// If active blob reaches it limit, create new and close old
    /// Returns number of bytes, written to blob
    /// # Examples

    // @TODO specify more useful error type
    pub fn write(&mut self, record: Record<K>) -> Result<(), ()> {
        self.initialized.ok_or(())?;
        // @TODO process unwrap explicitly
        if self.active_blob.as_ref().unwrap().size()? + record.size()
            > self.config.max_blob_size.unwrap()
            || self.active_blob.as_ref().unwrap().count()? >= self.config.max_data_in_blob.unwrap()
        {
            let new_active = Box::new(Default::default());
            // @TODO process unwrap explicitly
            let mut old_active = self.active_blob.replace(new_active).unwrap();
            old_active.flush()?;
            self.blobs.push(*old_active);
        }
        // @TODO process unwrap explicitly
        self.active_blob.as_mut().unwrap().write(record)
    }

    /// # Description
    /// Reads data with given key to `Vec<u8>`, if error ocured or there are no
    /// records with matching key, returns `Err(_)`

    // @TODO specify more useful error type
    pub fn read(&self, key: &K) -> Result<Record<K>, ()> {
        // @TODO match error in map_err
        if let Ok(r) = self
            .active_blob
            .as_ref()
            .ok_or(())?
            .read(key)
            .map_err(|_e| debug!("no records in active blob"))
        {
            Ok(r)
        } else if let Some(r) = self.blobs.iter().find_map(|blob| {
            blob.read(key)
                .map_err(|_e| debug!("no records with key in blob"))
                .ok()
        }) {
            Ok(r)
        } else {
            Err(())
        }
    }

    /// # Description
    /// Closes all file descriptors
    /// # Examples

    // @TODO specify more useful error type
    pub fn close(&mut self) -> Result<(), ()> {
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
}

impl<K> Storage<K>
where
    K: Default,
{
    fn prepare_work_dir(&mut self) -> io::Result<()> {
        let path = Path::new(self.config.work_dir.as_ref().unwrap()); // @TODO handle unwrap explicitly
        if !path.exists() {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path)?;
        } else {
            debug!("work dir exists: {}", path.display());
        }
        let lock_file = path.join(LOCK_FILE);
        debug!("try to open lock file: {}", lock_file.display());
        self.lock_file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_file)?,
        );
        Ok(())
    }

    // @TODO specify more useful error type
    #[inline]
    fn init_active_blob(&mut self) -> Result<(), ()> {
        self.active_blob = Some(Box::new(Default::default()));
        Ok(())
    }

    // @TODO specify more useful error type
    fn init_new(&mut self) -> Result<(), ()> {
        self.init_active_blob()
    }

    // @TODO specify more useful error type
    fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<(), ()> {
        self.blobs = files
            .iter()
            .filter_map(|entry| {
                // @TODO implement more file validations
                if entry.metadata().ok()?.is_file() {
                    Some(Blob::from_file(entry.path()).ok()?)
                } else {
                    debug!("skipping file \"{}\"", entry.path().display());
                    None
                }
            })
            .collect();
        self.set_active_blob()
    }

    // @TODO specify more useful error type
    fn set_active_blob(&mut self) -> Result<(), ()> {
        // @TODO Search for last active blob by index extracted from file name
        // @TODO Check whether last blob is active or closed
        self.active_blob = Some(Box::new(self.blobs.pop().ok_or(())?));
        Ok(())
    }
}

impl<K> Default for Storage<K> {
    fn default() -> Self {
        Self {
            initialized: None,
            config: Default::default(),
            active_blob: None,
            blobs: Vec::new(),
            lock_file: None,
        }
    }
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
    pub fn build<K>(self) -> Result<Storage<K>, ()> {
        if self.config.blob_file_name_prefix.is_none()
            || self.config.max_data_in_blob.is_none()
            || self.config.max_blob_size.is_none()
            || self.config.blob_file_name_prefix.is_none()
        {
            Err(())
        } else {
            Ok(Storage {
                config: self.config,
                ..Default::default()
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
    pub fn max_blob_size<U: Into<usize>>(mut self, max_blob_size: U) -> Self {
        let mbs = max_blob_size.into();
        if mbs > 0 {
            self.config.max_blob_size = Some(mbs);
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
    pub fn max_data_in_blob<U: Into<usize>>(mut self, max_data_in_blob: U) -> Self {
        let mdib = max_data_in_blob.into();
        if mdib > 0 {
            self.config.max_data_in_blob = Some(mdib);
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
    max_blob_size: Option<usize>,
    max_data_in_blob: Option<usize>,
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
            .max_blob_size(1_000_000usize)
            .max_data_in_blob(1_000usize)
            .build::<usize>()
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
            .max_blob_size(1_000_000usize)
            .max_data_in_blob(1_000usize)
            .build::<usize>()
            .unwrap();
        assert!(storage.init().is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn set_zero_max_data_in_blob() {
        let builder = Builder::new().max_data_in_blob(0usize);
        assert!(builder.config.max_data_in_blob.is_none());
    }

    #[test]
    fn set_zero_max_blob_size() {
        let builder = Builder::new().max_blob_size(0usize);
        assert!(builder.config.max_blob_size.is_none());
    }
    #[test]
    fn set_empty_prefix() {
        let builder = Builder::new().blob_file_name_prefix("");
        assert!(builder.config.blob_file_name_prefix.is_none());
    }
}
