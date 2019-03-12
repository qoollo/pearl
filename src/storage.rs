use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use crate::blob::Blob;

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
    config: Config,
    active_blob: Box<Option<Blob<K>>>,
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
        // @TODO implement
        Ok(())
    }

    /// Description
    /// Examples

    // @TODO specify more useful error type
    pub fn write(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

    /// Description
    /// Examples

    // @TODO specify more useful error type
    pub fn read(&self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
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
        self.active_blob = Box::new(Some(Default::default()));
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
        self.active_blob = Box::new(Some(self.blobs.pop().ok_or(())?));
        Ok(())
    }
}

impl<K> Default for Storage<K> {
    fn default() -> Self {
        Self {
            config: Default::default(),
            active_blob: Box::default(),
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
    pub fn build<K>(self) -> Storage<K> {
        Storage {
            config: self.config,
            ..Default::default()
        }
    }

    /// # Description
    /// Sets a string with work dir as pattern for blob naming.
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
        debug!("cmp {} with 0", mbs);
        if mbs > 0 {
            self.config.max_blob_size = Some(mbs);
            info!("maximum blob size set to: {}", mbs);
        } else {
            error!("zero size blobs is useless, not set");
        }
        self
    }
}

/// Description
/// Examples
#[derive(Default, Debug)]
struct Config {
    work_dir: Option<PathBuf>,
    max_blob_size: Option<usize>,
    max_data_in_blob: Option<usize>,
    blob_file_name_pattern: Option<String>,
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::fs::*;

    const RO_DIR_NAME: &str = "/tmp/pearl_test_readonly/";
    const FILE_NAME: &str = "/tmp/pearl_test.file";
    const WORK_DIR: &str = "/tmp/pearl_test/";

    #[test]
    fn set_work_dir() {
        let path = Path::new(WORK_DIR);
        create_dir_all(path).unwrap();
        let builder = Builder::new().work_dir(WORK_DIR);
        assert!(builder.config.work_dir.is_some());
        fs::remove_dir(path).unwrap();
    }

    #[test]
    fn set_readonly_work_dir() {
        let path = Path::new(RO_DIR_NAME);
        create_dir_all(path).unwrap();
        let mut perm = Path::new(RO_DIR_NAME).metadata().unwrap().permissions();
        perm.set_readonly(true);
        set_permissions(RO_DIR_NAME, perm).unwrap();
        let mut storage = Builder::new().work_dir(RO_DIR_NAME).build::<usize>();
        assert!(storage.init().is_err());
        fs::remove_dir(path).unwrap();
    }

    #[test]
    fn set_file_as_work_dir() {
        use std::io::Write;

        let path = Path::new(FILE_NAME);
        create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .unwrap();
        file.flush().unwrap();
        let mut storage = Builder::new().work_dir(FILE_NAME).build::<usize>();
        assert!(storage.init().is_err());
        fs::remove_file(path).unwrap();
    }
}
