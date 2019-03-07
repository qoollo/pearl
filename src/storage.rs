use std::{
    fs::{self, DirEntry},
    io,
    path::{Path, PathBuf},
};

use crate::blob::Blob;

/// # Description
/// Used to create a storage, configure it and manage
/// `T` - type of storage key, must be [Sized](https://doc.rust-lang.org/std/marker/trait.Sized.html)
/// # Examples
/// ```
/// use pearl::{Storage, Builder};
///
/// let mut storage = Builder::new().build::<u32>();
/// storage.init().unwrap();
/// ```
///
#[derive(Debug)]
pub struct Storage<T> {
    config: Config,
    active_blob: Box<Option<Blob<T>>>,
    blobs: Vec<Blob<T>>,
}

impl<T> Storage<T>
where
    T: Default,
{
    /// Creates a new instance of a storage with u32 key
    /// # Examples
    ///
    /// ```
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
        let wd = Path::new(&self.config.work_dir);
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
    /// ```
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

impl<T> Storage<T>
where
    T: Default,
{
    fn prepare_work_dir(&self) -> io::Result<()> {
        let path = Path::new(&self.config.work_dir);
        if !path.exists() {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path)
        } else {
            debug!("work dir exists: {}", path.display());
            Ok(())
        }
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

impl<T> Default for Storage<T> {
    fn default() -> Self {
        Self {
            config: Default::default(),
            active_blob: Box::default(),
            blobs: Vec::new(),
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
    pub fn build<T>(self) -> Storage<T> {
        Storage {
            config: self.config,
            ..Default::default()
        }
    }

    /// # Description
    /// Sets a string with work dir as pattern for blob naming.
    /// If path not exists, Storage will try to create at initialization stage.
    /// # Panics
    /// Panics if path is in readonly mode or path isn't a directory
    /// # Examples
    /// ```no-run
    /// let builder = Builder::new().work_dir("/tmp/pearl/");
    /// ```
    pub fn work_dir<S: Into<PathBuf>>(mut self, work_dir: S) -> Self {
        debug!("set work dir");
        let path: PathBuf = work_dir.into();
        debug!("check path existence");
        if !path.exists() {
            warn!("no such file or directory: {}", path.display());
        }
        debug!("load metadata");
        let metadata = path
            .metadata()
            .map_err(|e| error!("metadata load failed: {}", e))
            .unwrap();
        let permissions = metadata.permissions();
        debug!("check is path readonly");
        debug!("check is path a dir");
        if permissions.readonly() {
            error!("{} is in readonly mode", path.display());
            panic!("work dir must have write permission");
        } else if !metadata.is_dir() {
            error!("{} is not a directory", path.display());
            panic!("work dir must be a directory");
        }
        info!("work dir set: {}", path.display());
        self.config.work_dir = path;
        self
    }
}

/// Description
/// Examples
#[derive(Default, Debug)]
struct Config {
    work_dir: PathBuf,
    max_blobs_num: usize,
    max_blob_size: usize,
    max_data_in_blob: usize,
    blob_file_name_pattern: String,
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::fs::*;

    const RO_DIR_NAME: &str = "/tmp/pearl_test/readonly_folder/";
    const FILE_NAME: &str = "/tmp/pearl_test/work_dir.file";
    const WORK_DIR: &str = "/tmp/pearl_test/";

    #[test]
    #[should_panic]
    fn set_readonly_work_dir() {
        create_dir_all(RO_DIR_NAME).unwrap();
        let mut perm = Path::new(RO_DIR_NAME).metadata().unwrap().permissions();
        perm.set_readonly(true);
        set_permissions(RO_DIR_NAME, perm).unwrap();
        let _ = Builder::new().work_dir(RO_DIR_NAME);
    }

    #[test]
    #[should_panic]
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
        let _ = Builder::new().work_dir(FILE_NAME);
    }

    #[test]
    fn set_work_dir() {
        create_dir_all(Path::new(WORK_DIR)).unwrap();
        let _ = Builder::new().work_dir(WORK_DIR);
    }
}
