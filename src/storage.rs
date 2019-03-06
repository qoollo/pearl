use std::{
    fs::{self, DirEntry},
    io::{self, ErrorKind},
    path::Path,
};

use crate::blob::Blob;

/// Used to create a storage, configure it and manage
/// Examples
#[derive(Debug)]
pub struct Storage {
    config: Config,
    opened_blob: Box<Option<Blob>>,
    blobs: Vec<Blob>,
}

impl Storage {
    /// Creates a new instance of a storage
    /// # Examples
    ///
    /// ```
    /// use pearl::{Storage, Builder};
    /// let mut stor = Builder::new().build();
    /// let res = stor.init();
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
    /// Blobs count contains closed blobs and one opened, if is some
    /// # Examples
    /// ```
    /// use pearl::Builder;
    ///
    /// let mut storage = Builder::new().work_dir("/tmp/pearl/").build();
    /// storage.init();
    /// assert_eq!(storage.blobs_count(), 1);
    /// ```
    pub fn blobs_count(&self) -> usize {
        self.blobs.len() + if self.opened_blob.is_some() { 1 } else { 0 }
    }
}

impl Storage {
    fn prepare_work_dir(&self) -> io::Result<()> {
        let wd = Path::new(&self.config.work_dir);
        if let Err(e) = fs::read_dir(wd) {
            match e.kind() {
                ErrorKind::NotFound => {
                    error!("\"{}\" not found", self.config.work_dir);
                }
                _ => return Err(e),
            }
        }
        debug!(
            "create work dir recursively: {}",
            wd.to_str().unwrap_or("failed to convert path to str")
        );
        fs::create_dir_all(wd)?;
        Ok(())
    }

    // @TODO specify more useful error type
    #[inline]
    fn init_opened_blob(&mut self) -> Result<(), ()> {
        self.opened_blob = Box::new(Some(Default::default()));
        Ok(())
    }

    // @TODO specify more useful error type
    fn init_new(&mut self) -> Result<(), ()> {
        self.init_opened_blob()
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
                    None
                }
            })
            .collect();
        if let Some(blob_index) = self.search_for_opened_blob() {
            self.opened_blob = Box::new(Some(self.blobs.remove(blob_index)));
            Ok(())
        } else {
            self.init_opened_blob()
        }
    }

    fn search_for_opened_blob(&self) -> Option<usize> {
        let mut opened_blob_id = None;
        self.blobs.iter().enumerate().find(|(i, blob)| {
            let res = blob.is_opened();
            if res {
                opened_blob_id = Some(*i);
            }
            res
        });
        opened_blob_id
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            config: Default::default(),
            opened_blob: Box::default(),
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
    pub fn build(self) -> Storage {
        Storage {
            config: self.config,
            ..Default::default()
        }
    }

    /// Sets a string with work dir as pattern for blob naming
    /// Examples
    pub fn work_dir<S: Into<&'a str>>(mut self, work_dir: S) -> Self {
        // @TODO check path
        self.config.work_dir = work_dir.into().to_string();
        self
    }
}

/// Description
/// Examples
#[derive(Default, Debug)]
struct Config {
    work_dir: String,
    max_blobs_num: usize,
    max_blob_size: usize,
    max_data_in_blob: usize,
    blob_file_name_pattern: String,
}
