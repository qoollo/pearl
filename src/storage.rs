/// Used to create a storage, configure it and manage
/// Examples
#[derive(Debug)]
pub struct Storage {
    config: Config,
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
    pub fn init(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

    /// Description
    /// Examples
    pub fn write(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

    /// Description
    /// Examples
    pub fn read(&self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

    /// # Description
    /// Closes all file descriptors
    /// # Examples
    pub fn close(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            config: Default::default(),
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
        }
    }

    /// Sets a string with work dir as pattern for blob naming
    /// Examples
    pub fn work_dir<S: Into<&'a str>>(mut self, work_dir: S) -> Self {
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
    blob_file: String,
}
