use futures::{
    future::{Future, FutureObj, TryFutureExt},
    lock::Mutex,
    stream::{futures_unordered, Stream, StreamExt},
    task::{self, Poll, Spawn, SpawnExt, Waker},
};
use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::{
    blob::{self, Blob},
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
    shared: Arc<Shared>,
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    active_blob: Option<Box<Blob>>,
    blobs: Vec<Blob>,
    lock_file: Option<File>,
}

#[derive(Debug)]
struct Shared {
    config: Config,
    next_blob_id: AtomicUsize,
}

async fn launch_observer<S>(mut spawner: S, storage: Storage)
where
    S: SpawnExt,
{
    let mut observer = Observer {
        storage: Arc::new(storage),
        next_update: Instant::now(),
    };
    while let Some(f) = await!(observer.next()) {
        spawner.spawn(f).unwrap();
    }
}

struct Observer {
    storage: Arc<Storage>,
    next_update: Instant,
}

impl Stream for Observer {
    type Item = FutureObj<'static, ()>;

    fn poll_next(mut self: Pin<&mut Self>, waker: &Waker) -> Poll<Option<Self::Item>> {
        if self.next_update < Instant::now() {
            println!("Observer update");
            self.as_mut().next_update = Instant::now() + Duration::from_millis(1000);
            let storage_cloned = self.storage.clone();
            let fut = update_active_blob(storage_cloned);
            let fut_boxed = Box::new(fut);
            let obj = fut_boxed.into();
            Poll::Ready(Some(obj))
        } else {
            waker.wake();
            Poll::Pending
        }
    }
}

async fn update_active_blob(s: Arc<Storage>) {
    println!("Storage update, lock");
    // @TODO process unwrap explicitly
    let mut inner = await!(s.inner.lock());
    println!("lock acquired");
    let active_size = inner.active_blob.as_ref().unwrap().file_size().unwrap();
    let config_max = s.shared.config.max_blob_size.unwrap();
    let is_full = active_size > config_max
        || inner.active_blob.as_ref().unwrap().records_count() as u64
            >= s.shared.config.max_data_in_blob.unwrap();

    if is_full {
        println!("is full, replace");
        let next = s.next_blob_name().unwrap();
        println!("next name: {:?}", next);
        let new_active = Blob::open_new(next).unwrap().boxed();
        let old_active = inner.active_blob.replace(new_active).unwrap();
        inner.blobs.push(*old_active);
    }
    println!("not full yet, {} > {} = false", active_size, config_max);
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            inner: self.inner.clone(),
        }
    }
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
    pub async fn init<S>(&mut self, mut spawner: S) -> Result<()>
    where
        S: Spawn + Clone + Send + 'static,
    {
        // @TODO implement work dir validation
        await!(self.prepare_work_dir())?;
        let wd = Path::new(
            self.shared
                .config
                .work_dir
                .as_ref()
                .ok_or(Error::Unitialized)?,
        );
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
            await!(self.init_new()).unwrap(); // @TODO handle unwrap explicitly
        } else {
            debug!("working dir contains files, try init existing");
            trace!("ls:");
            files_in_work_dir
                .iter()
                .for_each(|name| trace!("{}", name.file_name().as_os_str().to_str().unwrap())); // @TODO handle unwrap explicitly
            await!(self.init_from_existing(files_in_work_dir)).unwrap(); // @TODO handle unwrap explicitly
        }
        spawner
            .spawn::<FutureObj<'static, ()>>(
                Box::new(launch_observer(spawner.clone(), self.clone())).into(),
            )
            .map_err(Error::ObserverSpawnFailed)?;
        Ok(())
    }

    /// # Description
    /// Writes bytes `value` to active blob
    /// If active blob reaches it limit, create new and close old
    /// Returns number of bytes, written to blob
    /// # Examples

    // @TODO specify more useful error type
    pub async fn write(self, record: Record) -> Result<()> {
        trace!("await for inner lock");
        let mut inner = await!(self.inner.lock());
        trace!("return write future");
        // @TODO process unwrap explicitly
        await!(inner
            .active_blob
            .as_mut()
            .unwrap()
            .write(record)
            .map_err(Error::BlobError))
    }

    async fn max_id(&self) -> Option<usize> {
        let active_blob_id = await!(self.inner.lock())
            .active_blob
            .as_ref()
            .map(|blob| blob.id());
        let blobs_max_id = await!(self.inner.lock())
            .blobs
            .iter()
            .max_by_key(|blob| blob.id())
            .map(Blob::id);
        active_blob_id.max(blobs_max_id)
    }

    fn next_blob_name(&self) -> Result<blob::FileName> {
        let next_id = self.shared.next_blob_id.fetch_add(1, Ordering::Relaxed);
        Ok(blob::FileName::new(
            self.shared
                .config
                .blob_file_name_prefix
                .as_ref()
                .ok_or(Error::Unitialized)?
                .to_owned(),
            next_id,
            BLOB_FILE_EXTENSION.to_owned(),
            self.shared
                .config
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
    pub async fn read(&self, key: Vec<u8>) -> Result<Record> {
        let inner = await!(self.inner.lock());
        if let Some(active_blob) = &inner.active_blob {
            return await!(active_blob.read(key)).map_err(Error::BlobError);
        } else {
            let futs = inner
                .blobs
                .iter()
                .map(|blob| blob.read(key.clone()))
                .collect::<Vec<_>>();
            let mut stream = futures_unordered(futs);
            return await!(stream.next())
                .ok_or(Error::RecordNotFound)?
                .map_err(Error::BlobError);
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
    pub async fn blobs_count(&self) -> usize {
        let inner = await!(self.inner.lock());
        inner.blobs.len() + if inner.active_blob.is_some() { 1 } else { 0 }
    }

    /// # Description
    /// Returns active blob file path, if active blob unitialized - None
    pub async fn active_blob_path(&self) -> Option<PathBuf> {
        Some(await!(self.inner.lock()).active_blob.as_ref()?.path())
    }

    async fn prepare_work_dir(&mut self) -> Result<()> {
        let path = Path::new(self.shared.config.work_dir.as_ref().unwrap()); // @TODO handle unwrap explicitly
        if !path.exists() {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path).map_err(Error::IO)?;
        } else {
            debug!("work dir exists: {}", path.display());
        }
        let lock_file = path.join(LOCK_FILE);
        debug!("try to open lock file: {}", lock_file.display());
        await!(self.inner.lock()).lock_file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_file)
                .map_err(Error::IO)?,
        );
        Ok(())
    }

    async fn init_new(&mut self) -> Result<()> {
        let next = self.next_blob_name()?;
        await!(self.inner.lock()).active_blob =
            Some(Blob::open_new(next).map_err(Error::InitActiveBlob)?.boxed());
        debug!(
            "created new active blob: {}",
            await!(self.inner.lock()).active_blob.as_ref().unwrap().id()
        );
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
        let mut temp_blobs: Vec<_> = files
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
                Some(blob)
            })
            .collect();
        temp_blobs.sort_by_key(Blob::id);
        let active_blob = temp_blobs.pop().unwrap().boxed();
        await!(self.inner.lock()).active_blob = Some(active_blob);
        await!(self.inner.lock()).blobs = temp_blobs;
        self.shared.next_blob_id.store(
            await!(self.max_id()).map(|i| i + 1).unwrap_or(0),
            Ordering::Relaxed,
        );
        Ok(())
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
    ObserverSpawnFailed(task::SpawnError),
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
                shared: Arc::new(Shared {
                    config: self.config.clone(),
                    next_blob_id: AtomicUsize::new(0),
                }),
                inner: Arc::new(Mutex::new(Inner {
                    active_blob: None,
                    blobs: Vec::new(),
                    lock_file: None,
                })),
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
#[derive(Debug, Clone)]
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
