use futures::{
    executor::block_on,
    future::{FutureExt, FutureObj},
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
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
pub type Result<T> = std::result::Result<T, Error>;

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
    twins_count: Arc<AtomicUsize>,
    shared: Arc<Shared>,
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Storage {
    fn drop(&mut self) {
        // self.inner.
        let twins = self.twins_count.fetch_sub(1, Ordering::Relaxed);
        if twins == 1 {
            self.shared.observer_state.store(false, Ordering::Relaxed);
            trace!("stop observer thread, await 1s");
            thread::sleep(Duration::from_millis(100));
        } else if twins < 1 {
            self.shared.observer_state.store(false, Ordering::Relaxed);
        }
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        self.twins_count.fetch_add(1, Ordering::Relaxed);
        let twins_count = self.twins_count.clone();
        Self {
            twins_count,
            shared: self.shared.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl Storage {
    /// Creates new uninitialized storage with given config
    pub fn new(config: Config) -> Self {
        Self {
            twins_count: Arc::new(AtomicUsize::new(0)),
            shared: Arc::new(Shared::new(config)),
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

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
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
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
        await!(inner.active_blob.as_mut().unwrap().write(record)).map_err(Error::BlobError)
    }

    async fn max_id(&self) -> Option<usize> {
        let inner = await!(self.inner.lock());
        let active_blob_id = inner.active_blob.as_ref().map(|blob| blob.id());
        let blobs_max_id = inner
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

    /// # Description
    /// Reads data with given key to `Vec<u8>`, if error ocured or there are no
    /// records with matching key, returns `Err(_)`
    // @TODO specify more useful error type
    pub async fn read(&self, key: Vec<u8>) -> Result<Record> {
        let inner = await!(self.inner.lock());
        if let Some(active_blob) = &inner.active_blob {
            await!(active_blob.read(key)).map_err(Error::BlobError)
        } else {
            let futs = inner
                .blobs
                .iter()
                .map(|blob| blob.read(key.clone()))
                .collect::<Vec<_>>();
            let mut stream = futures_unordered(futs);
            await!(stream.next())
                .ok_or(Error::RecordNotFound)?
                .map_err(Error::BlobError)
        }
    }

    /// # Description
    /// Closes all file descriptors
    /// # Examples

    // @TODO specify more useful error type
    pub fn close(&mut self) -> Result<()> {
        self.shared.observer_state.store(false, Ordering::Relaxed);
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
        let mut inner = await!(self.inner.lock());
        let next = self.next_blob_name()?;
        inner.active_blob = Some(Blob::open_new(next).map_err(Error::InitActiveBlob)?.boxed());
        debug!(
            "created new active blob: {}",
            inner.active_blob.as_ref().unwrap().id()
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
        let mut inner = await!(self.inner.lock());
        inner.active_blob = Some(active_blob);
        inner.blobs = temp_blobs;
        self.shared.next_blob_id.store(
            await!(self.max_id()).map(|i| i + 1).unwrap_or(0),
            Ordering::Relaxed,
        );
        Ok(())
    }
}

#[derive(Debug)]
struct Inner {
    active_blob: Option<Box<Blob>>,
    blobs: Vec<Blob>,
    lock_file: Option<File>,
}

impl Inner {
    fn new() -> Self {
        Self {
            active_blob: None,
            blobs: Vec::new(),
            lock_file: None,
        }
    }
}

#[derive(Debug)]
struct Shared {
    config: Config,
    next_blob_id: AtomicUsize,
    observer_state: Arc<AtomicBool>,
}

impl Shared {
    fn new(config: Config) -> Self {
        Self {
            config,
            next_blob_id: AtomicUsize::new(0),
            observer_state: Arc::new(AtomicBool::new(true)),
        }
    }
}

struct Observer<S>
where
    S: SpawnExt + Send + 'static,
{
    spawner: S,
    storage: Arc<Storage>,
    next_update: Instant,
    update_interval: Duration,
}

impl<S> Observer<S>
where
    S: SpawnExt + Send + 'static + Unpin + Sync,
{
    fn run(mut self) {
        while let Some(f) = block_on(self.next()) {
            self.spawner
                .spawn(f.map(|r| {
                    r.unwrap();
                }))
                .unwrap();
            thread::sleep(self.update_interval);
        }
        trace!("observer stopped");
    }
}

impl<S> Stream for Observer<S>
where
    S: SpawnExt + Send + 'static + Unpin,
{
    type Item = FutureObj<'static, Result<()>>;

    fn poll_next(mut self: Pin<&mut Self>, waker: &Waker) -> Poll<Option<Self::Item>> {
        let now = Instant::now();
        if self.next_update < now {
            trace!("Observer update");
            self.as_mut().next_update = now + self.update_interval;
            let storage_cloned = self.storage.clone();
            let fut = update_active_blob(storage_cloned);
            let fut_boxed = Box::new(fut);
            let obj = FutureObj::new(fut_boxed);
            Poll::Ready(Some(obj))
        } else if self.storage.shared.observer_state.load(Ordering::Relaxed) {
            waker.wake();
            Poll::Pending
        } else {
            trace!("observer state: false");
            Poll::Ready(None)
        }
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

impl From<blob::Error> for Error {
    fn from(blob_error: blob::Error) -> Self {
        Error::BlobError(blob_error)
    }
}

/// Description
/// Examples
#[derive(Debug, Clone)]
pub struct Config {
    pub work_dir: Option<PathBuf>,
    pub max_blob_size: Option<u64>,
    pub max_data_in_blob: Option<u64>,
    pub blob_file_name_prefix: Option<String>,
    pub update_interval_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            work_dir: None,
            max_blob_size: None,
            max_data_in_blob: None,
            blob_file_name_prefix: None,
            update_interval_ms: 100,
        }
    }
}

async fn launch_observer<S>(spawner: S, storage: Storage)
where
    S: SpawnExt + Send + 'static + Unpin + Sync,
{
    let observer = Observer {
        update_interval: Duration::from_millis(storage.shared.config.update_interval_ms),
        storage: Arc::new(storage),
        next_update: Instant::now(),
        spawner,
    };
    thread::spawn(move || observer.run());
}

async fn update_active_blob(s: Arc<Storage>) -> Result<()> {
    trace!("Storage update, lock");
    // @TODO process unwrap explicitly
    let mut inner = await!(s.inner.lock());
    trace!("lock acquired");
    let active_blob = inner.active_blob.as_ref().ok_or(Error::ActiveBlobNotSet)?;
    let active_size = active_blob.file_size()?;
    let config_max = s.shared.config.max_blob_size.ok_or(Error::Unitialized)?;
    let is_full = active_size > config_max
        || active_blob.records_count() as u64
            >= s.shared.config.max_data_in_blob.ok_or(Error::Unitialized)?;

    if is_full {
        trace!("is full, replace");
        let next = s.next_blob_name()?;
        trace!("next name: {:?}", next);
        let new_active = Blob::open_new(next)?.boxed();
        let old_active = inner
            .active_blob
            .replace(new_active)
            .ok_or(Error::ActiveBlobNotSet)?;
        inner.blobs.push(*old_active);
    }
    Ok(())
}
