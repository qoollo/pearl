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
/// # Examples
/// ```
/// use pearl::{Storage, Builder};
///
/// let builder = Builder::new()
///     .work_dir("/tmp/pearl/")
///     .max_blob_size(1_000_000)
///     .max_data_in_blob(1_000_000_000)
///     .blob_file_name_prefix("enough");
/// ```
/// let mut storage = builder.build().unwrap();
/// storage.init().unwrap();
/// ```
#[derive(Debug)]
pub struct Storage {
    twins_count: Arc<AtomicUsize>,
    shared: Arc<Shared>,
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Storage {
    fn drop(&mut self) {
        let twins = self.twins_count.fetch_sub(1, Ordering::Relaxed);
        // 1 is because twin#0 - in observer tread, twin#1 - self
        if twins <= 1 {
            self.shared.need_exit.store(false, Ordering::Relaxed);
            trace!("stop observer thread, await a little");
            thread::sleep(Duration::from_millis(100));
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
    /// Creates new uninitialized storage with provided config
    pub fn new(config: Config) -> Self {
        Self {
            twins_count: Arc::new(AtomicUsize::new(0)),
            shared: Arc::new(Shared::new(config)),
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    /// `init(..)` used to prepare all environment to further work.
    ///
    /// Storage works in dir provided to builder. If dir not exist,
    /// creates it, otherwise tries init dir as existing storage.
    pub async fn init<S>(&mut self, mut spawner: S) -> Result<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        // @TODO implement work dir validation
        await!(self.prepare_work_dir())?;

        if let Some(files) = self.work_dir_content()? {
            await!(self.init_from_existing(files))?
        } else {
            await!(self.init_new())?
        };
        let observer_fut_obj: FutureObj<_> =
            Box::new(launch_observer(spawner.clone(), self.clone())).into();
        spawner
            .spawn(observer_fut_obj)
            .map_err(Error::ObserverSpawnFailed)?;
        Ok(())
    }

    fn work_dir_content(&self) -> Result<Option<Vec<fs::DirEntry>>> {
        let wd = Path::new(
            self.shared
                .config
                .work_dir
                .as_ref()
                .ok_or(Error::Unitialized)?,
        );
        let files: Vec<_> = fs::read_dir(wd)
            .map_err(Error::IO)?
            .filter_map(|res_dir_entry| res_dir_entry.map_err(|e| error!("{}", e)).ok())
            .collect();
        if files
            .iter()
            .filter_map(|file| Some(file.file_name().as_os_str().to_str()?.to_owned()))
            .find(|name| name.ends_with(BLOB_FILE_EXTENSION))
            .is_none()
        {
            debug!("working dir is unitialized, starting empty storage");
            Ok(None)
        } else {
            debug!("working dir contains files, try init existing");
            Ok(Some(files))
        }
    }

    /// # Description
    /// Writes bytes `value` to active blob
    /// If active blob reaches it limit, create new and close old
    /// Returns number of bytes, written to blob
    /// # Examples
    pub async fn write(self, record: Record) -> Result<()> {
        trace!("await for inner lock");
        let mut inner = await!(self.inner.lock());
        trace!("return write future");
        let blob = inner.active_blob.as_mut().ok_or(Error::ActiveBlobNotSet)?;
        await!(blob.write(record)).map_err(Error::BlobError)
    }

    fn next_blob_name(&self) -> Result<blob::FileName> {
        let next_id = self.shared.next_blob_id.fetch_add(1, Ordering::Relaxed);
        let prefix = self
            .shared
            .config
            .blob_file_name_prefix
            .as_ref()
            .ok_or(Error::Unitialized)?
            .to_owned();
        let dir = self
            .shared
            .config
            .work_dir
            .as_ref()
            .ok_or(Error::Unitialized)?
            .to_owned();
        Ok(blob::FileName::new(
            prefix,
            next_id,
            BLOB_FILE_EXTENSION.to_owned(),
            dir,
        ))
    }

    /// # Description
    /// Reads data with given key to [`Record`], if error ocured or there are no
    /// records with matching key, returns [`Error::RecordNotFound`]
    ///
    /// [`Record`]: ../struct.Record.html
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
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
    /// Stop work dir observer thread
    pub fn close(&mut self) -> Result<()> {
        self.shared.need_exit.store(false, Ordering::Relaxed);
        // @TODO implement
        Ok(())
    }

    /// # Description
    /// Blobs count contains closed blobs and one active, if is some.
    /// # Examples
    /// ```
    /// # use pearl::Builder;
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
        let path = Path::new(
            self.shared
                .config
                .work_dir
                .as_ref()
                .ok_or(Error::Unitialized)?,
        );
        if path.exists() {
            debug!("work dir exists: {}", path.display());
        } else {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path).map_err(Error::IO)?;
        }

        let lock_file_path = path.join(LOCK_FILE);
        if lock_file_path.exists() {
            return Err(Error::WorkDirInUse);
        }

        debug!("try to open lock file: {}", lock_file_path.display());
        let lock_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_file_path)
            .map_err(Error::IO)?;
        await!(self.inner.lock()).lock_file = Some(lock_file);
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
        let dir_content = files.iter().map(DirEntry::path);
        let dir_files = dir_content.filter(|path| path.is_file());
        let mut temp_blobs: Vec<_> = dir_files
            .filter(|path| {
                path.extension()
                    .map(|os_str| os_str.to_str().unwrap())
                    .unwrap_or("")
                    == BLOB_FILE_EXTENSION
            })
            .filter_map(|path| {
                let blob = Blob::from_file(path.clone()).ok()?;
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
            inner.max_id().map(|i| i + 1).unwrap_or(0),
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

    fn max_id(&self) -> Option<usize> {
        let active_blob_id = self.active_blob.as_ref().map(|blob| blob.id());
        let blobs_max_id = self.blobs.last().map(Blob::id);
        active_blob_id.max(blobs_max_id)
    }
}

#[derive(Debug)]
struct Shared {
    config: Config,
    next_blob_id: AtomicUsize,
    need_exit: Arc<AtomicBool>,
}

impl Shared {
    fn new(config: Config) -> Self {
        Self {
            config,
            next_blob_id: AtomicUsize::new(0),
            need_exit: Arc::new(AtomicBool::new(true)),
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
        } else if self.storage.shared.need_exit.load(Ordering::Relaxed) {
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
    WorkDirInUse,
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
