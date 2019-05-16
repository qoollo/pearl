use futures::{
    executor::block_on,
    future::{self, FutureExt, FutureObj},
    lock::Mutex,
    stream::{futures_unordered::FuturesUnordered, Stream, StreamExt},
    task::{self, Context, Poll, Spawn, SpawnExt},
};
use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    marker::PhantomData,
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
    blob::{self, Blob, SimpleIndex},
    record::Record,
};

const BLOB_FILE_EXTENSION: &str = "blob";
const LOCK_FILE: &str = "pearl.lock";

/// # Description
/// A specialized storage result type
pub type Result<T> = std::result::Result<T, Error>;

/// # Description
/// A main storage struct. This type is clonable, cloning it will only create a new reference,
/// not a new storage.
/// Storage has a type parameter K.
/// To perform read/write operations K must implement [`Key`] trait.
///
/// # Examples
/// ```
/// use pearl::{Storage, Builder, Key};
/// use futures::executor::ThreadPool;
///
/// let mut pool = ThreadPool::new().unwrap();
/// let mut storage: Storage<String> = Builder::new()
///     .work_dir("/tmp/pearl/")
///     .max_blob_size(1_000_000)
///     .max_data_in_blob(1_000_000_000)
///     .blob_file_name_prefix("pearl-test")
///     .build()
///     .unwrap();
/// pool.run(storage.init(pool.clone())).unwrap();
/// ```
/// [`Key`]: trait.Key.html
#[derive(Debug)]
pub struct Storage<K> {
    inner: Inner,
    marker: PhantomData<K>,
}

#[derive(Debug)]
struct Inner {
    config: Config,
    safe: Arc<Mutex<Safe>>,
    next_blob_id: Arc<AtomicUsize>,
    need_exit: Arc<AtomicBool>,
    twins_count: Arc<AtomicUsize>,
}

struct Safe {
    active_blob: Option<Box<Blob<SimpleIndex>>>,
    blobs: Vec<Blob<SimpleIndex>>,
    lock_file: Option<File>,
}

impl<K> Drop for Storage<K> {
    fn drop(&mut self) {
        let twins = self.inner.twins_count.fetch_sub(1, Ordering::Relaxed);
        // 1 is because twin#0 - in observer thread, twin#1 - self
        if twins <= 1 {
            self.inner.need_exit.store(false, Ordering::Relaxed);
            trace!("stop observer thread, await a little");
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl<K> Clone for Storage<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

fn work_dir_content(wd: &Path) -> Result<Option<Vec<fs::DirEntry>>> {
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
impl<K> Storage<K> {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            inner: Inner::new(config),
            marker: PhantomData,
        }
    }

    /// [`init()`] used to prepare all environment to further work.
    ///
    /// Storage works in directory provided to builder. If directory don't exist,
    /// storage creates it, otherwise tries to init existing storage.
    ///
    /// [`init()`]: struct.Storage.html#method.init
    pub async fn init<S>(&mut self, mut spawner: S) -> Result<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        // @TODO implement work dir validation
        await!(self.prepare_work_dir())?;

        let cont_res = work_dir_content(
            self.inner
                .config
                .work_dir
                .as_ref()
                .ok_or(Error::Uninitialized)?,
        );

        if let Some(files) = cont_res? {
            await!(self.init_from_existing(files))?
        } else {
            await!(self.init_new())?
        };
        let observer_fut_obj: FutureObj<_> =
            Box::new(launch_observer(spawner.clone(), self.inner.clone())).into();
        spawner
            .spawn(observer_fut_obj)
            .map_err(Error::ObserverSpawnFailed)?;
        Ok(())
    }

    /// # Description
    /// Writes `data` to active blob asyncronously. If active blob reaches it limit, creates new
    /// and closes old.
    /// # Examples
    /// ```no-run
    /// block_on(async {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     let data = b"async written to blob".to_vec();
    ///     await!(storage.write(key, data))
    /// )};
    /// ```
    pub async fn write(self, key: impl Key, value: Vec<u8>) -> Result<()> {
        let record = Record::new(key, value);
        trace!("await for inner lock");
        let mut safe = await!(self.inner.safe.lock());
        trace!("return write future");
        let blob = safe.active_blob.as_mut().ok_or(Error::ActiveBlobNotSet)?;
        await!(blob.write(record)).map_err(Error::BlobError)
    }

    /// # Description
    /// Reads data with given key, if error ocured or there are no records with matching
    /// key, returns [`Error::RecordNotFound`]
    /// # Examples
    /// ```no-run
    /// let data = block_on(async {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     await!(storage.read(key))
    /// )};
    /// ```
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    pub async fn read(&self, key: impl Key) -> Result<Vec<u8>> {
        let inner = await!(self.inner.safe.lock());
        let active_blob_read_res = await!(inner
            .active_blob
            .as_ref()
            .ok_or(Error::ActiveBlobNotSet)?
            .read(key.as_ref().to_vec()));
        Ok(if let Ok(record) = active_blob_read_res {
            record
        } else {
            let stream: FuturesUnordered<_> = inner
                .blobs
                .iter()
                .map(|blob| blob.read(key.as_ref().to_vec()))
                .collect();
            let mut task = stream.skip_while(|res| future::ready(res.is_err()));
            await!(task.next())
                .ok_or(Error::RecordNotFound)?
                .map_err(Error::BlobError)?
        }
        .get_data())
    }

    /// # Description
    /// Stop work dir observer thread
    pub fn close(&self) -> Result<()> {
        self.inner.need_exit.store(false, Ordering::Relaxed);
        // @TODO implement
        Ok(())
    }

    /// # Description
    /// Blobs count contains closed blobs and one active, if is some.
    /// # Examples
    /// ```no-run
    /// # use pearl::Builder;
    /// let mut storage = Builder::new().work_dir("/tmp/pearl/").build::<f64>();
    /// storage.init();
    /// assert_eq!(storage.blobs_count(), 1);
    /// ```
    pub fn blobs_count(&self) -> usize {
        self.inner.next_blob_id.load(Ordering::Relaxed)
    }

    async fn prepare_work_dir(&mut self) -> Result<()> {
        let path = Path::new(
            self.inner
                .config
                .work_dir
                .as_ref()
                .ok_or(Error::Uninitialized)?,
        );
        if path.exists() {
            debug!("work dir exists: {}", path.display());
        } else {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path).map_err(Error::IO)?;
        }

        let lock_file_path = path.join(LOCK_FILE);
        // @TODO check if dir is locked
        // if lock_file_path.exists() {
        //     return Err(Error::WorkDirInUse);
        // }

        debug!("try to open lock file: {}", lock_file_path.display());
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lock_file_path)
            .map_err(Error::IO)?;
        await!(self.inner.safe.lock()).lock_file = Some(lock_file);
        Ok(())
    }

    async fn init_new(&mut self) -> Result<()> {
        let mut safe_locked = await!(self.inner.safe.lock());
        let next = self.inner.next_blob_name()?;
        safe_locked.active_blob =
            Some(Blob::open_new(next).map_err(Error::InitActiveBlob)?.boxed());
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
        let dir_content = files.iter().map(DirEntry::path);
        let dir_files = dir_content.filter(|path| path.is_file());
        let mut blob_files = dir_files.filter_map(|path| {
            if path.extension()?.to_str()? == BLOB_FILE_EXTENSION {
                Some(path)
            } else {
                None
            }
        });
        let mut temp_blobs = blob_files.try_fold(
            Vec::new(),
            |mut temp_blobs, path| -> Result<Vec<Blob<SimpleIndex>>> {
                let blob = Blob::from_file(path)?;
                blob.check_data_consistency()?;
                temp_blobs.push(blob);
                Ok(temp_blobs)
            },
        )?;
        temp_blobs.sort_by_key(Blob::id);
        let active_blob = temp_blobs.pop().ok_or(Error::Uninitialized)?.boxed();
        let mut safe_locked = await!(self.inner.safe.lock());
        safe_locked.active_blob = Some(active_blob);
        safe_locked.blobs = temp_blobs;
        self.inner.next_blob_id.store(
            safe_locked.max_id().map(|i| i + 1).unwrap_or(0),
            Ordering::Relaxed,
        );
        Ok(())
    }
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        self.twins_count.fetch_add(1, Ordering::Relaxed);
        Self {
            config: self.config.clone(),
            safe: self.safe.clone(),
            next_blob_id: self.next_blob_id.clone(),
            need_exit: self.need_exit.clone(),
            twins_count: self.twins_count.clone(),
        }
    }
}

impl Inner {
    fn new(config: Config) -> Self {
        Self {
            config,
            safe: Arc::new(Mutex::new(Safe::new())),
            next_blob_id: Arc::new(AtomicUsize::new(0)),
            need_exit: Arc::new(AtomicBool::new(false)),
            twins_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn next_blob_name(&self) -> Result<blob::FileName> {
        let next_id = self.next_blob_id.fetch_add(1, Ordering::Relaxed);
        let prefix = self
            .config
            .blob_file_name_prefix
            .as_ref()
            .ok_or(Error::Uninitialized)?
            .to_owned();
        let dir = self
            .config
            .work_dir
            .as_ref()
            .ok_or(Error::Uninitialized)?
            .to_owned();
        Ok(blob::FileName::new(
            prefix,
            next_id,
            BLOB_FILE_EXTENSION.to_owned(),
            dir,
        ))
    }
}

impl Safe {
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

struct Observer<S>
where
    S: SpawnExt + Send + 'static,
{
    spawner: S,
    inner: Inner,
    next_update: Instant,
    update_interval: Duration,
}

impl<S> Observer<S>
where
    S: SpawnExt + Send + 'static + Unpin + Sync,
{
    fn new(update_interval: Duration, inner: Inner, next_update: Instant, spawner: S) -> Self {
        Self {
            update_interval,
            inner,
            next_update,
            spawner,
        }
    }
    fn run(mut self) {
        while let Some(f) = block_on(self.next()) {
            if let Err(e) = self.spawner.spawn(f.map(|r| {
                r.expect("active blob update future paniced");
            })) {
                error!("{:?}", e);
                break;
            }
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let now = Instant::now();
        if self.next_update < now {
            trace!("Observer update");
            self.as_mut().next_update = now + self.update_interval;
            let inner_cloned = self.inner.clone();
            let res = async {
                if let Some(inner) = await!(active_blob_check(inner_cloned))? {
                    await!(update_active_blob(inner))?;
                }
                Ok(())
            };
            let res = Box::new(res);
            let obj = FutureObj::new(res);
            Poll::Ready(Some(obj))
        } else if self.inner.need_exit.load(Ordering::Relaxed) {
            cx.waker().wake_by_ref();
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
    Uninitialized,
    IO(io::Error),
    RecordNotFound,
    ObserverSpawnFailed(task::SpawnError),
    WorkDirInUse,
    KeySizeMismatch(usize),
}

impl From<blob::Error> for Error {
    fn from(blob_error: blob::Error) -> Self {
        Error::BlobError(blob_error)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    // pub key_size: Option<u16>,
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

async fn launch_observer<S>(spawner: S, inner: Inner)
where
    S: SpawnExt + Send + 'static + Unpin + Sync,
{
    let observer = Observer::new(
        Duration::from_millis(inner.config.update_interval_ms),
        inner,
        Instant::now(),
        spawner,
    );
    thread::spawn(move || observer.run());
}

async fn active_blob_check(inner: Inner) -> Result<Option<Inner>> {
    let (active_size, active_count) = {
        let safe_locked = await!(inner.safe.lock());
        let active_blob = safe_locked
            .active_blob
            .as_ref()
            .ok_or(Error::ActiveBlobNotSet)?;
        (active_blob.file_size()?, active_blob.records_count() as u64)
    };
    let config_max_size = inner.config.max_blob_size.ok_or(Error::Uninitialized)?;
    let config_max_count = inner.config.max_data_in_blob.ok_or(Error::Uninitialized)?;
    if active_size > config_max_size || active_count >= config_max_count {
        Ok(Some(inner))
    } else {
        Ok(None)
    }
}

async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    let new_active = Blob::open_new(next_name)?.boxed();

    let mut safe_locked = await!(inner.safe.lock());
    let mut old_active = safe_locked
        .active_blob
        .replace(new_active)
        .ok_or(Error::ActiveBlobNotSet)?;
    old_active.flush()?;
    safe_locked.blobs.push(*old_active);
    Ok(())
}

/// Trait `Key`
pub trait Key: AsRef<[u8]> {
    /// Key must have fixed length
    const LEN: u16;

    /// Convert `Self` into `Vec<u8>`
    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}
