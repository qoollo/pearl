use futures::{
    executor::block_on,
    future::{self, FutureExt, FutureObj},
    lock::Mutex,
    stream::{futures_unordered::FuturesUnordered, Stream, StreamExt},
    task::{Context, Poll, Spawn, SpawnExt},
};
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{self, DirEntry, File, OpenOptions},
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

use super::error::{Error, ErrorKind};
use crate::{
    blob::{self, Blob},
    record::Record,
};

const BLOB_FILE_EXTENSION: &str = "blob";
const LOCK_FILE: &str = "pearl.lock";
// For now it is only constant from libc,
const O_EXCL: i32 = 128;

/// # Description
/// A specialized storage result type
pub type Result<T> = std::result::Result<T, Error>;

/// A main storage struct.
/// # Description
/// This type is clonable, cloning it will only create a new reference,
/// not a new storage.
/// Storage has a type parameter K.
/// To perform read/write operations K must implement [`Key`] trait.
///
/// # Examples
/// ```no-run
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
    active_blob: Option<Box<Blob>>,
    blobs: Vec<Blob>,
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
        .map_err(Error::new)?
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
        self.prepare_work_dir().await?;

        let cont_res = work_dir_content(
            self.inner
                .config
                .work_dir
                .as_ref()
                .ok_or(ErrorKind::Uninitialized)?,
        );
        if let Some(files) = cont_res? {
            self.init_from_existing(files).await?
        } else {
            self.init_new().await?
        };
        let observer_fut_obj: FutureObj<_> =
            Box::new(launch_observer(spawner.clone(), self.inner.clone())).into();
        spawner
            .spawn(observer_fut_obj)
            .map_err(|e| Error::raw(format!("{:?}", e)))?;
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
    ///     storage.write(key, data).await
    /// )};
    /// ```
    pub async fn write(self, key: impl Key, value: Vec<u8>) -> Result<()> {
        let record = Record::new(key, value);
        trace!("await for inner lock");
        let mut safe = self.inner.safe.lock().await;
        trace!("return write future");
        let blob = safe
            .active_blob
            .as_mut()
            .ok_or(ErrorKind::ActiveBlobNotSet)?;
        blob.write(record).await.map_err(Error::new)
    }

    /// # Description
    /// Reads data with given key, if error ocured or there are no records with matching
    /// key, returns [`Error::RecordNotFound`]
    /// # Examples
    /// ```no-run
    /// let data = block_on(async {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     storage.read(key).await
    /// )};
    /// ```
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    pub async fn read(&self, key: impl Key) -> Result<Vec<u8>> {
        let inner = self.inner.safe.lock().await;
        let active_blob_read_res = inner
            .active_blob
            .as_ref()
            .ok_or(ErrorKind::ActiveBlobNotSet)?
            .read(key.as_ref().to_vec())
            .await;
        Ok(if let Ok(record) = active_blob_read_res {
            record
        } else {
            let stream: FuturesUnordered<_> = inner
                .blobs
                .iter()
                .map(|blob| blob.read(key.as_ref().to_vec()))
                .collect();
            debug!("await for stream of read futures: {}", stream.len());
            let mut task = stream.skip_while(|res| future::ready(res.is_err()));
            task.next()
                .await
                .ok_or(ErrorKind::RecordNotFound)?
                .map_err(Error::new)?
        }
        .get_data())
    }

    /// # Description
    /// Stop work dir observer thread
    pub async fn close(&self) -> Result<()> {
        self.inner.need_exit.store(false, Ordering::Relaxed);
        self.inner.safe.lock().await.lock_file = None;
        if let Some(ref work_dir) = self.inner.config.work_dir {
            fs::remove_file(work_dir.join(LOCK_FILE)).map_err(Error::new)?;
        };
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
                .ok_or(ErrorKind::Uninitialized)?,
        );
        if path.exists() {
            debug!("work dir exists: {}", path.display());
        } else {
            debug!("creating work dir recursively: {}", path.display());
            fs::create_dir_all(path).map_err(Error::new)?;
        }
        self.try_lock_dir(path).await
    }

    async fn try_lock_dir<'a>(&'a self, path: &'a Path) -> Result<()> {
        let lock_file_path = path.join(LOCK_FILE);
        debug!("try to open lock file: {}", lock_file_path.display());
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(O_EXCL)
            .open(&lock_file_path)
            .map_err(Error::new)?;
        debug!("{} not locked", path.display());
        self.inner.safe.lock().await.lock_file = Some(lock_file);
        Ok(())
    }

    async fn init_new(&mut self) -> Result<()> {
        let safe_locked = self.inner.safe.lock();
        let next = self.inner.next_blob_name()?;
        safe_locked.await.active_blob =
            Some(Blob::open_new(next).await.map_err(Error::new)?.boxed());
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
        debug!("read working directory content");
        let dir_content = files.iter().map(DirEntry::path);
        debug!("read {} entities", dir_content.len());
        let dir_files = dir_content.filter(|path| path.is_file());
        debug!("filter potential blob files");
        let blob_files = dir_files.filter_map(|path| {
            if path.extension()?.to_str()? == BLOB_FILE_EXTENSION {
                Some(path)
            } else {
                None
            }
        });
        debug!("init blobs from found files");
        let futures: FuturesUnordered<_> = blob_files.map(Blob::from_file).collect();
        debug!("async init blobs from file");
        let blob_res: Vec<_> = futures.collect().await;
        debug!("all {} futures finished", blob_res.len());
        let mut blobs: Vec<_> = blob_res
            .into_iter()
            .filter_map(|res| {
                if let Err(ref e) = res {
                    error!("{:?}", e);
                }
                res.ok()
            })
            .collect();
        debug!("{} blobs successfully created", blobs.len());
        blobs.sort_by_key(Blob::id);
        let active_blob = blobs
            .pop()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?
            .boxed();
        let mut safe_locked = self.inner.safe.lock().await;
        safe_locked.active_blob = Some(active_blob);
        safe_locked.blobs = blobs;
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
            .ok_or(ErrorKind::Uninitialized)?
            .to_owned();
        let dir = self
            .config
            .work_dir
            .as_ref()
            .ok_or(ErrorKind::Uninitialized)?
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
                if let Some(inner) = active_blob_check(inner_cloned).await? {
                    update_active_blob(inner).await?;
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
        let safe_locked = inner.safe.lock().await;
        let active_blob = safe_locked
            .active_blob
            .as_ref()
            .ok_or(ErrorKind::ActiveBlobNotSet)?;
        (
            active_blob.file_size().map_err(Error::new)?,
            active_blob.records_count().await.map_err(Error::new)? as u64,
        )
    };
    let config_max_size = inner
        .config
        .max_blob_size
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    let config_max_count = inner
        .config
        .max_data_in_blob
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    if active_size > config_max_size || active_count >= config_max_count {
        Ok(Some(inner))
    } else {
        Ok(None)
    }
}

async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    let new_active = Blob::open_new(next_name).await.map_err(Error::new)?.boxed();

    let mut safe_locked = inner.safe.lock().await;
    let mut old_active = safe_locked
        .active_blob
        .replace(new_active)
        .ok_or(ErrorKind::ActiveBlobNotSet)?;
    old_active.dump().await.map_err(Error::new)?;
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
