use super::prelude::*;

const BLOB_FILE_EXTENSION: &str = "blob";
const LOCK_FILE: &str = "pearl.lock";

const O_EXCL: i32 = 128;

/// A main storage struct.
///
/// This type is clonable, cloning it will only create a new reference,
/// not a new storage.
/// Storage has a type parameter K.
/// To perform read/write operations K must implement [`Key`] trait.
///
/// # Examples
///
/// ```no-run
/// use pearl::{Storage, Builder, Key};
///
/// #[tokio::main]
/// async fn main() {
///     let mut storage: Storage<String> = Builder::new()
///         .work_dir("/tmp/pearl/")
///         .max_blob_size(1_000_000)
///         .max_data_in_blob(1_000_000_000)
///         .blob_file_name_prefix("pearl-test")
///         .build()
///         .unwrap();
///     storage.init().await.unwrap();
/// }
/// ```
///
/// [`Key`]: trait.Key.html
#[derive(Debug)]
pub struct Storage<K> {
    pub(crate) inner: Inner,
    marker: PhantomData<K>,
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) config: Config,
    pub(crate) safe: Arc<Mutex<Safe>>,
    next_blob_id: Arc<AtomicUsize>,
    pub(crate) need_exit: Arc<AtomicBool>,
    twins_count: Arc<AtomicUsize>,
    pub(crate) ioring: Rio,
}

#[derive(Debug)]
pub(crate) struct Safe {
    pub(crate) active_blob: Option<Box<Blob>>,
    pub(crate) blobs: Vec<Blob>,
    lock_file: Option<StdFile>,
}

impl<K> Drop for Storage<K> {
    fn drop(&mut self) {
        let twins = self.inner.twins_count.fetch_sub(1, ORD);
        // 1 is because twin#0 - in observer thread, twin#1 - self
        if twins <= 1 {
            trace!("stop observer thread");
            self.inner.need_exit.store(false, ORD);
        }
    }
}

impl<K> Clone for Storage<K> {
    #[must_use]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

async fn work_dir_content(wd: &Path) -> Result<Option<Vec<DirEntry>>> {
    let files = read_dir(wd).await?;
    let files = files.filter_map(IOResult::ok);
    let files: Vec<_> = files.collect().await;
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
    pub(crate) fn new(config: Config, ioring: Rio) -> Self {
        Self {
            inner: Inner::new(config, ioring),
            marker: PhantomData,
        }
    }

    /// [`init()`] used to prepare all environment to further work.
    ///
    /// Storage works in directory provided to builder. If directory don't exist,
    /// storage creates it, otherwise tries to init existing storage.
    /// # Errors
    /// Returns error in case of failures with IO operations or
    /// if some of the required params are missed.
    ///
    /// [`init()`]: struct.Storage.html#method.init
    pub async fn init(&mut self) -> Result<()> {
        // @TODO implement work dir validation
        self.prepare_work_dir().await?;
        let cont_res = work_dir_content(
            self.inner
                .config
                .work_dir()
                .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?,
        )
        .await;
        trace!("work dir content loaded");
        if let Some(files) = cont_res? {
            trace!("storage init from existing files");
            self.init_from_existing(files)
                .await
                .context("failed to init from existing blobs")?
        } else {
            self.init_new().await?
        };
        trace!("new storage initialized");
        launch_observer(self.inner.clone());
        trace!("observer started");
        Ok(())
    }

    /// Writes `data` to active blob asyncronously. If active blob reaches it limit, creates new
    /// and closes old.
    /// # Examples
    /// ```no-run
    /// async fn write_data() {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     let data = b"async written to blob".to_vec();
    ///     storage.write(key, data).await
    /// }
    /// ```
    /// # Errors
    /// Fails with the same errors as [`write_with`]
    ///
    /// [`write_with`]: Storage::write_with
    pub async fn write(&self, key: impl Key, value: Vec<u8>) -> Result<()> {
        self.write_with(key, value, Meta::new()).await
    }

    /// Similar to [`write`] but with metadata
    /// # Examples
    /// ```no-run
    /// async fn write_data() {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     let data = b"async written to blob".to_vec();
    ///     let meta = Meta::new();
    ///     meta.insert("version".to_string(), b"1.0".to_vec());
    ///     storage.write_with(&key, data, meta).await
    /// }
    /// ```
    /// # Errors
    /// Fails if duplicates are not allowed and record already exists.
    pub async fn write_with(&self, key: impl Key, value: Vec<u8>, meta: Meta) -> Result<()> {
        if !self.inner.config.allow_duplicates() {
            let existing_metas = self
                .get_all_existing_metas(&key)
                .await
                .with_context(|| "storage write with get all existing metas failed")?;
            if existing_metas.contains(&meta) {
                warn!("record with key {:?} and meta {:?} exists", key, meta);
                return Ok(());
            }
        }
        let record = Record::create(&key, value, meta)
            .with_context(|| "storage write with record creation failed")?;
        let mut safe = self.inner.safe.lock().await;
        let blob = safe
            .active_blob
            .as_mut()
            .ok_or_else(Error::active_blob_not_set)?;
        blob.write(record).await
    }

    async fn get_all_existing_metas(&self, key: impl Key) -> Result<Vec<Meta>> {
        let mut safe = self.inner.safe.lock().await;
        let active_blob = safe
            .active_blob
            .as_mut()
            .ok_or_else(Error::active_blob_not_set)?;
        let mut metas = Vec::new();
        if let Some(meta) = active_blob
            .get_all_metas(key.as_ref())
            .await
            .with_context(|| "storage get all existing metas from active blob failed")?
        {
            metas.extend(meta);
        }
        for blob in &safe.blobs {
            if let Some(meta) = blob.get_all_metas(key.as_ref()).await.with_context(|| {
                format!(
                    "storage get all existing metas from blob failed: {}",
                    blob.name()
                )
            })? {
                metas.extend(meta);
            }
        }
        Ok(metas)
    }

    /// Reads the first found data matching given key.
    /// # Examples
    /// ```no-run
    /// async fn read_data() {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     let data = storage.read(key).await;
    /// }
    /// ```
    /// # Errors
    /// Same as [`read_with`]
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    /// [`read_with`]: Storage::read_with
    #[inline]
    pub async fn read(&self, key: impl Key) -> Result<Vec<u8>> {
        self.read_with_optional_meta(key, None).await
    }
    /// Reads data matching given key and metadata
    /// # Examples
    /// ```no-run
    /// async fn read_data() {
    ///     let key = 42u64.to_be_bytes().to_vec();
    ///     let meta = Meta::new();
    ///     meta.insert("version".to_string(), b"1.0".to_vec());
    ///     let data = storage.read(&key, &meta).await;
    /// }
    /// ```
    /// # Errors
    /// Return error if record is not found.
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    #[inline]
    pub async fn read_with(&self, key: impl Key, meta: &Meta) -> Result<Vec<u8>> {
        self.read_with_optional_meta(key, Some(meta)).await
    }

    /// Returns stream producing entries with matching key
    pub fn read_all<'a>(&'a self, key: &'a impl Key) -> ReadAll<'a, K> {
        ReadAll::new(self, key.as_ref())
    }

    async fn read_with_optional_meta(&self, key: impl Key, meta: Option<&Meta>) -> Result<Vec<u8>> {
        debug!("storage read with optional meta");
        let inner = self.inner.safe.lock().await;
        let key = key.as_ref();
        let active_blob_read_res = inner
            .active_blob
            .as_ref()
            .ok_or_else(|| Error::active_blob_not_set())?
            .read_any(key, meta)
            .await;
        debug!("storage read with optional meta from active blob finished");
        let record = match active_blob_read_res {
            Ok(record) => {
                debug!("storage read with optional meta active blob returned record");
                record
            }
            Err(e) => {
                debug!(
                    "storage read with optional meta active blob returned: {:#?}",
                    e
                );
                let stream: FuturesUnordered<_> = inner
                    .blobs
                    .iter()
                    .map(|blob| blob.read_any(key, meta))
                    .collect();
                let mut task = stream.skip_while(Result::is_err);
                task.next().await.ok_or_else(|| Error::not_found())??
            }
        };
        Ok(record)
    }

    /// Stop blob updater and release lock file
    /// # Errors
    /// Fails because of any IO errors
    pub async fn close(&self) -> Result<()> {
        let mut safe = self.inner.safe.lock().await;
        let active_blob = safe.active_blob.take();
        if let Some(mut blob) = active_blob {
            blob.dump()
                .await
                .with_context(|| format!("blob {} dump failed", blob.name()))?;
            debug!("active blob dumped");
        }
        self.inner.need_exit.store(false, ORD);
        safe.lock_file = None;
        if let Some(work_dir) = self.inner.config.work_dir() {
            std::fs::remove_file(work_dir.join(LOCK_FILE))?;
        }
        info!("active blob dumped, lock released");
        Ok(())
    }

    /// `blob_count` returns number of closed blobs plus one active, if there is some.
    /// # Examples
    /// ```no-run
    /// use pearl::Builder;
    ///
    /// let mut storage = Builder::new().work_dir("/tmp/pearl/").build::<f64>();
    /// storage.init().await;
    /// assert_eq!(storage.blobs_count(), 1);
    /// ```
    #[must_use]
    pub fn blobs_count(&self) -> usize {
        self.inner.next_blob_id.load(ORD)
    }

    async fn prepare_work_dir(&mut self) -> Result<()> {
        let work_dir = self.inner.config.work_dir().ok_or_else(|| {
            error!("Work dir is not set");
            Error::unitialized()
        })?;
        let path = Path::new(work_dir);
        if path.exists() {
            debug!("work dir exists: {}", path.display());
        } else {
            debug!("creating work dir recursively: {}", path.display());
            std::fs::create_dir_all(path)?;
        }
        self.try_lock_dir(path).await
    }

    async fn try_lock_dir<'a>(&'a self, path: &'a Path) -> Result<()> {
        let lock_file_path = path.join(LOCK_FILE);
        debug!("try to open lock file: {}", lock_file_path.display());
        let lock_file = StdOpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(O_EXCL)
            .open(&lock_file_path)
            .map_err(|e| {
                error!("working directory is locked: {:?}", lock_file_path);
                error!("check if any other bob instances are running");
                error!("or delete .lock file and try again");
                e
            })?;
        debug!("{} not locked", path.display());
        self.inner.safe.lock().await.lock_file = Some(lock_file);
        Ok(())
    }

    async fn init_new(&mut self) -> Result<()> {
        let safe_locked = self.inner.safe.lock();
        let next = self.inner.next_blob_name()?;
        let config = self.filter_config();
        let mut safe = safe_locked.await;
        let blob = Blob::open_new(next, self.inner.ioring.clone(), config)
            .await?
            .boxed();
        safe.active_blob = Some(blob);
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
        trace!("init from existing: {:#?}", files);
        let mut blobs = Self::read_blobs(&files, self.inner.ioring.clone(), self.filter_config())
            .await
            .context("failed to read blobs")?;

        debug!("{} blobs successfully created", blobs.len());
        blobs.sort_by_key(Blob::id);
        let mut active_blob = blobs
            .pop()
            .ok_or_else(|| {
                let wd = self.inner.config.work_dir();
                error!("There are some blob files in the work dir: {:?}", wd);
                error!("Creating blobs from all these files failed");
                Error::from(ErrorKind::Uninitialized)
            })?
            .boxed();
        let mut safe_locked = self.inner.safe.lock().await;
        active_blob.load_index().await?;
        for blob in &mut blobs {
            debug!("dump all blobs except active blob");
            blob.dump().await?;
        }
        safe_locked.active_blob = Some(active_blob);
        safe_locked.blobs = blobs;
        self.inner
            .next_blob_id
            .store(safe_locked.max_id().map_or(0, |i| i + 1), ORD);
        Ok(())
    }

    async fn read_blobs(
        files: &[DirEntry],
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> Result<Vec<Blob>> {
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
        let futures: FuturesUnordered<_> = blob_files
            .map(|file| Blob::from_file(file, ioring.clone(), filter_config.clone()))
            .collect();
        debug!("async init blobs from file");
        futures
            .try_collect()
            .await
            .context("failed to read existing blobs")
    }

    /// `contains` is used to check whether a key is in storage.
    /// Slower than `check_bloom`, because doesn't prevent disk IO operations.
    /// `contains` returns either "definitely in storage" or "definitely not".
    /// # Errors
    /// Fails because of any IO errors
    pub async fn contains(&self, key: impl Key) -> Result<bool> {
        let key = key.as_ref();
        let inner = self.inner.safe.lock().await;
        let in_active = if let Some(active_blob) = &inner.active_blob {
            active_blob.contains(key, None).await?
        } else {
            false
        };
        if !in_active {
            for blob in &inner.blobs {
                if blob.contains(key, None).await? {
                    return Ok(true);
                }
            }
        }
        Ok(in_active)
    }

    /// `check_bloom` is used to check whether a key is in storage.
    /// If bloom filter opt out, returns `None`.
    /// Uses bloom filter under the hood, so false positive results are possible,
    /// but false negatives are not.
    /// In other words, `check_bloom` returns either "possibly in storage" or "definitely not".
    pub async fn check_bloom(&self, key: impl Key) -> Option<bool> {
        trace!("[{:?}] check in blobs bloom filter", &key.to_vec());
        let inner = self.inner.safe.lock().await;
        let in_active = inner
            .active_blob
            .as_ref()
            .map_or(Some(false), |active_blob| {
                active_blob.check_bloom(key.as_ref())
            })?;
        let in_closed = inner
            .blobs
            .iter()
            .any(|blob| blob.check_bloom(key.as_ref()) == Some(true));
        Some(in_active || in_closed)
    }

    /// Total records count in storage.
    pub async fn records_count(&self) -> usize {
        self.inner.records_count().await
    }

    /// Records count per blob. Format: (`blob_id`, count). Last value is from active blob.
    pub async fn records_count_detailed(&self) -> Vec<(usize, usize)> {
        self.inner.records_count_detailed().await
    }

    /// Records count in active blob. Returns None if active blob not set or any IO error occured.
    pub async fn records_count_in_active_blob(&self) -> Option<usize> {
        self.inner.records_count_in_active_blob().await
    }

    /// Syncronizes data and metadata of the active blob with the filesystem.
    /// Like `tokio::std::fs::File::sync_data`, this function will attempt to ensure that in-core data reaches the filesystem before returning.
    /// May not syncronize file metadata to the file system.
    /// # Errors
    /// Fails because of any IO errors
    pub async fn fsyncdata(&self) -> IOResult<()> {
        self.inner.fsyncdata().await
    }

    fn filter_config(&self) -> Option<BloomConfig> {
        self.inner.config.filter()
    }
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        self.twins_count.fetch_add(1, ORD);
        Self {
            config: self.config.clone(),
            safe: self.safe.clone(),
            next_blob_id: self.next_blob_id.clone(),
            need_exit: self.need_exit.clone(),
            twins_count: self.twins_count.clone(),
            ioring: self.ioring.clone(),
        }
    }
}

impl Inner {
    fn new(config: Config, ioring: Rio) -> Self {
        Self {
            config,
            safe: Arc::new(Mutex::new(Safe::new())),
            next_blob_id: Arc::new(AtomicUsize::new(0)),
            need_exit: Arc::new(AtomicBool::new(false)),
            twins_count: Arc::new(AtomicUsize::new(0)),
            ioring,
        }
    }

    pub(crate) fn next_blob_name(&self) -> Result<blob::FileName> {
        let next_id = self.next_blob_id.fetch_add(1, ORD);
        let prefix = self
            .config
            .blob_file_name_prefix()
            .ok_or_else(|| {
                error!("Blob file name prefix is not set");
                Error::unitialized()
            })?
            .to_owned();
        let dir = self
            .config
            .work_dir()
            .ok_or_else(|| {
                error!("Work dir is not set");
                Error::unitialized()
            })?
            .to_owned();
        Ok(blob::FileName::new(
            prefix,
            next_id,
            BLOB_FILE_EXTENSION.to_owned(),
            dir,
        ))
    }

    async fn records_count(&self) -> usize {
        self.safe.lock().await.records_count().await
    }

    async fn records_count_detailed(&self) -> Vec<(usize, usize)> {
        self.safe.lock().await.records_count_detailed().await
    }

    async fn records_count_in_active_blob(&self) -> Option<usize> {
        self.safe.lock().await.records_count_in_active_blob().await
    }

    async fn fsyncdata(&self) -> IOResult<()> {
        self.safe.lock().await.fsyncdata().await
    }
}

impl Safe {
    const fn new() -> Self {
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

    async fn records_count(&self) -> usize {
        let details = self.records_count_detailed().await;
        details.iter().fold(0, |acc, (_, count)| acc + count)
    }

    async fn records_count_detailed(&self) -> Vec<(usize, usize)> {
        let mut results = Vec::new();
        for blob in &self.blobs {
            let count = blob.records_count().await;
            if let Ok(c) = count {
                let value = (blob.id(), c);
                debug!("push: {:?}", value);
                results.push(value);
            }
        }
        if let Some(count) = self.records_count_in_active_blob().await {
            let value = (self.blobs.len(), count);
            debug!("push: {:?}", value);
            results.push(value);
        }
        results
    }

    async fn records_count_in_active_blob(&self) -> Option<usize> {
        if let Some(ref blob) = self.active_blob {
            blob.records_count().await.ok()
        } else {
            None
        }
    }

    async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref blob) = self.active_blob {
            blob.fsyncdata().await?;
        }
        Ok(())
    }
}

fn launch_observer(inner: Inner) {
    let observer = Observer::new(
        Duration::from_millis(inner.config.update_interval_ms()),
        inner,
    );
    tokio::spawn(observer.run());
}

/// Trait `Key`
pub trait Key: AsRef<[u8]> + Debug {
    /// Key must have fixed length
    const LEN: u16;

    /// Convert `Self` into `Vec<u8>`
    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl<T> Key for &T
where
    T: Key,
{
    const LEN: u16 = T::LEN;
}
