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
pub struct Storage<K: Key> {
    pub(crate) inner: Inner,
    observer: Observer,
    marker: PhantomData<K>,
}

#[derive(Debug, Clone)]
pub(crate) struct Inner {
    pub(crate) config: Config,
    pub(crate) safe: Arc<RwLock<Safe>>,
    next_blob_id: Arc<AtomicUsize>,
    pub(crate) ioring: Option<Rio>,
}

#[derive(Debug)]
pub(crate) struct Safe {
    pub(crate) active_blob: Option<Box<Blob>>,
    pub(crate) blobs: Arc<RwLock<Vec<Blob>>>,
    lock_file: Option<StdFile>,
}

impl<K: Key> Clone for Storage<K> {
    #[must_use]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            observer: self.observer.clone(),
            marker: PhantomData,
        }
    }
}

async fn work_dir_content(wd: &Path) -> Result<Option<Vec<DirEntry>>> {
    let mut files = Vec::new();
    let mut dir = read_dir(wd).await?;
    while let Some(file) = dir.next_entry().await.transpose() {
        if let Ok(file) = file {
            files.push(file);
        }
    }

    let content = if files
        .iter()
        .filter_map(|file| Some(file.file_name().as_os_str().to_str()?.to_owned()))
        .any(|name| name.ends_with(BLOB_FILE_EXTENSION))
    {
        debug!("working dir contains files, try init existing");
        Some(files)
    } else {
        debug!("working dir is uninitialized, starting empty storage");
        None
    };
    Ok(content)
}

impl<K: Key> Storage<K> {
    pub(crate) fn new(config: Config, ioring: Option<Rio>) -> Self {
        let dump_sem = config.dump_sem();
        let inner = Inner::new(config, ioring);
        let observer = Observer::new(inner.clone(), dump_sem);
        Self {
            inner,
            observer,
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
        self.init_ext(true).await
    }

    /// [`init_noactive()`] used to prepare all environment to further work, but unlike `init`
    /// doesn't set active blob, which means that first write may take time..
    ///
    /// Storage works in directory provided to builder. If directory don't exist,
    /// storage creates it, otherwise tries to init existing storage.
    /// # Errors
    /// Returns error in case of failures with IO operations or
    /// if some of the required params are missed.
    ///
    /// [`init_noactive()`]: struct.Storage.html#method.init
    pub async fn init_noactive(&mut self) -> Result<()> {
        self.init_ext(false).await
    }

    async fn init_ext(&mut self, with_active: bool) -> Result<()> {
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
            self.init_from_existing(files, with_active)
                .await
                .context("failed to init from existing blobs")?
        } else {
            self.init_new().await?
        };
        trace!("new storage initialized");
        self.launch_observer();
        trace!("observer started");
        Ok(())
    }

    /// Checks if there is a pending async operation
    /// Returns boolean value (true - if there is, false otherwise)
    /// Never falls
    pub fn is_pending(&self) -> bool {
        self.observer.is_pending()
    }

    /// FIXME: maybe it would be better to add check of `is_pending` state of observer for all
    /// sync operations and return result in more appropriate way for that case (change Result<bool>
    /// on Result<OpRes>, where OpRes = Pending|Done|NotDone for example)

    /// Checks if active blob is set
    /// Returns boolean value
    /// Never falls
    pub async fn has_active_blob(&self) -> bool {
        self.inner.has_active_blob().await
    }

    /// Creates active blob
    /// NOTICE! This function works in current thread, so it may take time. To perform this
    /// asyncronously, use [`create_active_blob_async()`]
    /// Returns true if new blob was created else false
    /// # Errors
    /// Fails if it's not possible to create new blob
    /// [`create_active_blob_async()`]: struct.Storage.html#method.create_active_blob_async
    pub async fn create_active_blob(&self) -> Result<bool> {
        self.inner.create_active_blob().await
    }

    /// Creates active blob
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`create_active_blob()`]
    /// [`create_active_blob()`]: struct.Storage.html#method.create_active_blob
    pub async fn create_active_blob_async(&self) {
        self.observer.create_active_blob().await
    }

    /// Dumps active blob
    /// NOTICE! This function works in current thread, so it may take time. To perform this
    /// asyncronously, use [`close_active_blob_async()`]
    /// Returns true if blob was really dumped else false
    /// # Errors
    /// Fails if there are some errors during dump
    /// [`close_active_blob_async()`]: struct.Storage.html#method.create_active_blob_async
    pub async fn close_active_blob(&self) -> Result<bool> {
        self.inner.close_active_blob().await
    }

    /// Dumps active blob
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`close_active_blob()`]
    pub async fn close_active_blob_async(&self) {
        self.observer.close_active_blob().await
    }

    /// Sets last blob from closed blobs as active if there is no active blobs
    /// NOTICE! This function works in current thread, so it may take time. To perform this
    /// asyncronously, use [`restore_active_blob_async()`]
    /// Returns true if last blob was set as active as false
    /// # Errors
    /// Fails if active blob is set or there is no closed blobs
    /// [`restore_active_blob_async()`]: struct.Storage.html#method.restore_active_blob_async
    pub async fn restore_active_blob(&self) -> Result<bool> {
        self.inner.restore_active_blob().await
    }

    /// Sets last blob from closed blobs as active if there is no active blobs
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`restore_active_blob()`]
    /// [`restore_active_blob()`]: struct.Storage.html#method.restore_active_blob
    pub async fn restore_active_blob_async(&self) {
        self.observer.restore_active_blob().await
    }

    /// Writes `data` to active blob asyncronously. If active blob reaches it limit, creates new
    /// and closes old.
    /// NOTICE! First write into storage without active blob may take more time due to active blob
    /// creation
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
    pub async fn write(&self, key: impl AsRef<K>, value: Vec<u8>) -> Result<()> {
        self.write_with_optional_meta(key.as_ref(), value, None)
            .await
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
    pub async fn write_with(&self, key: impl AsRef<K>, value: Vec<u8>, meta: Meta) -> Result<()> {
        self.write_with_optional_meta(key.as_ref(), value, Some(meta))
            .await
    }

    async fn write_with_optional_meta(
        &self,
        key: &K,
        value: Vec<u8>,
        meta: Option<Meta>,
    ) -> Result<()> {
        debug!("storage write with {:?}, {}b, {:?}", key, value.len(), meta);
        // if active blob is set, this function will only check this fact and return false
        if self.create_active_blob().await? {
            info!("Active blob was set during write operation");
        }
        if !self.inner.config.allow_duplicates()
            && self.contains_with(key.as_ref(), meta.as_ref()).await?
        {
            warn!("record with key {:?} and meta {:?} exists", key, meta);
            return Ok(());
        }
        let record = Record::create(key, value, meta.unwrap_or_default())
            .with_context(|| "storage write with record creation failed")?;
        let mut safe = self.inner.safe.write().await;
        let blob = safe
            .active_blob
            .as_mut()
            .ok_or_else(Error::active_blob_not_set)?;
        blob.write(record).await.or_else(|err| {
            let e = err.downcast::<Error>()?;
            if let ErrorKind::FileUnavailable(kind) = e.kind() {
                let work_dir = self
                    .inner
                    .config
                    .work_dir()
                    .ok_or_else(Error::uninitialized)?;
                Err(Error::work_dir_unavailable(work_dir, e.to_string(), kind.to_owned()).into())
            } else {
                Err(e.into())
            }
        })
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
    pub async fn read(&self, key: impl AsRef<K>) -> Result<Vec<u8>> {
        debug!("storage read {:?}", key.as_ref());
        self.read_with_optional_meta(key.as_ref(), None).await
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
    pub async fn read_with(&self, key: impl AsRef<K>, meta: &Meta) -> Result<Vec<u8>> {
        let key = key.as_ref();
        debug!("storage read with {:?}", key);
        self.read_with_optional_meta(key, Some(meta))
            .await
            .with_context(|| "read with optional meta failed")
    }

    /// Returns entries with matching key
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn read_all(&self, key: impl AsRef<K>) -> Result<Vec<Entry>> {
        let key = key.as_ref().as_ref();
        let mut all_entries = Vec::new();
        let safe = self.inner.safe.read().await;
        let active_blob = safe
            .active_blob
            .as_ref()
            .ok_or_else(Error::active_blob_not_set)?;
        if let Some(entries) = active_blob.read_all_entries(key).await? {
            debug!(
                "storage core read all active blob entries {}",
                entries.len()
            );
            all_entries.extend(entries);
        }
        let blobs = safe.blobs.read().await;
        let entries_closed_blobs = blobs
            .iter()
            .map(|b| b.read_all_entries(key))
            .collect::<FuturesUnordered<_>>();
        entries_closed_blobs
            .try_filter_map(future::ok)
            .try_for_each(|v| {
                debug!("storage core read all closed blob {} entries", v.len());
                all_entries.extend(v);
                future::ok(())
            })
            .await?;
        debug!("storage core read all total {} entries", all_entries.len());
        Ok(all_entries)
    }

    async fn read_with_optional_meta(&self, key: &K, meta: Option<&Meta>) -> Result<Vec<u8>> {
        debug!("storage read with optional meta {:?}, {:?}", key, meta);
        let safe = self.inner.safe.read().await;
        let key = key.as_ref();
        if let Some(ablob) = safe.active_blob.as_ref() {
            match ablob.read_any(key, meta).await {
                Ok(data) => {
                    debug!("storage read with optional meta active blob returned data");
                    return Ok(data);
                }
                Err(e) => debug!("read with optional meta active blob returned: {:#?}", e),
            }
        }
        Self::get_any_data(&safe, key, meta).await
    }

    async fn get_any_data(safe: &Safe, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        let blobs = safe.blobs.read().await;
        let stream: FuturesUnordered<_> =
            blobs.iter().map(|blob| blob.read_any(key, meta)).collect();
        debug!("read with optional meta {} closed blobs", stream.len());
        let mut task = stream.skip_while(Result::is_err);
        task.next()
            .await
            .ok_or_else(Error::not_found)?
            .with_context(|| "no results in closed blobs")
    }

    /// Stop blob updater and release lock file
    /// # Errors
    /// Fails because of any IO errors
    pub async fn close(self) -> Result<()> {
        let mut safe = self.inner.safe.write().await;
        let active_blob = safe.active_blob.take();
        let mut res = Ok(());
        if let Some(mut blob) = active_blob {
            res = res.and(
                blob.dump()
                    .await
                    .map(|_| info!("active blob dumped"))
                    .with_context(|| format!("blob {} dump failed", blob.name())),
            )
        }
        safe.lock_file = None;
        if let Some(work_dir) = self.inner.config.work_dir() {
            res = res.and(
                std::fs::remove_file(work_dir.join(LOCK_FILE))
                    .map(|_| info!("lock released"))
                    .with_context(|| "failed to remove lockfile ({:?})"),
            );
        }
        res
    }

    /// `blob_count` returns exact number of closed blobs plus one active, if there is some.
    /// It locks on inner structure, so it much slower than `next_blob_id`.
    /// # Examples
    /// ```no-run
    /// use pearl::Builder;
    ///
    /// let mut storage = Builder::new().work_dir("/tmp/pearl/").build::<f64>();
    /// storage.init().await;
    /// assert_eq!(storage.blobs_count(), 1);
    /// ```
    pub async fn blobs_count(&self) -> usize {
        let safe = self.inner.safe.read().await;
        let count = safe.blobs.read().await.len();
        if safe.active_blob.is_some() {
            count + 1
        } else {
            count
        }
    }

    /// `index_memory` returns the amount of memory used by blob to store indices
    pub async fn index_memory(&self) -> usize {
        let safe = self.inner.safe.read().await;
        if let Some(ablob) = safe.active_blob.as_ref() {
            ablob.index_memory()
        } else {
            0
        }
    }

    /// Returns next blob ID. If pearl dir structure wasn't changed from the outside,
    /// returned number is equal to `blobs_count`. But this method doesn't require
    /// lock. So it is much faster than `blobs_count`.
    #[must_use]
    pub fn next_blob_id(&self) -> usize {
        self.inner.next_blob_id.load(ORD)
    }

    async fn prepare_work_dir(&mut self) -> Result<()> {
        let work_dir = self.inner.config.work_dir().ok_or_else(|| {
            error!("Work dir is not set");
            Error::uninitialized()
        })?;
        let path = Path::new(work_dir);
        if path.exists() {
            debug!("work dir exists: {}", path.display());
        } else if self.inner.config.create_work_dir() {
            debug!("creating work dir recursively: {}", path.display());
            std::fs::create_dir_all(path)?;
        } else {
            error!("work dir path not found: {}", path.display());
            return Err(Error::work_dir_unavailable(
                path,
                "work dir path not found".to_owned(),
                IOErrorKind::NotFound,
            ))
            .with_context(|| "failed to prepare work dir");
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
        self.inner.safe.write().await.lock_file = Some(lock_file);
        Ok(())
    }

    async fn init_new(&mut self) -> Result<()> {
        let next = self.inner.next_blob_name()?;
        let config = self.filter_config();
        let mut safe = self.inner.safe.write().await;
        let blob = Blob::open_new(next, self.inner.ioring.clone(), config)
            .await?
            .boxed();
        safe.active_blob = Some(blob);
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>, with_active: bool) -> Result<()> {
        trace!("init from existing: {:#?}", files);
        let disk_access_sem = self.observer.get_dump_sem();
        let mut blobs = Self::read_blobs(
            &files,
            self.inner.ioring.clone(),
            disk_access_sem,
            &self.inner.config,
        )
        .await
        .context("failed to read blobs")?;

        debug!("{} blobs successfully created", blobs.len());
        blobs.sort_by_key(Blob::id);

        let active_blob = if with_active {
            Some(Self::pop_active(&mut blobs, &self.inner.config).await?)
        } else {
            None
        };

        for blob in &mut blobs {
            debug!("dump all blobs except active blob");
            blob.dump().await?;
        }

        let mut safe = self.inner.safe.write().await;
        safe.active_blob = active_blob;
        *safe.blobs.write().await = blobs;
        self.inner
            .next_blob_id
            .store(safe.max_id().await.map_or(0, |i| i + 1), ORD);
        Ok(())
    }

    async fn pop_active(blobs: &mut Vec<Blob>, config: &Config) -> Result<Box<Blob>> {
        let mut active_blob = blobs
            .pop()
            .ok_or_else(|| {
                let wd = config.work_dir();
                error!("There are some blob files in the work dir: {:?}", wd);
                error!("Creating blobs from all these files failed");
                Error::from(ErrorKind::Uninitialized)
            })?
            .boxed();
        active_blob.load_index(K::LEN).await?;
        Ok(active_blob)
    }

    async fn read_blobs(
        files: &[DirEntry],
        ioring: Option<Rio>,
        disk_access_sem: Arc<Semaphore>,
        config: &Config,
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
        let mut futures: FuturesUnordered<_> = blob_files
            .map(|file| async {
                let sem = disk_access_sem.clone();
                let _sem = sem.acquire().await.expect("sem is closed");
                Blob::from_file(file.clone(), ioring.clone(), config.filter(), K::LEN)
                    .await
                    .map_err(|e| (e, file))
            })
            .collect();
        debug!("async init blobs from file");
        let mut blobs = Vec::new();
        while let Some(blob_res) = futures.next().await {
            match blob_res {
                Ok(blob) => blobs.push(blob),
                Err((e, file)) => {
                    let msg = format!(
                        "Failed to read existing blob!\nPath: {:?};\nReason: {:?}.",
                        file, e
                    );
                    if config.ignore_corrupted() {
                        error!("{}", msg);
                    } else {
                        return Err(e.context(msg));
                    }
                }
            }
        }
        Ok(blobs)
    }

    /// `contains` is used to check whether a key is in storage.
    /// Slower than `check_bloom`, because doesn't prevent disk IO operations.
    /// `contains` returns either "definitely in storage" or "definitely not".
    /// # Errors
    /// Fails because of any IO errors
    pub async fn contains(&self, key: K) -> Result<bool> {
        let key = key.as_ref();
        self.contains_with(key, None).await
    }

    async fn contains_with(&self, key: &[u8], meta: Option<&Meta>) -> Result<bool> {
        let inner = self.inner.safe.read().await;
        if let Some(active_blob) = &inner.active_blob {
            if active_blob.contains(key, meta).await? {
                return Ok(true);
            }
        }
        let blobs = inner.blobs.read().await;
        for blob in blobs.iter() {
            if blob.contains(key, meta).await? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// `check_bloom` is used to check whether a key is in storage.
    /// If bloom filter opt out, returns `None`.
    /// Uses bloom filter under the hood, so false positive results are possible,
    /// but false negatives are not.
    /// In other words, `check_bloom` returns either "possibly in storage" or "definitely not".
    pub async fn check_bloom(&self, key: impl AsRef<K>) -> Option<bool> {
        let key = key.as_ref();
        trace!("[{:?}] check in blobs bloom filter", &key.to_vec());
        let inner = self.inner.safe.read().await;
        let in_active = inner
            .active_blob
            .as_ref()
            .map_or(Some(false), |active_blob| {
                active_blob.check_bloom(key.as_ref())
            })?;
        let in_closed = inner
            .blobs
            .read()
            .await
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
    /// Fails because of any IO errors.
    pub async fn fsyncdata(&self) -> IOResult<()> {
        self.inner.fsyncdata().await
    }

    /// Force updates active blob on new one to dump index of old one on disk and free RAM.
    /// This function was used previously instead of [`close_active_blob_async()`]
    /// Creates new active blob.
    /// # Errors
    /// Fails because of any IO errors.
    /// Or if there are some problems with syncronization.
    /// [`close_active_blob_async()`]: struct.Storage.html#method.close_active_blob_async
    pub async fn force_update_active_blob(&self, predicate: ActiveBlobPred) {
        self.observer.force_update_active_blob(predicate).await
    }

    fn filter_config(&self) -> Option<BloomConfig> {
        self.inner.config.filter()
    }

    fn launch_observer(&mut self) {
        self.observer.run();
    }
}

impl Inner {
    fn new(config: Config, ioring: Option<Rio>) -> Self {
        Self {
            config,
            safe: Arc::new(RwLock::new(Safe::new())),
            next_blob_id: Arc::new(AtomicUsize::new(0)),
            ioring,
        }
    }

    pub(crate) async fn restore_active_blob(&self) -> Result<bool> {
        if self.has_active_blob().await {
            return Ok(false);
        }
        let mut safe = self.safe.write().await;
        if let None = safe.active_blob {
            let blob_opt = safe.blobs.write().await.pop().map(|b| b.boxed());
            if let Some(blob) = blob_opt {
                safe.active_blob = Some(blob);
                Ok(true)
            } else {
                // NOTE: maybe it would be better panic in case of this operation with empty blob list?
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn create_active_blob(&self) -> Result<bool> {
        if self.has_active_blob().await {
            return Ok(false);
        }
        let mut safe = self.safe.write().await;
        if let None = safe.active_blob {
            let next = self.next_blob_name()?;
            let config = self.config.filter();
            let blob = Blob::open_new(next, self.ioring.clone(), config)
                .await?
                .boxed();
            safe.active_blob = Some(blob);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn close_active_blob(&self) -> Result<bool> {
        if !self.has_active_blob().await {
            return Ok(false);
        }
        let mut safe = self.safe.write().await;
        if safe.active_blob.is_none() {
            Ok(false)
        } else {
            // FIXME: write lock is still held, so everyone will wait for dump, maybe it's better
            // to derive this operation to `try_dump_old_blob_indexes`
            safe.active_blob.as_mut().unwrap(/*None case checked*/).dump().await?;
            // always true
            if let Some(ablob) = safe.active_blob.take() {
                safe.blobs.write().await.push(*ablob);
            }
            Ok(true)
        }
    }

    pub(crate) async fn has_active_blob(&self) -> bool {
        if let None = self.safe.read().await.active_blob {
            false
        } else {
            true
        }
    }

    pub(crate) async fn active_blob_stat(&self) -> Option<ActiveBlobStat> {
        if let Some(ablob) = self.safe.read().await.active_blob.as_ref() {
            let records_count = ablob.records_count();
            let index_memory = ablob.index_memory();
            let file_size = ablob.file_size() as usize;
            Some(ActiveBlobStat::new(records_count, index_memory, file_size))
        } else {
            None
        }
    }

    // FIXME: Maybe we should revert counter if new blob creation failed?
    // It'll make code a bit more complicated, but blobs will sequentially grow for sure
    pub(crate) fn next_blob_name(&self) -> Result<blob::FileName> {
        let next_id = self.next_blob_id.fetch_add(1, ORD);
        let prefix = self
            .config
            .blob_file_name_prefix()
            .ok_or_else(|| {
                error!("Blob file name prefix is not set");
                Error::uninitialized()
            })?
            .to_owned();
        let dir = self
            .config
            .work_dir()
            .ok_or_else(|| {
                error!("Work dir is not set");
                Error::uninitialized()
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
        self.safe.read().await.records_count().await
    }

    async fn records_count_detailed(&self) -> Vec<(usize, usize)> {
        self.safe.read().await.records_count_detailed().await
    }

    async fn records_count_in_active_blob(&self) -> Option<usize> {
        let inner = self.safe.read().await;
        inner.active_blob.as_ref().map(|b| b.records_count())
    }

    async fn fsyncdata(&self) -> IOResult<()> {
        self.safe.read().await.fsyncdata().await
    }

    pub(crate) async fn try_dump_old_blob_indexes(&mut self, sem: Arc<Semaphore>) {
        self.safe.write().await.try_dump_old_blob_indexes(sem).await;
    }
}

impl Safe {
    fn new() -> Self {
        Self {
            active_blob: None,
            blobs: Arc::new(RwLock::new(Vec::new())),
            lock_file: None,
        }
    }

    async fn max_id(&self) -> Option<usize> {
        let active_blob_id = self.active_blob.as_ref().map(|blob| blob.id());
        let blobs_max_id = self.blobs.read().await.last().map(Blob::id);
        active_blob_id.max(blobs_max_id)
    }

    async fn records_count(&self) -> usize {
        let details = self.records_count_detailed().await;
        details.iter().fold(0, |acc, (_, count)| acc + count)
    }

    async fn records_count_detailed(&self) -> Vec<(usize, usize)> {
        let mut results = Vec::new();
        let blobs = self.blobs.read().await;
        for blob in blobs.iter() {
            let count = blob.records_count();
            let value = (blob.id(), count);
            debug!("push: {:?}", value);
            results.push(value);
        }
        if let Some(blob) = self.active_blob.as_ref() {
            let value = (blobs.len(), blob.records_count());
            debug!("push: {:?}", value);
            results.push(value);
        }
        results
    }

    async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref blob) = self.active_blob {
            blob.fsyncdata().await?;
        }
        Ok(())
    }

    pub(crate) async fn replace_active_blob(&mut self, blob: Box<Blob>) -> Result<()> {
        let old_active = self.active_blob.replace(blob);
        if let Some(blob) = old_active {
            self.blobs.write().await.push(*blob);
        }
        Ok(())
    }

    pub(crate) async fn try_dump_old_blob_indexes(&mut self, sem: Arc<Semaphore>) {
        let blobs = self.blobs.clone();
        tokio::spawn(async move {
            trace!("acquire blobs write to dump old blobs");
            let mut write_blobs = blobs.write().await;
            trace!("dump old blobs");
            for blob in write_blobs.iter_mut() {
                trace!("dumping old blob");
                let _ = sem.acquire().await;
                trace!("acquired sem for dumping old blobs");
                if let Err(e) = blob.dump().await {
                    error!("Error dumping blob ({}): {}", blob.name(), e);
                }
                trace!("finished dumping old blob");
            }
        });
    }
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
