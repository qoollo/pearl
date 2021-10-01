use super::prelude::*;
use futures::stream::FuturesOrdered;
use tokio::fs::{create_dir, create_dir_all};

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
        // @TODO implement work dir validation
        self.prepare_work_dir()
            .await
            .context("failed to prepare work dir")?;
        let wd = self
            .inner
            .config
            .work_dir()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
        let cont_res = work_dir_content(wd)
            .await
            .with_context(|| format!("failed to read work dir content: {}", wd.display()));
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
        self.launch_observer();
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
        let active_blob_read_res = safe
            .active_blob
            .as_ref()
            .ok_or_else(Error::active_blob_not_set)?
            .read_any(key, meta, true)
            .await;
        debug!("storage read with optional meta from active blob finished");
        match active_blob_read_res {
            Ok(data) => {
                debug!("storage read with optional meta active blob returned data");
                Ok(data)
            }
            Err(e) => {
                debug!("read with optional meta active blob returned: {:#?}", e);
                Self::get_any_data(&safe, key, meta).await
            }
        }
    }

    async fn get_data_last(safe: &Safe, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        let blobs = safe.blobs.read().await;
        let blobs_stream: FuturesOrdered<_> = blobs
            .iter()
            .enumerate()
            .map(|(i, blob)| async move {
                if blob.check_filters_non_blocking(key).await {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        let possible_blobs: Vec<_> = blobs_stream.filter_map(|e| e).collect().await;
        debug!(
            "len of possible blobs: {} (start len: {})",
            possible_blobs.len(),
            blobs.len()
        );
        let stream: FuturesOrdered<_> = possible_blobs
            .iter()
            .rev()
            .map(|i| blobs[*i].read_any(key, meta, false))
            .collect();
        debug!("read with optional meta {} closed blobs", stream.len());
        let mut task = stream.skip_while(Result::is_err);
        task.next().await.ok_or_else(Error::not_found)?
    }

    #[allow(dead_code)]
    async fn get_data_any(safe: &Safe, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        let blobs = safe.blobs.read().await;
        let stream: FuturesUnordered<_> = blobs
            .iter()
            .map(|blob| blob.read_any(key, meta, true))
            .collect();
        debug!("read with optional meta {} closed blobs", stream.len());
        let mut task = stream.skip_while(Result::is_err);
        task.next()
            .await
            .ok_or_else(Error::not_found)?
            .with_context(|| "no results in closed blobs")
    }

    async fn get_any_data(safe: &Safe, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        Self::get_data_last(safe, key, meta).await
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
            create_dir_all(path).await?;
        } else {
            error!("work dir path not found: {}", path.display());
            return Err(Error::work_dir_unavailable(
                path,
                "work dir path not found".to_owned(),
                IOErrorKind::NotFound,
            ))
            .with_context(|| "failed to prepare work dir");
        }
        let corrupted_dir_path = path.join(self.inner.config.corrupted_dir_name());
        if corrupted_dir_path.exists() {
            debug!("{} dir exists", path.display());
        } else {
            debug!("creating dir for corrupted files: {}", path.display());
            create_dir(corrupted_dir_path).await.with_context(|| {
                format!(
                    "failed to create dir for corrupted files: {}",
                    path.display()
                )
            })?;
        }
        self.try_lock_dir(path)
            .await
            .with_context(|| format!("failed to lock work dir: {}", path.display()))
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
        let mut safe = self.inner.safe.write().await;
        let blob = Blob::open_new(next, self.inner.ioring.clone(), self.inner.config.index())
            .await?
            .boxed();
        safe.active_blob = Some(blob);
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>) -> Result<()> {
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
        let mut active_blob = blobs
            .pop()
            .ok_or_else(|| {
                let wd = self.inner.config.work_dir();
                error!("There are some blob files in the work dir: {:?}", wd);
                error!("Creating blobs from all these files failed");
                Error::from(ErrorKind::Uninitialized)
            })?
            .boxed();
        let mut safe = self.inner.safe.write().await;
        active_blob.load_index(K::LEN).await?;
        for blob in &mut blobs {
            debug!("dump all blobs except active blob");
            blob.dump().await?;
        }
        safe.active_blob = Some(active_blob);
        *safe.blobs.write().await = blobs;
        self.inner
            .next_blob_id
            .store(safe.max_id().await.map_or(0, |i| i + 1), ORD);
        Ok(())
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
                Blob::from_file(file.clone(), ioring.clone(), config.index(), K::LEN)
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
                    } else if Self::should_save_corrupted_blob(&e) {
                        log::error!(
                            "save corrupted blob '{}' to directory '{}'",
                            file.display(),
                            config.corrupted_dir_name()
                        );
                        Self::save_corrupted_blob(&file, config.corrupted_dir_name())
                            .await
                            .with_context(|| {
                                anyhow::anyhow!(format!("failed to save corrupted blob {:?}", file))
                            })?;
                    } else {
                        return Err(e.context(msg));
                    }
                }
            }
        }
        Ok(blobs)
    }

    fn should_save_corrupted_blob(error: &anyhow::Error) -> bool {
        if let Some(error) = error.downcast_ref::<Error>() {
            return match error.kind() {
                ErrorKind::Bincode(_)
                | ErrorKind::BlobValidation(_)
                | ErrorKind::RecordValidation(_)
                | ErrorKind::IndexValidation(_) => true,
                _ => false,
            };
        }
        false
    }

    async fn save_corrupted_blob(path: &Path, corrupted_dir_name: &str) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            anyhow::anyhow!("[{}] blob path don't have parent directory", path.display())
        })?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("[{}] blob path don't have file name", path.display()))?
            .to_os_string();
        let corrupted_path = parent.join(corrupted_dir_name).join(file_name);
        tokio::fs::rename(&path, &corrupted_path)
            .await
            .with_context(|| {
                anyhow::anyhow!(format!(
                    "failed to move file {:?} to {:?}",
                    path, corrupted_path
                ))
            })?;
        Self::remove_index_by_blob_path(path).await?;
        Ok(())
    }

    async fn remove_index_by_blob_path(path: &Path) -> Result<()> {
        let index_path = path.with_extension(blob::BLOB_INDEX_FILE_EXTENSION);
        if index_path.exists() {
            tokio::fs::remove_file(&index_path).await.with_context(|| {
                anyhow::anyhow!(format!("failed to remove file {:?}", index_path))
            })?;
        }
        Ok(())
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

    /// `check_filters` is used to check whether a key is in storage.
    /// Range (min-max test) and bloom filters are used.
    /// If bloom filter opt out and range filter passes, returns `None`.
    /// False positive results are possible, but false negatives are not.
    /// In other words, `check_filters` returns either "possibly in storage" or "definitely not".
    pub async fn check_filters(&self, key: impl AsRef<K>) -> Option<bool> {
        let key = key.as_ref();
        trace!("[{:?}] check in blobs bloom filter", &key.to_vec());
        let inner = self.inner.safe.read().await;
        let in_active = inner
            .active_blob
            .as_ref()
            .map_or(false, |active_blob| active_blob.check_filters(key.as_ref()));
        let in_closed = inner
            .blobs
            .read()
            .await
            .iter()
            .any(|blob| blob.check_filters(key.as_ref()) == true);
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

    /// Force closes active blob to dump index on disk and free RAM.
    /// Creates new active blob.
    /// # Errors
    /// Fails because of any IO errors.
    /// Or if there are any problems with syncronization.
    pub async fn close_active_blob(&self) {
        self.observer.close_active_blob().await
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
        let old_active = self
            .active_blob
            .replace(blob)
            .ok_or_else(Error::active_blob_not_set)?;
        self.blobs.write().await.push(*old_active);
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
