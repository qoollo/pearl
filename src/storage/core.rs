use crate::error::ValidationErrorKind;

use super::prelude::*;
use futures::stream::FuturesOrdered;
use tokio::fs::{create_dir, create_dir_all};

const BLOB_FILE_EXTENSION: &str = "blob";

/// A main storage struct.
///
/// This type is clonable, cloning it will only create a new reference,
/// not a new storage.
/// Storage has a type kindeter K.
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
#[derive(Debug, Clone)]
pub struct Storage<K>
where
    for<'a> K: Key<'a>,
{
    pub(crate) inner: Inner<K>,
    observer: Observer<K>,
}

#[derive(Debug, Clone)]
pub(crate) struct Inner<K>
where
    for<'a> K: Key<'a>,
{
    pub(crate) config: Config,
    pub(crate) safe: Arc<RwLock<Safe<K>>>,
    next_blob_id: Arc<AtomicUsize>,
    pub(crate) ioring: Option<Rio>,
}

#[derive(Debug)]
pub(crate) struct Safe<K>
where
    for<'a> K: Key<'a>,
{
    pub(crate) active_blob: Option<Box<Blob<K>>>,
    pub(crate) blobs: Arc<RwLock<HierarchicalFilters<K, Bloom, Blob<K>>>>,
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

impl<K> Storage<K>
where
    for<'a> K: Key<'a> + 'static,
{
    pub(crate) fn new(config: Config, ioring: Option<Rio>) -> Self {
        let dump_sem = config.dump_sem();
        let inner = Inner::new(config, ioring);
        let observer = Observer::new(inner.clone(), dump_sem);
        Self { inner, observer }
    }

    /// [`init()`] used to prepare all environment to further work.
    ///
    /// Storage works in directory provided to builder. If directory don't exist,
    /// storage creates it, otherwise tries to init existing storage.
    /// # Errors
    /// Returns error in case of failures with IO operations or
    /// if some of the required kinds are missed.
    ///
    /// [`init()`]: struct.Storage.html#method.init
    pub async fn init(&mut self) -> Result<()> {
        self.init_ext(true).await
    }

    /// [`init_lazy()`] used to prepare all environment to further work, but unlike `init`
    /// doesn't set active blob, which means that first write may take time..
    ///
    /// Storage works in directory provided to builder. If directory don't exist,
    /// storage creates it, otherwise tries to init existing storage.
    /// # Errors
    /// Returns error in case of failures with IO operations or
    /// if some of the required params are missed.
    ///
    /// [`init_lazy()`]: struct.Storage.html#method.init
    pub async fn init_lazy(&mut self) -> Result<()> {
        self.init_ext(false).await
    }

    async fn init_ext(&mut self, with_active: bool) -> Result<()> {
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
    /// asyncronously, use [`create_active_blob_in_background()`]
    /// Returns true if new blob was created else false
    /// # Errors
    /// Fails if it's not possible to create new blob
    /// [`create_active_blob_in_background()`]: struct.Storage.html#method.create_active_blob_async
    pub async fn try_create_active_blob(&self) -> Result<()> {
        self.inner.create_active_blob().await
    }

    /// Creates active blob
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`try_create_active_blob()`]
    /// [`try_create_active_blob()`]: struct.Storage.html#method.try_create_active_blob
    pub async fn create_active_blob_in_background(&self) {
        self.observer.create_active_blob().await
    }

    /// Dumps active blob
    /// NOTICE! This function works in current thread, so it may take time. To perform this
    /// asyncronously, use [`close_active_blob_in_background()`]
    /// Returns true if blob was really dumped else false
    /// # Errors
    /// Fails if there are some errors during dump
    /// [`close_active_blob_in_background()`]: struct.Storage.html#method.create_active_blob_async
    pub async fn try_close_active_blob(&self) -> Result<()> {
        let result = self.inner.close_active_blob().await;
        self.observer.try_dump_old_blob_indexes().await;
        result
    }

    /// Dumps active blob
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`try_close_active_blob()`]
    pub async fn close_active_blob_in_background(&self) {
        self.observer.close_active_blob().await;
        self.observer.try_dump_old_blob_indexes().await
    }

    /// Sets last blob from closed blobs as active if there is no active blobs
    /// NOTICE! This function works in current thread, so it may take time. To perform this
    /// asyncronously, use [`restore_active_blob_in_background()`]
    /// Returns true if last blob was set as active as false
    /// # Errors
    /// Fails if active blob is set or there is no closed blobs
    /// [`restore_active_blob_in_background()`]: struct.Storage.html#method.restore_active_blob_async
    pub async fn try_restore_active_blob(&self) -> Result<()> {
        self.inner.restore_active_blob().await
    }

    /// Sets last blob from closed blobs as active if there is no active blobs
    /// NOTICE! This function returns immediately, so you can't check result of operation. If you
    /// want be sure about operation's result, use [`try_restore_active_blob()`]
    /// [`try_restore_active_blob()`]: struct.Storage.html#method.try_restore_active_blob
    pub async fn restore_active_blob_in_background(&self) {
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
        self.write_with_optional_meta(key, value, None).await
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
        self.write_with_optional_meta(key, value, Some(meta)).await
    }

    async fn write_with_optional_meta(
        &self,
        key: impl AsRef<K>,
        value: Vec<u8>,
        meta: Option<Meta>,
    ) -> Result<()> {
        let key = key.as_ref();
        debug!("storage write with {:?}, {}b, {:?}", key, value.len(), meta);
        // if active blob is set, this function will only check this fact and return false
        if self.try_create_active_blob().await.is_ok() {
            info!("Active blob was set during write operation");
        }
        if !self.inner.config.allow_duplicates() && self.contains_with(key, meta.as_ref()).await? {
            warn!(
                "record with key {:?} and meta {:?} exists",
                key.as_ref(),
                meta
            );
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
        let key = key.as_ref();
        debug!("storage read {:?}", key);
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
        let key = key.as_ref();
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
            .iter_possible_childs(key)
            .map(|b| b.1.data.read_all_entries(key))
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
        if let Some(ablob) = safe.active_blob.as_ref() {
            match ablob.read_any(key, meta, true).await {
                Ok(data) => {
                    debug!("storage read with optional meta active blob returned data");
                    return Ok(data);
                }
                Err(e) => debug!("read with optional meta active blob returned: {:#?}", e),
            }
        }
        Self::get_any_data(&safe, key, meta).await
    }

    async fn get_data_last(safe: &Safe<K>, key: &K, meta: Option<&Meta>) -> Result<Vec<u8>> {
        let blobs = safe.blobs.read().await;
        let possible_blobs = blobs
            .iter_possible_childs_rev(key)
            .map(|(id, blob)| async move {
                if matches!(blob.data.check_filters(key).await, Ok(true)) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<FuturesOrdered<_>>()
            .filter_map(|x| x)
            .collect::<Vec<_>>()
            .await;
        debug!(
            "len of possible blobs: {} (start len: {})",
            possible_blobs.len(),
            blobs.len()
        );
        let stream: FuturesOrdered<_> = possible_blobs
            .into_iter()
            .filter_map(|id| blobs.get_child(id))
            .map(|blob| blob.data.read_any(key, meta, false))
            .collect();
        debug!("read with optional meta {} closed blobs", stream.len());
        let mut task = stream.skip_while(Result::is_err);
        task.next().await.ok_or_else(Error::not_found)?
    }

    #[allow(dead_code)]
    async fn get_data_any(safe: &Safe<K>, key: &K, meta: Option<&Meta>) -> Result<Vec<u8>> {
        let blobs = safe.blobs.read().await;
        let stream: FuturesUnordered<_> = blobs
            .iter_possible_childs_rev(key)
            .map(|blob| blob.1.data.read_any(key, meta, true))
            .collect();
        debug!("read with optional meta {} closed blobs", stream.len());
        let mut task = stream.skip_while(Result::is_err);
        task.next()
            .await
            .ok_or_else(Error::not_found)?
            .with_context(|| "no results in closed blobs")
    }

    async fn get_any_data(safe: &Safe<K>, key: &K, meta: Option<&Meta>) -> Result<Vec<u8>> {
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
        *safe.blobs.write().await =
            HierarchicalFilters::from_vec(self.inner.config.bloom_filter_group_size(), 1, blobs)
                .await;
        self.inner
            .next_blob_id
            .store(safe.max_id().await.map_or(0, |i| i + 1), ORD);
        Ok(())
    }

    async fn pop_active(blobs: &mut Vec<Blob<K>>, config: &Config) -> Result<Box<Blob<K>>> {
        let mut active_blob = blobs
            .pop()
            .ok_or_else(|| {
                let wd = config.work_dir();
                error!("No blobs in {:?} to create an active one", wd);
                Error::from(ErrorKind::Uninitialized)
            })?
            .boxed();
        active_blob.load_index().await?;
        Ok(active_blob)
    }

    async fn read_blobs(
        files: &[DirEntry],
        ioring: Option<Rio>,
        disk_access_sem: Arc<Semaphore>,
        config: &Config,
    ) -> Result<Vec<Blob<K>>> {
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
                Blob::from_file(file.clone(), ioring.clone(), config.index())
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
                    let msg = format!("Failed to read existing blob: {}", file.display());
                    if config.ignore_corrupted() {
                        error!("{}, cause: {:#}", msg, e);
                    } else if Self::should_save_corrupted_blob(&e) {
                        error!(
                            "save corrupted blob '{}' to directory '{}'",
                            file.display(),
                            config.corrupted_dir_name()
                        );
                        Self::save_corrupted_blob(&file, config.corrupted_dir_name())
                            .await
                            .with_context(|| {
                                anyhow!(format!("failed to save corrupted blob {:?}", file))
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
        debug!("decide wether to save corrupted blobs: {:#}", error);
        if let Some(error) = error.downcast_ref::<Error>() {
            return match error.kind() {
                ErrorKind::Bincode(_) => true,
                ErrorKind::Validation { kind, cause: _ } => {
                    !matches!(kind, ValidationErrorKind::BlobVersion)
                }
                _ => false,
            };
        }
        false
    }

    async fn save_corrupted_blob(path: &Path, corrupted_dir_name: &str) -> Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("[{}] blob path don't have parent directory", path.display()))?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow!("[{}] blob path don't have file name", path.display()))?
            .to_os_string();
        let corrupted_dir_path = parent.join(corrupted_dir_name);
        let corrupted_path = corrupted_dir_path.join(file_name);
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
        tokio::fs::rename(&path, &corrupted_path)
            .await
            .with_context(|| {
                anyhow!(format!(
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
            tokio::fs::remove_file(&index_path)
                .await
                .with_context(|| anyhow!(format!("failed to remove file {:?}", index_path)))?;
        }
        Ok(())
    }

    /// `contains` is used to check whether a key is in storage.
    /// Slower than `check_bloom`, because doesn't prevent disk IO operations.
    /// `contains` returns either "definitely in storage" or "definitely not".
    /// # Errors
    /// Fails because of any IO errors
    pub async fn contains(&self, key: impl AsRef<K>) -> Result<bool> {
        self.contains_with(key.as_ref(), None).await
    }

    async fn contains_with(&self, key: &K, meta: Option<&Meta>) -> Result<bool> {
        let inner = self.inner.safe.read().await;
        if let Some(active_blob) = &inner.active_blob {
            if active_blob.contains(key, meta).await? {
                return Ok(true);
            }
        }
        let blobs = inner.blobs.read().await;
        for blob in blobs.iter_possible_childs(key) {
            if blob.1.data.contains(key, meta).await? {
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
        trace!("[{:?}] check in blobs bloom filter", key);
        let inner = self.inner.safe.read().await;
        let in_active = if let Some(active_blob) = inner.active_blob.as_ref() {
            active_blob
                .check_filters(key)
                .await
                .map_err(|e| {
                    error!(
                        "Failed to check filter for active blob for key {:?}: {}",
                        key, e
                    );
                    e
                })
                .unwrap_or(false)
        } else {
            false
        };
        if in_active {
            return Some(true);
        }

        let blobs = inner.blobs.read().await;
        let (offloaded, in_memory): (Vec<&Blob<K>>, Vec<&Blob<K>>) =
            blobs.iter().partition(|blob| blob.is_filter_offloaded());

        let in_closed = in_memory
            .iter()
            .any(|blob| blob.check_filters_in_memory(key));
        if in_closed {
            return Some(true);
        }

        let in_closed_offloaded = offloaded
            .iter()
            .map(|blob| blob.check_filters(key))
            .collect::<FuturesUnordered<_>>()
            .any(|value| value.unwrap_or(false))
            .await;
        Some(in_closed_offloaded)
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
    /// This function was used previously instead of [`close_active_blob_in_background()`]
    /// Creates new active blob.
    /// # Errors
    /// Fails because of any IO errors.
    /// Or if there are some problems with syncronization.
    /// [`close_active_blob_in_background()`]: struct.Storage.html#method.close_active_blob_async
    pub async fn force_update_active_blob(&self, predicate: ActiveBlobPred) {
        self.observer.force_update_active_blob(predicate).await;
        self.observer.try_dump_old_blob_indexes().await
    }

    fn launch_observer(&mut self) {
        self.observer.run();
    }
}

impl<K> Inner<K>
where
    for<'a> K: Key<'a> + 'static,
{
    fn new(config: Config, ioring: Option<Rio>) -> Self {
        Self {
            safe: Arc::new(RwLock::new(Safe::new(config.bloom_filter_group_size()))),
            config,
            next_blob_id: Arc::new(AtomicUsize::new(0)),
            ioring,
        }
    }

    pub(crate) async fn restore_active_blob(&self) -> Result<()> {
        if self.has_active_blob().await {
            return Err(Error::active_blob_already_exists().into());
        }
        let mut safe = self.safe.write().await;
        if let None = safe.active_blob {
            let blob_opt = safe.blobs.write().await.pop().map(|b| b.boxed());
            if let Some(blob) = blob_opt {
                safe.active_blob = Some(blob);
                Ok(())
            } else {
                Err(Error::uninitialized().into())
            }
        } else {
            Err(Error::active_blob_not_set().into())
        }
    }

    pub(crate) async fn create_active_blob(&self) -> Result<()> {
        if self.has_active_blob().await {
            return Err(Error::active_blob_already_exists().into());
        }
        let mut safe = self.safe.write().await;
        if let None = safe.active_blob {
            let next = self.next_blob_name()?;
            let config = self.config.index();
            let blob = Blob::open_new(next, self.ioring.clone(), config)
                .await?
                .boxed();
            safe.active_blob = Some(blob);
            Ok(())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn close_active_blob(&self) -> Result<()> {
        if !self.has_active_blob().await {
            return Err(Error::active_blob_doesnt_exist().into());
        }
        let mut safe = self.safe.write().await;
        if safe.active_blob.is_none() {
            Err(Error::active_blob_doesnt_exist().into())
        } else {
            // always true
            if let Some(ablob) = safe.active_blob.take() {
                ablob.fsyncdata().await?;
                safe.blobs.write().await.push(*ablob).await;
            }
            Ok(())
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

    pub(crate) async fn try_dump_old_blob_indexes(&self, sem: Arc<Semaphore>) {
        self.safe.write().await.try_dump_old_blob_indexes(sem).await;
    }
}

impl<K> Safe<K>
where
    for<'a> K: Key<'a> + 'static,
{
    fn new(group_size: usize) -> Self {
        Self {
            active_blob: None,
            blobs: Arc::new(RwLock::new(HierarchicalFilters::new(group_size, 1))),
        }
    }

    async fn max_id(&self) -> Option<usize> {
        let active_blob_id = self.active_blob.as_ref().map(|blob| blob.id());
        let blobs_max_id = self.blobs.read().await.last().map(|x| x.id());
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

    pub(crate) async fn replace_active_blob(&mut self, blob: Box<Blob<K>>) -> Result<()> {
        let old_active = self.active_blob.replace(blob);
        if let Some(blob) = old_active {
            self.blobs.write().await.push(*blob).await;
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
pub trait Key<'a>:
    AsRef<[u8]> + From<Vec<u8>> + Debug + Clone + Send + Sync + Ord + Default
{
    /// Key must have fixed length
    const LEN: u16;

    /// Reference type for zero-copy key creation
    type Ref: RefKey<'a>;

    /// Convert `Self` into `Vec<u8>`
    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    /// Convert `Self` to `Self::Ref`
    fn as_ref_key(&'a self) -> Self::Ref {
        Self::Ref::from(self.as_ref())
    }
}

/// Trait for reference key type
pub trait RefKey<'a>: Ord + From<&'a [u8]> {}
#[async_trait::async_trait]
impl<K> BloomProvider<K> for Storage<K>
where
    for<'a> K: Key<'a> + 'static,
{
    type Filter = <Blob<K> as BloomProvider<K>>::Filter;
    async fn check_filter(&self, item: &K) -> FilterResult {
        let inner = self.inner.safe.read().await;
        let active = inner
            .active_blob
            .as_ref()
            .map(|b| b.check_filter_fast(item))
            .unwrap_or_default();
        let ret = inner.blobs.read().await.check_filter(item).await;
        ret + active
    }

    fn check_filter_fast(&self, _item: &K) -> FilterResult {
        FilterResult::NeedAdditionalCheck
    }

    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        let inner = self.inner.safe.read().await;
        let ret = inner
            .blobs
            .write()
            .await
            .offload_buffer(needed_memory, level)
            .await;
        ret
    }

    async fn get_filter(&self) -> Option<Self::Filter> {
        let inner = self.inner.safe.read().await;
        let mut ret = inner.blobs.read().await.get_filter_fast().cloned();
        if let Some(filter) = &mut ret {
            if let Some(active_filter) = inner
                .active_blob
                .as_ref()
                .map(|b| b.get_filter_fast())
                .flatten()
            {
                if !filter.checked_add_assign(active_filter) {
                    return None;
                }
            }
        }
        ret
    }

    fn get_filter_fast(&self) -> Option<&Self::Filter> {
        None
    }

    async fn filter_memory_allocated(&self) -> usize {
        let safe = self.inner.safe.read().await;
        let active = safe
            .active_blob
            .as_ref()
            .map_or(0, |blob| blob.filter_memory_allocated());

        let closed = safe.blobs.read().await.filter_memory_allocated().await;
        active + closed
    }
}
