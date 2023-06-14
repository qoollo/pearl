use std::sync::atomic::AtomicBool;

use crate::{
    error::ValidationErrorKind,
    filter::{CombinedFilter, FilterTrait}, blob::DeleteResult,
};

use super::{prelude::*, read_result::BlobRecordTimestamp};
use bytes::Bytes;
use futures::stream::FuturesOrdered;
use tokio::fs::{create_dir, create_dir_all};

const BLOB_FILE_EXTENSION: &str = "blob";

/// A main storage struct.
///
/// Storage has a generic parameter K representing the type of `Key`.
/// To perform read/write operations K must implement [`Key`] trait.
///
/// # Examples
///
/// ```no_run
/// use pearl::{Storage, Builder, Key, ArrayKey};
///
/// #[tokio::main]
/// async fn main() {
///     let mut storage: Storage<ArrayKey<8>> = Builder::new()
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
pub struct Storage<K>
where
    for<'a> K: Key<'a>,
{
    inner: Arc<Inner<K>>,
    observer: Observer<K>
}

#[derive(Debug)]
pub(crate) struct Inner<K>
where
    for<'a> K: Key<'a>,
{
    config: Config,
    safe: RwLock<Safe<K>>,
    next_blob_id: AtomicUsize,
    iodriver: IoDriver,
    corrupted_blobs: AtomicUsize,
    fsync_in_progress: AtomicBool,
}

#[derive(Debug)]
pub(crate) struct Safe<K>
where
    for<'a> K: Key<'a>,
{
    active_blob: Option<Box<ASRwLock<Blob<K>>>>,
    blobs: Arc<RwLock<HierarchicalFilters<K, CombinedFilter<K>, Blob<K>>>>,
}

/// Helper struct to add names to result parameters
struct ReadBlobsResult<K> 
where
    for<'a> K: Key<'a>,
{
    blobs: Vec<Blob<K>>,
    new_corrupted_blob_count: usize,
    max_blob_id: Option<usize>
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
    pub(crate) fn new(config: Config, iodriver: IoDriver) -> Self {
        let inner = Arc::new(Inner::new(config, iodriver));
        let observer = Observer::new(inner.clone());
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
    /// ```no_run
    /// use pearl::{Builder, Storage, ArrayKey};
    ///
    /// async fn write_data(storage: Storage<ArrayKey<8>>) {
    ///     let key = ArrayKey::<8>::default();
    ///     let data = b"async written to blob".to_vec().into();
    ///     storage.write(key, data).await;
    /// }
    /// ```
    /// # Errors
    /// Fails with the same errors as [`write_with`]
    ///
    /// [`write_with`]: Storage::write_with
    pub async fn write(&self, key: impl AsRef<K>, value: Bytes) -> Result<()> {
        self.write_with_optional_meta(key, value, None).await
    }

    /// Similar to [`write`] but with metadata
    /// # Examples
    /// ```no_run
    /// use pearl::{Builder, Meta, Storage, ArrayKey};
    ///
    /// async fn write_data(storage: Storage<ArrayKey<8>>) {
    ///     let key = ArrayKey::<8>::default();
    ///     let data = b"async written to blob".to_vec().into();
    ///     let mut meta = Meta::new();
    ///     meta.insert("version".to_string(), b"1.0".to_vec());
    ///     storage.write_with(&key, data, meta).await;
    /// }
    /// ```
    /// # Errors
    /// Fails if duplicates are not allowed and record already exists.
    pub async fn write_with(&self, key: impl AsRef<K>, value: Bytes, meta: Meta) -> Result<()> {
        self.write_with_optional_meta(key, value, Some(meta)).await
    }

    /// Free all resources that may be freed without work interruption
    /// NOTICE! This function frees part of the resources in separate thread,
    /// so actual resources may be freed later
    pub async fn free_excess_resources(&self) -> usize {
        let memory = self.inactive_index_memory().await;
        self.observer.try_dump_old_blob_indexes().await;
        memory
    }

    /// Get size in bytes of inactive indexes
    pub async fn inactive_index_memory(&self) -> usize {
        let safe = self.inner.safe.read().await;
        let blobs = safe.blobs.read().await;
        blobs.iter().fold(0, |s, n| s + n.index_memory())
    }

    /// Get size in bytes of all freeable resources
    pub async fn index_memory(&self) -> usize {
        self.active_index_memory().await + self.inactive_index_memory().await
    }

    async fn write_with_optional_meta(
        &self,
        key: impl AsRef<K>,
        value: Bytes,
        meta: Option<Meta>,
    ) -> Result<()> {
        let key = key.as_ref();
        debug!("storage write with {:?}, {}b, {:?}", key, value.len(), meta);
        // if active blob is set, this function will only check this fact and return false
        if self.try_create_active_blob().await.is_ok() {
            info!("Active blob was set during write operation");
        }
        if !self.inner.config.allow_duplicates()
            && self.contains_with(key, meta.as_ref()).await?.is_found()
        {
            warn!(
                "record with key {:?} and meta {:?} exists",
                key.as_ref(),
                meta
            );
            return Ok(());
        }
        let record = Record::create(key, value, meta.unwrap_or_default())
            .with_context(|| "storage write with record creation failed")?;
        let safe = self.inner.safe.read().await;
        let blob = safe
            .active_blob
            .as_ref()
            .ok_or_else(Error::active_blob_not_set)?;
        let result = Blob::write(blob, key, record).await.or_else::<anyhow::Error, _>(|err| {
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
        })?;
        self.try_update_active_blob(blob).await?;
        if self.inner.should_try_fsync(result.dirty_bytes) {
            self.observer.try_fsync_data().await;
        }
        Ok(())
    }

    async fn try_update_active_blob(&self, active_blob: &Box<ASRwLock<Blob<K>>>) -> Result<()> {
        let config_max_size = self
            .inner
            .config
            .max_blob_size()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
        let config_max_count = self
            .inner
            .config
            .max_data_in_blob()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
        let active_blob = active_blob.read().await;
        if active_blob.file_size() >= config_max_size
            || active_blob.records_count() as u64 >= config_max_count
        {
            // In case of current time being earlier than active blob's creation, error will contain the difference
            let dur = active_blob.created_at().elapsed().map_err(|e| e.duration());
            let dur = match dur {
                Ok(d) => d,
                Err(d) => d,
            };
            if dur.as_millis() > self.inner.config.debounce_interval_ms() as u128 {
                self.observer.try_update_active_blob().await;
            }
        }
        Ok(())
    }

    /// Reads the first found data matching given key.
    /// # Examples
    /// ```no_run
    /// use pearl::{Builder, Meta, Storage, ArrayKey};
    ///
    /// async fn read_data(storage: Storage<ArrayKey<8>>) {
    ///     let key = ArrayKey::<8>::default();
    ///     let data = storage.read(key).await;
    /// }
    /// ```
    /// # Errors
    /// Same as [`read_with`]
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    /// [`read_with`]: Storage::read_with
    #[inline]
    pub async fn read(&self, key: impl AsRef<K>) -> Result<ReadResult<Bytes>> {
        let key = key.as_ref();
        debug!("storage read {:?}", key);
        self.read_with_optional_meta(key, None).await
    }
    /// Reads data matching given key and metadata
    /// # Examples
    /// ```no_run
    /// use pearl::{Builder, Meta, Storage, ArrayKey};
    ///
    /// async fn read_data(storage: Storage<ArrayKey<8>>) {
    ///     let key = ArrayKey::<8>::default();
    ///     let mut meta = Meta::new();
    ///     meta.insert("version".to_string(), b"1.0".to_vec());
    ///     let data = storage.read_with(&key, &meta).await;
    /// }
    /// ```
    /// # Errors
    /// Return error if record is not found.
    ///
    /// [`Error::RecordNotFound`]: enum.Error.html#RecordNotFound
    #[inline]
    pub async fn read_with(&self, key: impl AsRef<K>, meta: &Meta) -> Result<ReadResult<Bytes>> {
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
        let mut entries = self.read_all_with_deletion_marker(key).await?;
        if let Some(e) = entries.last() {
            if e.is_deleted() {
                entries.truncate(entries.len() - 1);
            }
        }
        Ok(entries)
    }

    /// Returns entries with matching key and deletion marker, if any
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn read_all_with_deletion_marker(&self, key: impl AsRef<K>) -> Result<Vec<Entry>> {
        let key = key.as_ref();
        let mut all_entries = Vec::new();
        let safe = self.inner.safe.read().await;
        let active_blob = safe.active_blob.as_ref();
        if let Some(active_blob) = active_blob {
            let entries = active_blob
                .read()
                .await
                .read_all_entries_with_deletion_marker(key)
                .await?;
            debug!(
                "storage core read all active blob entries {}",
                entries.len()
            );
            if let Some(e) = entries.last() {
                if e.is_deleted() {
                    return Ok(entries);
                }
            }
            all_entries.extend(entries);
        }
        let blobs = safe.blobs.read().await;
        let mut futures = blobs
            .iter_possible_childs_rev(key)
            .map(|b| b.1.data.read_all_entries_with_deletion_marker(key))
            .collect::<FuturesOrdered<_>>();
        while let Some(data) = futures.next().await {
            let entries = data?;
            if let Some(e) = entries.last() {
                if e.is_deleted() {
                    all_entries.extend(entries);
                    return Ok(all_entries);
                }
            }
            all_entries.extend(entries);
        }
        debug!(
            "storage core read from non-active total {} entries",
            all_entries.len()
        );
        debug_assert!(all_entries
            .iter()
            .zip(all_entries.iter().skip(1))
            .all(|(x, y)| x.created() >= y.created()));
        Ok(all_entries)
    }

    async fn read_with_optional_meta(
        &self,
        key: &K,
        meta: Option<&Meta>,
    ) -> Result<ReadResult<Bytes>> {
        debug!("storage read with optional meta {:?}, {:?}", key, meta);
        let safe = self.inner.safe.read().await;
        if let Some(ablob) = safe.active_blob.as_ref() {
            match ablob.read().await.read_last(key, meta, true).await {
                Ok(data) => {
                    if data.is_presented() {
                        debug!("storage read with optional meta active blob returned data");
                        return Ok(data);
                    }
                }
                Err(e) => debug!("read with optional meta active blob returned: {:#?}", e),
            }
        }
        Self::get_data_last(&safe, key, meta).await
    }

    async fn get_data_last(
        safe: &Safe<K>,
        key: &K,
        meta: Option<&Meta>,
    ) -> Result<ReadResult<Bytes>> {
        let blobs = safe.blobs.read().await;
        let possible_blobs = blobs
            .iter_possible_childs_rev(key)
            .map(|(id, blob)| async move {
                if !matches!(blob.data.check_filter(key).await, FilterResult::NotContains) {
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
            .map(|blob| blob.data.read_last(key, meta, false))
            .collect();
        debug!("read with optional meta {} closed blobs", stream.len());

        let mut task = stream.skip_while(|read_res| {
            match read_res {
                Ok(inner_res) => inner_res.is_not_found(), // Skip not found
                Err(e) => {
                    debug!("error reading data from Blob (blob.read_any): {:?}", e);
                    true // skip errors
                }
            }
        });

        task.next().await.unwrap_or(Ok(ReadResult::NotFound))
    }

    /// Stop blob updater and release lock file
    /// # Errors
    /// Fails because of any IO errors
    pub async fn close(self) -> Result<()> {
        let mut res = Ok(());
        {
            let mut safe = self.inner.safe.write().await;
            let active_blob = safe.active_blob.take();      
            if let Some(blob) = active_blob {
                let mut blob = blob.write().await;
                res = res.and(
                    blob.dump()
                        .await
                        .map(|_| info!("active blob dumped"))
                        .with_context(|| format!("blob {} dump failed", blob.name())),
                )
            }
        };

        // Wait for observer worker shutdown. Locks should be released at this point
        self.observer.shutdown().await;
        res
    }

    /// `blob_count` returns exact number of closed blobs plus one active, if there is some.
    /// It locks on inner structure, so it much slower than `next_blob_id`.
    /// # Examples
    /// ```no_run
    /// use pearl::{Builder, Storage, ArrayKey};
    ///
    /// async fn check_blobs_count(storage: Storage<ArrayKey<8>>) {
    ///     assert_eq!(storage.blobs_count().await, 1);
    /// }
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

    /// `blob_count` returns exact number of corrupted blobs.
    pub fn corrupted_blobs_count(&self) -> usize {
        self.inner.corrupted_blobs.load(Ordering::Acquire)
    }

    /// `active_index_memory` returns the amount of memory used by blob to store active indices
    pub async fn active_index_memory(&self) -> usize {
        let safe = self.inner.safe.read().await;
        if let Some(ablob) = safe.active_blob.as_ref() {
            ablob.read().await.index_memory()
        } else {
            0
        }
    }

    /// `disk_used` returns amount of disk space occupied by storage related  files
    pub async fn disk_used(&self) -> u64 {
        let safe = self.inner.safe.read().await;
        let lock = safe.blobs.read().await;
        let mut result = 0;
        if let Some(ablob) = safe.active_blob.as_ref() {
            result += ablob.read().await.disk_used();
        }
        for blob in lock.iter() {
            result += blob.disk_used();
        }
        result
    }

    /// Returns next blob ID. If pearl dir structure wasn't changed from the outside,
    /// returned number is equal to `blobs_count`. But this method doesn't require
    /// lock. So it is much faster than `blobs_count`.
    #[must_use]
    pub fn next_blob_id(&self) -> usize {
        self.inner.next_blob_id.load(Ordering::Acquire)
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
        let corrupted = Self::count_old_corrupted_blobs(&self.inner.config).await;
        self.inner
            .corrupted_blobs
            .store(corrupted, Ordering::Release);

        let next = self.inner.next_blob_name()?;
        let mut safe = self.inner.safe.write().await;
        let blob =
            Blob::open_new(next, self.inner.iodriver.clone(), self.inner.config.blob()).await?;
        safe.active_blob = Some(Box::new(ASRwLock::new(blob)));
        Ok(())
    }

    async fn init_from_existing(&mut self, files: Vec<DirEntry>, with_active: bool) -> Result<()> {
        trace!("init from existing: {:#?}", files);
        let existed_corrupted_blob_count = Self::count_old_corrupted_blobs(&self.inner.config).await;
        let disk_access_sem = self.inner.get_dump_sem();
        let ReadBlobsResult { mut blobs, max_blob_id, new_corrupted_blob_count} = Self::read_blobs(
            &files,
            self.inner.iodriver.clone(),
            disk_access_sem,
            &self.inner.config,
        )
        .await
        .context("failed to read blobs")?;

        self.inner
            .corrupted_blobs
            .store(existed_corrupted_blob_count + new_corrupted_blob_count, Ordering::Release);
        self.inner
            .next_blob_id
            .store(max_blob_id.map_or(0, |i| i + 1), Ordering::Release);

        debug!("{} blobs successfully created", blobs.len());
        blobs.sort_by_key(Blob::id);

        let active_blob = if with_active {
            if blobs.is_empty() && new_corrupted_blob_count > 0 {
                let next = self.inner.next_blob_name()?;
                Some(Blob::open_new(next, self.inner.iodriver.clone(), self.inner.config.blob()).await?)
            } else {
                Some(Self::pop_active(&mut blobs, &self.inner.config).await?)
            }
        } else {
            None
        };

        for blob in &mut blobs {
            debug!("dump all blobs except active blob");
            blob.dump().await?;
        }

        let mut safe = self.inner.safe.write().await;
        safe.active_blob = active_blob.map(|ab| Box::new(ASRwLock::new(ab)));
        *safe.blobs.write().await =
            HierarchicalFilters::from_vec(self.inner.config.bloom_filter_group_size(), 1, blobs).await;
        self.inner.next_blob_id.fetch_max(safe.max_id().await.map_or(0, |i| i + 1), Ordering::AcqRel);
        Ok(())
    }

    async fn pop_active(blobs: &mut Vec<Blob<K>>, config: &Config) -> Result<Blob<K>> {
        let mut active_blob = blobs
            .pop()
            .ok_or_else(|| {
                let wd = config.work_dir();
                error!("No blobs in {:?} to create an active one", wd);
                Error::from(ErrorKind::Uninitialized)
            })?;
        active_blob.load_index().await?;
        Ok(active_blob)
    }

    async fn read_blobs(
        files: &[DirEntry],
        iodriver: IoDriver,
        disk_access_sem: Arc<Semaphore>,
        config: &Config,
    ) -> Result<ReadBlobsResult<K>> {
        let mut corrupted = 0;
        let mut max_blob_id: Option<usize> = None;

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
                Blob::from_file(file.clone(), iodriver.clone(), config.blob())
                    .await
                    .map_err(|e| (e, file))
            })
            .collect();
        debug!("async init blobs from file");
        let mut blobs = Vec::new();
        while let Some(blob_res) = futures.next().await {
            match blob_res {
                Ok(blob) => {
                    max_blob_id = max_blob_id.max(Some(blob.id()));
                    blobs.push(blob);
                },
                Err((e, file)) => {
                    let msg = format!("Failed to read existing blob: {}", file.display());

                    if let Ok(file_name) = blob::FileName::from_path(&file) {
                        max_blob_id = max_blob_id.max(Some(file_name.id()));
                    }

                    if config.ignore_corrupted() {
                        error!("{}, cause: {:#}", msg, e);
                    } else if Self::should_save_corrupted_blob(&e) {
                        warn!(
                            "Corrupted BLOB detected. Save corrupted blob '{}' to directory '{}'",
                            file.display(),
                            config.corrupted_dir_name()
                        );
                        Self::save_corrupted_blob(&file, config.corrupted_dir_name())
                            .await
                            .with_context(|| {
                                anyhow!(format!("failed to save corrupted blob {:?}", file))
                            })?;
                        corrupted += 1;
                    } else {
                        return Err(e.context(msg));
                    }
                }
            }
        }
        Ok(ReadBlobsResult { blobs, new_corrupted_blob_count: corrupted, max_blob_id })
    }

    async fn count_old_corrupted_blobs(config: &Config) -> usize {
        let mut corrupted = 0;

        if let Some(work_dir_path) = config.work_dir() {
            let mut corrupted_dir_path = work_dir_path.to_path_buf();
            corrupted_dir_path.push(config.corrupted_dir_name());
            if corrupted_dir_path.exists() {
                let dir = read_dir(&corrupted_dir_path).await;
                if let Err(e) = dir {
                    warn!(
                        "can't read corrupted blob dir {}: {}",
                        corrupted_dir_path.display(),
                        e
                    );
                    return corrupted;
                }
                let mut dir = dir.unwrap();

                while let Ok(Some(file)) = dir.next_entry().await {
                    let path = file.path();
                    if path.is_file() {
                        let extension = path.extension();
                        if let Some(BLOB_FILE_EXTENSION) = extension.and_then(|ext| ext.to_str()) {
                            corrupted += 1;
                        }
                    }
                }
            }
        }

        corrupted
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
    /// Also returns creation timestamp
    /// # Errors
    /// Fails because of any IO errors
    pub async fn contains(&self, key: impl AsRef<K>) -> Result<ReadResult<BlobRecordTimestamp>> {
        self.contains_with(key.as_ref(), None).await
    }

    async fn contains_with(
        &self,
        key: &K,
        meta: Option<&Meta>,
    ) -> Result<ReadResult<BlobRecordTimestamp>> {
        let inner = self.inner.safe.read().await;
        if let Some(active_blob) = &inner.active_blob {
            let res = active_blob.read().await.contains(key, meta).await?;
            if res.is_presented() {
                return Ok(res);
            }
        }
        let blobs = inner.blobs.read().await;
        for blob in blobs.iter_possible_childs_rev(key) {
            let res = blob.1.data.contains(key, meta).await?;
            if res.is_presented() {
                return Ok(res);
            }
        }

        Ok(ReadResult::NotFound)
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
            active_blob.read().await.check_filter(key).await
        } else {
            FilterResult::NotContains
        };
        if in_active == FilterResult::NeedAdditionalCheck {
            return Some(true);
        }

        let blobs = inner.blobs.read().await;
        let (offloaded, in_memory): (Vec<&Blob<K>>, Vec<&Blob<K>>) =
            blobs.iter().partition(|blob| {
                blob.get_filter_fast()
                    .map_or(false, |filter| filter.is_filter_offloaded())
            });

        let in_closed = in_memory
            .iter()
            .any(|blob| blob.check_filter_fast(key) == FilterResult::NeedAdditionalCheck);
        if in_closed {
            return Some(true);
        }

        let in_closed_offloaded = offloaded
            .iter()
            .map(|blob| blob.check_filter(key))
            .collect::<FuturesUnordered<_>>()
            .any(|value| value == FilterResult::NeedAdditionalCheck)
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

    /// Delete entries with matching key
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn delete(&self, key: impl AsRef<K>, only_if_presented: bool) -> Result<u64> {
        {
            // Try read lock first
            let safe = self.inner.safe.read().await;
            if only_if_presented || safe.active_blob.is_some() {
                return self.delete_core(&safe, key.as_ref(), only_if_presented).await;
            }
        }

        // Active blob should be initialized => use write lock
        let mut safe = self.inner.safe.write().await;
        if !only_if_presented {
            self.inner.ensure_active_blob_exists(&mut safe).await?;
        }
        return self.delete_core(&mut safe, key.as_ref(), only_if_presented).await;
    }

    /// Core deletion logic, when lock on `Safe<K>` is acquired
    async fn delete_core(&self, safe: &Safe<K>, key: &K, only_if_presented: bool) -> Result<u64> {
        let deleted_in_active_result = Self::mark_all_as_deleted_active(safe, key, only_if_presented).await?;
        let deleted_in_active = deleted_in_active_result.as_ref().map(|r| if r.deleted { 1 } else { 0 }).unwrap_or(0);
        let deleted_in_closed = Self::mark_all_as_deleted_closed(safe, key).await?;

        if deleted_in_closed > 0 {
            self.observer.defer_dump_old_blob_indexes().await;
        }
        if let Some(result) = deleted_in_active_result {
            if self.inner.should_try_fsync(result.dirty_bytes) {
                self.observer.try_fsync_data().await;
            }
        }

        debug!("{} deleted total", deleted_in_active + deleted_in_closed);
        Ok(deleted_in_active + deleted_in_closed)
    }

    async fn mark_all_as_deleted_closed(safe: &Safe<K>, key: &K) -> Result<u64> {
        let mut blobs = safe.blobs.write().await;
        let entries_closed_blobs = blobs
            .iter_mut()
            .map(|b| b.mark_all_as_deleted(key, true))
            .collect::<FuturesUnordered<_>>();
        let total = entries_closed_blobs
            .map(|result| match result {
                Ok(result) => {
                    if result.deleted {
                        1
                    } else {
                        0
                    }
                }
                Err(error) => {
                    warn!("failed to delete records: {}", error);
                    0
                }
            })
            .fold(0, |s, n| s + n)
            .await;
        debug!("{} deleted from closed blobs", total);
        Ok(total as u64)
    }

    async fn mark_all_as_deleted_active(
        safe: &Safe<K>,
        key: &K,
        only_if_presented: bool,
    ) -> Result<Option<DeleteResult>> {
        if !only_if_presented {
            assert!(safe.active_blob.is_some(), "Active BLOB should be initialized before calling 'mark_all_as_deleted_active'");
        }
        let active_blob = safe.active_blob.as_deref();
        if let Some(active_blob) = active_blob {
            let result = active_blob
                .write()
                .await
                .mark_all_as_deleted(key, only_if_presented)
                .await?;
            debug!("Deleted {} records from active blob", if result.deleted { 1 } else { 0 });
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

impl<K> Inner<K>
where
    for<'a> K: Key<'a> + 'static,
{
    fn new(config: Config, iodriver: IoDriver) -> Self {
        Self {
            safe: RwLock::new(Safe::new(config.bloom_filter_group_size())),
            config,
            next_blob_id: AtomicUsize::new(0),
            iodriver,
            corrupted_blobs: AtomicUsize::new(0),
            fsync_in_progress: AtomicBool::new(false)
        }
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn io_driver(&self) -> &IoDriver {
        &self.iodriver
    }

    pub(crate) fn safe(&self) -> &RwLock<Safe<K>> {
        &self.safe
    }

    fn get_dump_sem(&self) -> Arc<Semaphore> {
        self.config.dump_sem()
    }

    pub(crate) async fn restore_active_blob(&self) -> Result<()> {
        if self.has_active_blob().await {
            return Err(Error::active_blob_already_exists().into());
        }
        let mut safe = self.safe.write().await;
        if let None = safe.active_blob {
            let blob_opt = safe.blobs.write().await.pop();
            if let Some(blob) = blob_opt {
                safe.active_blob = Some(Box::new(ASRwLock::new(blob)));
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
        self.ensure_active_blob_exists(&mut *safe).await
    }

    async fn ensure_active_blob_exists(&self, safe: &mut Safe<K>) -> Result<()> {
        if let None = safe.active_blob {
            let next = self.next_blob_name()?;
            let blob = Blob::open_new(next, self.iodriver.clone(), self.config.blob()).await?;
            safe.active_blob = Some(Box::new(ASRwLock::new(blob)));
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
                let ablob = (*ablob).into_inner();
                ablob.fsyncdata().await?;
                safe.blobs.write().await.push(ablob).await;
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
            let ablob = ablob.read().await;
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
        let next_id = self.next_blob_id.fetch_add(1, Ordering::AcqRel);
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
        if let Some(ablob) = inner.active_blob.as_ref() {
            Some(ablob.read().await.records_count())
        } else {
            None
        }
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if self.fsync_in_progress.compare_exchange(false, true, Ordering::Release, Ordering::Acquire).is_err() {
            return Ok(())
        }

        let _flag = ResetableFlag { flag: &self.fsync_in_progress };

        let safe = self.safe.read().await;
        if let Some(ablob) = &safe.active_blob {
            let ablob = ablob.read().await;
            if !self.too_many_dirty_bytes(ablob.file_dirty_bytes()) {
                return Ok(());
            }
        }

        safe.fsyncdata().await
    }

    /// Dumps indexes on old blobs. This method is slow, so it is better to run it in background
    pub(crate) async fn try_dump_old_blob_indexes(&self) {
        Safe::try_dump_old_blob_indexes(&self.safe, self.get_dump_sem(), Duration::from_millis(200)).await;
    }

    pub(crate) fn should_try_fsync(&self, dirty_bytes: u64) -> bool {
        self.too_many_dirty_bytes(dirty_bytes) && !self.fsync_in_progress.load(Ordering::Acquire)
    }

    fn too_many_dirty_bytes(&self, dirty_bytes: u64) -> bool {
        dirty_bytes > self.config().max_dirty_bytes_before_sync()
    }
}

struct ResetableFlag<'a> {
    flag: &'a AtomicBool
}

impl<'a> Drop for ResetableFlag<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
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
        let mut id = None;
        if let Some(ablob) = self.active_blob.as_ref() {
            id = Some(ablob.read().await.id());
        }
        let blobs_max_id = self.blobs.read().await.last().map(|x| x.id());
        id.max(blobs_max_id)
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
            let value = (blobs.len(), blob.read().await.records_count());
            debug!("push: {:?}", value);
            results.push(value);
        }
        results
    }

    async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref blob) = self.active_blob {
            blob.read().await.fsyncdata().await?;
        }
        Ok(())
    }

    pub(crate) async fn read_active_blob<'a>(&'a self) -> Option<async_lock::RwLockReadGuard<'a, Blob<K>>> {
        match &self.active_blob {
            None => None,
            Some(blob) => Some(blob.read().await)
        }
    }

    pub(crate) async fn replace_active_blob(&mut self, blob: Blob<K>) -> Result<()> {
        let old_active = self.active_blob.replace(Box::new(ASRwLock::new(blob)));
        if let Some(blob) = old_active {
            self.blobs.write().await.push(blob.into_inner()).await;
        }
        Ok(())
    }

    /// Dumps indexes on old blobs. This method is slow, so it is better to run it in background
    pub(crate) async fn try_dump_old_blob_indexes(safe: &RwLock<Self>, dump_sem: Arc<Semaphore>, max_quantum: Duration) {
        trace!("dump indexes for old blobs started");

        let mut finished = false;
        let mut current_progress: usize = 0;
        while !finished {
            finished = true;

            let safe_guard = safe.read().await;
            let mut write_blobs = safe_guard.blobs.write().await;  

            let quantum_start = Instant::now();
            let progress_start = current_progress; // Every new iteration should go further
            for blob in write_blobs.iter_mut().skip(current_progress) {
                if current_progress > progress_start && quantum_start.elapsed() > max_quantum {
                    // Time quantum ended. Unlock blobs to allow other threads to work
                    finished = false;
                    break;
                }
                current_progress += 1;
                let _ = dump_sem.acquire().await;
                if let Err(e) = blob.dump().await {
                    error!("Error dumping blob ({}): {}", blob.name(), e);
                } else {
                    trace!("finished dumping old blob: {}", blob.name());
                }
            }
        }

        trace!("dump indexes for old blobs finished");
    }
}

#[async_trait::async_trait]
impl<K> BloomProvider<K> for Storage<K>
where
    for<'a> K: Key<'a> + 'static,
{
    type Filter = <Blob<K> as BloomProvider<K>>::Filter;
    async fn check_filter(&self, item: &K) -> FilterResult {
        let inner = self.inner.safe.read().await;
        let active = if let Some(ablob) = inner.active_blob.as_ref() {
            ablob.read().await.check_filter_fast(item)
        } else {
            FilterResult::default()
        };
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
            if let Some(ablob) = inner.active_blob.as_ref() {
                let ablob = ablob.read().await;
                if let Some(active_filter) = ablob.get_filter_fast() {
                    if !filter.checked_add_assign(active_filter) {
                        return None;
                    }
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
        let active = if let Some(blob) = safe.active_blob.as_ref() {
            blob.read().await.filter_memory_allocated().await
        } else {
            0
        };

        let closed = safe.blobs.read().await.filter_memory_allocated().await;
        active + closed
    }
}
