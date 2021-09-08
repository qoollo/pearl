use super::*;

#[derive(Debug)]
pub struct HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    inner: HierarchicalBloomInner<Arc<Child>>,
    children: Vec<Arc<Child>>,
}

#[derive(Debug)]
enum HierarchicalBloomInner<Child>
where
    Child: BloomProvider + Send + Sync,
{
    /// Inner filters
    Vec(Vec<HierarchicalBloomInner<Child>>),
    /// Leaf
    Item(Child),
    /// Intermediate filter
    Filter(Bloom),
}

impl<Child> Default for HierarchicalBloomInner<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn default() -> Self {
        Self::Vec(Default::default())
    }
}

impl<Child> Default for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn default() -> Self {
        Self {
            inner: Default::default(),
            children: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<Child, Key> BloomProvider for HierarchicalBloomInner<Child>
where
    Child: BloomProvider<Key = Key> + Send + Sync,
    Key: Send + Sync + AsRef<[u8]>,
{
    type Key = Key;
    async fn check_filter(&self, item: &Self::Key) -> Result<Option<bool>> {
        match self {
            HierarchicalBloomInner::Vec(vec) => {
                let mut falses = false;
                for filter in vec {
                    match filter.check_filter(item).await {
                        Ok(Some(true)) => return Ok(Some(true)),
                        Ok(Some(false)) => {}
                        _ => {
                            falses = false;
                        }
                    }
                }
                if falses {
                    Ok(Some(false))
                } else {
                    Ok(None)
                }
            }
            HierarchicalBloomInner::Item(child) => child.check_filter(item).await,
            HierarchicalBloomInner::Filter(filter) => {
                if let Ok(res) = filter.contains_in_memory(item) {
                    Ok(Some(res))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        match self {
            HierarchicalBloomInner::Vec(vec) => {
                let mut freed = 0;
                for filter in vec {
                    freed += filter.offload_buffer(needed_memory).await;
                }
                freed
            }
            HierarchicalBloomInner::Item(child) => child.offload_buffer(needed_memory).await,
            HierarchicalBloomInner::Filter(filter) => filter.offload_from_memory(),
        }
    }
}

impl<Child> HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    /// Create new container
    pub fn new() -> Self {
        Default::default()
    }

    /// Count of childs in container
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Clear container to default value
    pub fn clear(&mut self) {
        *self = Default::default();
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Arc<Child>> {
        self.children.iter()
    }
}

impl<Child> From<Vec<Child>> for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn from(vec: Vec<Child>) -> Self {
        let children: Vec<Arc<Child>> = vec.into_iter().map(|child| child.into()).collect();
        Self {
            inner: HierarchicalBloomInner::Vec(
                children
                    .iter()
                    .map(|child| HierarchicalBloomInner::Item(child.clone()))
                    .collect(),
            ),
            children,
        }
    }
}
