use std::{
    ops::{Deref, DerefMut},
    sync::Weak,
};

use super::*;

/// Container for types which is support bloom filtering
pub struct HierarchicalBloom<Child> {
    inner: Arc<RwLock<HierarchicalBloomInner<Child>>>,
    children: Vec<Arc<Leaf<Child>>>,
    group_size: usize,
}

/// Leaf of filter tree
#[derive(Debug)]
pub struct Leaf<T> {
    parent: Weak<RwLock<HierarchicalBloomInner<T>>>,
    data: RwLock<T>,
}

impl<Child, Key> Leaf<Child>
where
    Child: BloomProvider<Key = Key> + Send + Sync,
    Key: Send + Sync + AsRef<[u8]> + ?Sized,
{
    /// Data associated with leaf
    pub fn data(&self) -> &RwLock<Child> {
        &self.data
    }

    /// Adds key to parent nodes
    pub async fn add_to_parents(&self, key: &[u8]) {
        if let Some(parent) = self.parent.upgrade() {
            parent.write().await.add_to_parents(key).await;
        }
    }
}

#[derive(Default)]
struct Node<T> {
    filter: Option<Bloom>,
    children: Vec<Arc<RwLock<HierarchicalBloomInner<T>>>>,
    parent: Weak<RwLock<HierarchicalBloomInner<T>>>,
}

impl<T> Debug for Node<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Node")
            .field("filter", &self.filter)
            .finish()
    }
}

impl<T: Debug> Debug for HierarchicalBloom<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("HierarchicalBloom")
            .field("inner", &self.inner)
            .field("group_size", &self.group_size)
            .finish()
    }
}

#[derive(Debug)]
enum HierarchicalBloomInner<T> {
    /// Inner filters
    Node(Node<T>),
    /// Leaf
    Leaf(Weak<Leaf<T>>),
}

impl<T> Default for HierarchicalBloomInner<T> {
    fn default() -> Self {
        Self::Node(Node {
            filter: None,
            children: vec![],
            parent: Default::default(),
        })
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
            group_size: 8,
        }
    }
}

impl<Child, Key> HierarchicalBloomInner<Child>
where
    Child: BloomProvider<Key = Key> + Send + Sync,
    Key: Send + Sync + AsRef<[u8]> + ?Sized,
{
    #[async_recursion::async_recursion]
    async fn add_to_parents(&mut self, item: &[u8]) {
        match self {
            Self::Node(node) => {
                if let Some(filter) = &mut node.filter {
                    let _ = filter.add(item);
                }
                if let Some(parent) = node.parent.upgrade() {
                    parent.write().await.add_to_parents(item).await;
                }
            }
            Self::Leaf(leaf) => {
                if let Some(leaf) = leaf.upgrade() {
                    if let Some(parent) = leaf.parent.upgrade() {
                        parent.write().await.add_to_parents(item).await;
                    }
                }
            }
        }
    }

    fn is_child_dropped(&self) -> bool {
        match self {
            Self::Leaf(leaf) => leaf.strong_count() == 0,
            _ => false,
        }
    }

    #[async_recursion::async_recursion]
    async fn filter_memory_allocated(&self) -> usize {
        match &self {
            Self::Node(node) => {
                let mut allocated = node
                    .filter
                    .as_ref()
                    .map(|x| x.memory_allocated())
                    .unwrap_or_default();
                for child in node.children.iter() {
                    allocated += child.read().await.filter_memory_allocated().await;
                }
                allocated
            }
            Self::Leaf(leaf) => {
                if let Some(leaf) = leaf.upgrade() {
                    leaf.data.read().await.filter_memory_allocated().await
                } else {
                    0
                }
            }
        }
    }

    #[async_recursion::async_recursion]
    async fn check_filter(&self, item: &Key, check_leafs: bool) -> Option<bool> {
        match self {
            HierarchicalBloomInner::Node(node) => {
                if let Some(filter) = &node.filter {
                    if let Some(false) = filter.contains_in_memory(item) {
                        return Some(false);
                    }
                }
                let mut has_none = false;
                for filter in node.children.iter() {
                    if self.is_child_dropped() {
                        continue;
                    }
                    match filter.read().await.check_filter(item, check_leafs).await {
                        Some(true) => return Some(true),
                        Some(false) => {}
                        _ => {
                            has_none = true;
                        }
                    }
                }
                if has_none {
                    None
                } else {
                    Some(false)
                }
            }
            HierarchicalBloomInner::Leaf(leaf) if check_leafs => {
                leaf.upgrade()?.data.read().await.check_filter(item).await
            }
            _ => None,
        }
    }

    #[async_recursion::async_recursion]
    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        match self {
            Self::Node(node) => {
                let mut freed = 0;
                for filter in node.children.iter() {
                    if freed >= needed_memory {
                        return freed;
                    }
                    freed += filter.write().await.offload_buffer(needed_memory).await;
                }
                freed
            }
            Self::Leaf(child) => {
                if let Some(child) = child.upgrade() {
                    child.data.write().await.offload_buffer(needed_memory).await
                } else {
                    0
                }
            }
        }
    }

    fn merge_filters(dest: &mut Option<Bloom>, source: &Option<Bloom>) {
        match source {
            Some(source_filter) => {
                let res = match dest {
                    Some(dest_filter) => dest_filter.checked_add_assign(source_filter),
                    _ => None,
                };
                if res.is_none() {
                    error!("{:?} + {:?}", dest, source);
                    *dest = None;
                }
            }
            None => *dest = None,
        }
    }

    async fn get_filter(&self) -> Option<Bloom> {
        match self {
            Self::Node(node) => node.filter.clone(),
            Self::Leaf(leaf) => leaf.upgrade()?.data.read().await.get_filter().await,
        }
    }

    async fn push_group(&mut self, value: Arc<RwLock<Self>>) {
        match self {
            Self::Node(node) => {
                if node.children.is_empty() {
                    node.filter = value.read().await.get_filter().await.clone();
                    error!("Initial filter: {:?}", node.filter);
                } else {
                    Self::merge_filters(&mut node.filter, &value.read().await.get_filter().await);
                }
                node.children.push(value);
            }
            _ => unreachable!(),
        }
    }

    async fn push_item(&mut self, child: Arc<Leaf<Child>>) {
        match self {
            Self::Node(node) => {
                if node.children.is_empty() {
                    node.filter = child.data.read().await.get_filter().await.clone();
                } else {
                    Self::merge_filters(
                        &mut node.filter,
                        &child.data.read().await.get_filter().await,
                    )
                }
                node.children
                    .push(Arc::new(RwLock::new(Self::Leaf(Arc::downgrade(&child)))));
            }
            _ => unreachable!(),
        }
    }

    fn empty_child(this: &Arc<RwLock<Self>>) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::Node(Node {
            parent: Arc::downgrade(this),
            children: vec![],
            filter: Default::default(),
        })))
    }

    async fn push(this: &Arc<RwLock<Self>>, child: Child, group_size: usize) -> Arc<Leaf<Child>> {
        match this.write().await.deref_mut() {
            Self::Node(node) => {
                if node.children.is_empty() {
                    node.children.push(Self::empty_child(this));
                    node.filter = child.get_filter().await.clone();
                } else {
                    Self::merge_filters(&mut node.filter, &child.get_filter().await)
                }
                let last = node.children.last().expect("Have values").clone();
                let leaf = Arc::new(Leaf {
                    data: RwLock::new(child),
                    parent: Arc::downgrade(&last),
                });
                let create_new_group = {
                    let mut last_mut = last.write().await;
                    last_mut.push_item(leaf.clone()).await;
                    last_mut.len() >= group_size
                };
                if create_new_group {
                    let last = node.children.pop().expect("Have values");
                    if node.children.is_empty() {
                        node.children.push(Self::empty_child(this));
                    }
                    let pred_last = node.children.last().expect("Have values");
                    pred_last.write().await.push_group(last).await;
                    node.children.push(Default::default());
                }
                leaf
            }
            _ => unreachable!(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Node(node) => node.children.len(),
            _ => 0,
        }
    }
}

#[async_trait::async_trait]
impl<Child, Key> BloomProvider for HierarchicalBloom<Child>
where
    Child: BloomProvider<Key = Key> + Send + Sync,
    Key: Send + Sync + AsRef<[u8]> + ?Sized,
{
    type Key = Key;

    async fn check_filter(&self, item: &Self::Key) -> Option<bool> {
        self.inner.read().await.check_filter(item, true).await
    }

    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        self.inner.write().await.offload_buffer(needed_memory).await
    }

    async fn get_filter(&self) -> Option<Bloom> {
        match &self.inner.read().await.deref() {
            HierarchicalBloomInner::Node(node) => node.filter.clone(),
            _ => None,
        }
    }

    async fn filter_memory_allocated(&self) -> usize {
        self.inner.read().await.filter_memory_allocated().await
    }
}

impl<Child> HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
    <Child as BloomProvider>::Key: AsRef<[u8]>,
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

    /// Returns a iterator over the childs
    pub fn iter(&self) -> core::slice::Iter<'_, Arc<Leaf<Child>>> {
        self.children.iter()
    }

    /// Returns last child
    pub fn last(&self) -> Option<&Arc<Leaf<Child>>> {
        self.children.last()
    }

    /// Add child to collection
    pub async fn push(&mut self, item: Child) {
        let leaf = HierarchicalBloomInner::push(&self.inner, item, self.group_size).await;
        self.children.push(leaf);
    }

    /// Returns children elements as Vec
    pub fn into_vec(self) -> Vec<Arc<Leaf<Child>>> {
        self.children
    }

    /// Returns mutable reference to inner container.
    pub fn children_mut(&mut self) -> &mut Vec<Arc<Leaf<Child>>> {
        &mut self.children
    }

    /// Returns reference to inner container.
    pub fn children(&self) -> &Vec<Arc<Leaf<Child>>> {
        &self.children
    }

    /// Extends conatiner with values
    pub async fn extend(&mut self, values: Vec<Child>) {
        for child in values {
            self.push(child).await;
        }
    }

    /// Checks intermediate filters and skip leafs
    pub async fn check_filter_without_leafs(&self, item: &Child::Key) -> Option<bool> {
        self.inner.read().await.check_filter(item, false).await
    }
}

impl<Child> IntoIterator for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    type Item = Arc<Leaf<Child>>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.children.into_iter()
    }
}
