use std::{
    ops::{Deref, DerefMut},
    sync::Weak,
};

use super::*;

#[derive(Hash, Clone, Copy, Default, Debug, PartialEq, Eq)]
/// Represents id of inner in collection
pub struct InnerId(u64);

#[derive(Hash, Clone, Copy, Default, Debug, PartialEq, Eq)]
/// Represents id of child in collection
pub struct ChildId(u64);

impl ChildId {
    fn get_inc(&mut self) -> Self {
        let ret = *self;
        self.0 += 1;
        ret
    }
}

impl InnerId {
    fn get_inc(&mut self) -> Self {
        let ret = *self;
        self.0 += 1;
        ret
    }
}

#[derive(Debug)]
/// Container for types which is support bloom filtering
pub struct HierarchicalBloom<Child> {
    inner: HashMap<InnerId, HierarchicalBloomInner>,
    children: HashMap<ChildId, Leaf<Child>>,
    root: InnerId,
    inner_id: InnerId,
    children_id: ChildId,
    group_size: usize,
}

/// Leaf of filter tree
#[derive(Debug)]
pub struct Leaf<T> {
    /// Leaf parent id
    pub parent: InnerId,
    /// Leaf data
    pub data: T,
}

#[derive(Debug, Clone)]
enum HierarchicalBloomInner {
    Node {
        filter: Option<Bloom>,
        children: Vec<InnerId>,
        parent: Option<InnerId>,
    },
    Leaf {
        parent: InnerId,
        leaf: ChildId,
    },
}

impl Default for HierarchicalBloomInner {
    fn default() -> Self {
        Self::Node {
            filter: None,
            children: vec![],
            parent: Default::default(),
        }
    }
}

impl<Child> Default for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn default() -> Self {
        let mut inner_id = InnerId::default();
        let root = inner_id.get_inc();
        let mut inner = HashMap::<_, _>::default();
        inner.insert(root, Default::default());
        Self {
            inner,
            children: Default::default(),
            inner_id,
            children_id: Default::default(),
            root,
            group_size: 8,
        }
    }
}

impl HierarchicalBloomInner {
    fn len(&self) -> usize {
        match self {
            HierarchicalBloomInner::Node { children, .. } => children.len(),
            HierarchicalBloomInner::Leaf { .. } => panic!("Should not be called on leaf"),
        }
    }

    fn add_node(&mut self, node_filter: &Option<Bloom>, id: InnerId) {
        match self {
            HierarchicalBloomInner::Node {
                filter, children, ..
            } => {
                Self::merge_filters(filter, node_filter);
                children.push(id);
            }
            HierarchicalBloomInner::Leaf { .. } => panic!("Should not be called on leaf"),
        }
    }

    fn add_to_filter(&mut self, item: &[u8]) {
        match self {
            Self::Node { filter, .. } => {
                if let Some(filter) = filter {
                    let _ = filter.add(item);
                }
            }
            _ => {}
        }
    }

    fn parent_id(&self) -> Option<InnerId> {
        match &self {
            Self::Node { parent, .. } => parent.clone(),
            Self::Leaf { parent, .. } => Some(parent.clone()),
        }
    }

    fn filter_memory_allocated(&self) -> usize {
        match &self {
            Self::Node { filter, .. } => filter.as_ref().map(|f| f.memory_allocated()).unwrap_or(0),
            _ => 0,
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
}

#[async_trait::async_trait]
impl<Child, Key> BloomProvider for HierarchicalBloom<Child>
where
    Child: BloomProvider<Key = Key> + Send + Sync,
    Key: Send + Sync + AsRef<[u8]> + ?Sized,
{
    type Key = Key;

    async fn check_filter(&self, item: &Self::Key) -> Option<bool> {
        self.check_filter_in(self.root, item, true).await
    }

    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        self.offload_filters_in(self.root, needed_memory).await
    }

    async fn get_filter(&self) -> Option<Bloom> {
        match &self.inner.get(&self.root) {
            Some(HierarchicalBloomInner::Node { filter, .. }) => filter.clone(),
            _ => None,
        }
    }

    async fn filter_memory_allocated(&self) -> usize {
        self.filter_memory_allocated_in(self.root).await
    }
}

impl<Child> HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
    <Child as BloomProvider>::Key: AsRef<[u8]>,
{
    #[async_recursion::async_recursion]
    async fn check_filter_in(
        &self,
        id: InnerId,
        item: &Child::Key,
        check_child: bool,
    ) -> Option<bool> {
        if let Some(curr) = self.inner.get(&id) {
            match curr {
                HierarchicalBloomInner::Node {
                    filter, children, ..
                } => {
                    if let Some(filter) = filter {
                        if let Some(true) = filter.contains_in_memory(item) {
                            return Some(true);
                        }
                    }
                    let mut have_none = false;
                    for id in children {
                        match self.check_filter_in(*id, item, check_child).await {
                            Some(true) => return Some(true),
                            None => have_none = true,
                            _ => {}
                        }
                    }
                    if have_none {
                        None
                    } else {
                        Some(false)
                    }
                }
                HierarchicalBloomInner::Leaf { leaf, .. } if check_child => {
                    if let Some(child) = self.children.get(leaf) {
                        child.data.check_filter(item).await
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    #[async_recursion::async_recursion]
    async fn filter_memory_allocated_in(&self, id: InnerId) -> usize {
        if let Some(curr) = self.inner.get(&id) {
            match &curr {
                HierarchicalBloomInner::Node {
                    filter, children, ..
                } => {
                    let mut allocated = filter.as_ref().map(|f| f.memory_allocated()).unwrap_or(0);
                    for id in children {
                        allocated += self.filter_memory_allocated_in(*id).await;
                    }
                    allocated
                }
                HierarchicalBloomInner::Leaf { leaf, .. } => {
                    if let Some(leaf) = self.children.get(leaf) {
                        leaf.data.filter_memory_allocated().await
                    } else {
                        0
                    }
                }
            }
        } else {
            0
        }
    }

    async fn offload_filter(&mut self, id: InnerId, needed_memory: usize) -> usize {
        if let Some(curr) = self.inner.get_mut(&id) {
            match curr {
                HierarchicalBloomInner::Node { filter, .. } => filter
                    .as_mut()
                    .map(|f| f.offload_from_memory())
                    .unwrap_or(0),
                HierarchicalBloomInner::Leaf { leaf, .. } => {
                    if let Some(leaf) = self.children.get_mut(leaf) {
                        leaf.data.offload_buffer(needed_memory).await
                    } else {
                        0
                    }
                }
            }
        } else {
            0
        }
    }

    #[async_recursion::async_recursion]
    async fn offload_filters_in(&mut self, id: InnerId, needed_memory: usize) -> usize {
        if let Some(curr) = self.inner.get(&id).cloned() {
            match &curr {
                HierarchicalBloomInner::Node { children, .. } => {
                    let mut freed = 0;
                    for id in children {
                        if freed >= needed_memory {
                            return freed;
                        }
                        freed += self
                            .offload_filters_in(*id, needed_memory.saturating_sub(freed))
                            .await;
                    }
                    if freed >= needed_memory {
                        return freed;
                    }
                    freed + self.offload_filter(id, needed_memory).await
                }
                HierarchicalBloomInner::Leaf { .. } => self.offload_filter(id, needed_memory).await,
            }
        } else {
            0
        }
    }

    /// Add key to all parents in collection
    pub fn add_to_parents(&mut self, child_id: ChildId, item: &[u8]) {
        if let Some(child) = self.children.get(&child_id) {
            let mut id = child.parent;
            loop {
                let curr = self.inner.get_mut(&id);
                if let Some(curr) = curr {
                    curr.add_to_filter(item);
                    if let Some(parent) = curr.parent_id() {
                        id = parent;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }

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
    pub fn iter(&self) -> impl Iterator<Item = (&ChildId, &Leaf<Child>)> {
        self.children.iter()
    }

    /// Returns a iterator over the childs
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&ChildId, &mut Leaf<Child>)> {
        self.children.iter_mut()
    }

    /// Returns id of next child
    pub fn next_id(&self) -> u64 {
        self.children_id.0
    }

    /// Add child to collection
    pub async fn push(&mut self, item: Child) {
        let item_filter = item.get_filter().await;
        let root_len = self
            .inner
            .get(&self.root)
            .expect("should be presented")
            .len();
        let last_container = if root_len == 0 {
            *self.get_mut(self.root).1 = item_filter.clone();
            self.push_inner_container()
        } else {
            self.last_inner_container().expect("should exist")
        };
        let child_id = self.children_id.get_inc();
        let child = Leaf {
            data: item,
            parent: last_container,
        };
        self.children.insert(child_id, child);
        let inner_id = self.inner_id.get_inc();
        self.inner.insert(
            inner_id,
            HierarchicalBloomInner::Leaf {
                parent: inner_id,
                leaf: child_id,
            },
        );
        self.add_child(last_container, inner_id, &item_filter);
    }

    fn add_child(&mut self, id: InnerId, child_id: InnerId, child_filter: &Option<Bloom>) {
        let (children, filter) = self.get_mut(id);
        if children.is_empty() {
            *filter = child_filter.clone();
        } else {
            HierarchicalBloomInner::merge_filters(filter, &child_filter);
        }
        children.push(child_id);
    }

    fn get_mut(&mut self, id: InnerId) -> (&mut Vec<InnerId>, &mut Option<Bloom>) {
        match self.inner.get_mut(&id) {
            Some(HierarchicalBloomInner::Node {
                children, filter, ..
            }) => (children, filter),
            _ => unreachable!(),
        }
    }

    fn get(&self, id: InnerId) -> (&Vec<InnerId>, &Option<Bloom>) {
        match self.inner.get(&id) {
            Some(HierarchicalBloomInner::Node {
                children, filter, ..
            }) => (children, filter),
            _ => unreachable!(),
        }
    }

    fn last_inner_container(&self) -> Option<InnerId> {
        self.get(self.root).0.last().cloned()
    }

    fn push_inner_container(&mut self) -> InnerId {
        let inner_id = self.inner_id.get_inc();
        self.inner.insert(
            inner_id,
            HierarchicalBloomInner::Node {
                parent: Some(self.root),
                filter: None,
                children: vec![],
            },
        );
        self.get_mut(self.root).0.push(inner_id);
        inner_id
    }

    fn get_inner_to_add_leaf(&mut self) {}

    /// Returns children elements as Vec
    pub fn into_vec(self) -> Vec<Leaf<Child>> {
        self.children.into_values().collect()
    }

    /// Returns mutable reference to inner container.
    pub fn children_mut(&mut self) -> impl Iterator<Item = (&ChildId, &mut Leaf<Child>)> {
        self.children.iter_mut()
    }

    /// Returns reference to inner container.
    pub fn children(&mut self) -> impl Iterator<Item = (&ChildId, &Leaf<Child>)> {
        self.children.iter()
    }

    /// Extends conatiner with values
    pub async fn extend(&mut self, values: Vec<Child>) {
        for child in values {
            self.push(child).await;
        }
    }

    /// Checks intermediate filters and skip leafs
    pub async fn check_filter_without_leafs(&self, item: &Child::Key) -> Option<bool> {
        self.check_filter_in(self.root, item, false).await
    }
}

impl<Child> IntoIterator for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    type Item = (ChildId, Leaf<Child>);

    type IntoIter = std::collections::hash_map::IntoIter<ChildId, Leaf<Child>>;

    fn into_iter(self) -> Self::IntoIter {
        self.children.into_iter()
    }
}
