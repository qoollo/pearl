use std::{borrow::Cow, collections::BTreeSet};

use super::*;

#[derive(Hash, Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// Represents id of inner in collection
pub struct InnerId(u64);

#[derive(Hash, Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

/// Container for types which is support bloom filtering
pub struct HierarchicalBloom<Child> {
    inner: BTreeMap<InnerId, HierarchicalBloomInner>,
    children: BTreeMap<ChildId, Leaf<Child>>,
    root: InnerId,
    inner_id: InnerId,
    children_id: ChildId,
    group_size: usize,
    level: usize,
}

impl<T> Debug for HierarchicalBloom<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct Inner<'a, T>(&'a HierarchicalBloom<T>, InnerId);
        impl<'a, T> Debug for Inner<'a, T> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                match self.0.inner.get(&self.1) {
                    Some(HierarchicalBloomInner::Node(node)) => f
                        .debug_struct("Node")
                        .field("filter", &node.filter)
                        .field(
                            "children",
                            &node
                                .children
                                .iter()
                                .map(|x| Inner(self.0, *x))
                                .collect::<Vec<_>>(),
                        )
                        .finish(),
                    Some(HierarchicalBloomInner::Leaf(leaf)) => {
                        f.debug_struct("Leaf").field("leaf", &leaf.leaf).finish()
                    }
                    None => f.debug_struct("Empty").finish(),
                }
            }
        }
        f.debug_struct("HierarchicalBloom")
            .field("inner", &Inner(self, self.root))
            .field("group_size", &self.group_size)
            .field("level", &self.level)
            .finish()
    }
}

#[derive(Debug)]
/// Leaf
pub struct Leaf<T> {
    /// Leaf parent id
    pub parent: InnerId,
    /// Leaf data
    pub data: T,
}

#[derive(Debug, Clone)]
struct InnerNode {
    filter: Option<Bloom>,
    children: Vec<InnerId>,
    parent: Option<InnerId>,
}

#[derive(Debug, Clone)]
struct InnerLeaf {
    parent: InnerId,
    leaf: ChildId,
}

#[derive(Debug, Clone)]
enum HierarchicalBloomInner {
    Node(InnerNode),
    Leaf(InnerLeaf),
}

impl Default for HierarchicalBloomInner {
    fn default() -> Self {
        Self::Node(InnerNode {
            filter: None,
            children: vec![],
            parent: Default::default(),
        })
    }
}

impl HierarchicalBloomInner {
    fn add_to_filter(&mut self, item: &[u8]) {
        match self {
            Self::Node(node) => {
                if let Some(filter) = &mut node.filter {
                    let _ = filter.add(item);
                }
            }
            _ => {}
        }
    }

    fn parent_id(&self) -> Option<InnerId> {
        match &self {
            Self::Node(node) => node.parent.clone(),
            Self::Leaf(leaf) => Some(leaf.parent.clone()),
        }
    }

    fn merge_filters(dest: &mut Option<Bloom>, source: Option<&Bloom>) {
        match source {
            Some(source_filter) => {
                let res = match dest {
                    Some(dest_filter) => dest_filter.checked_add_assign(source_filter),
                    _ => None,
                };
                if res.is_none() {
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
    Child: BloomProvider<Key = Key> + Send + Sync + 'static,
    Key: Send + Sync + AsRef<[u8]> + ?Sized,
{
    type Key = Key;

    async fn check_filter(&self, item: &Self::Key) -> FilterResult {
        let res = self
            .iter_possible_childs(item.as_ref())
            .map(|(_, leaf)| leaf.data.check_filter(item))
            .collect::<FuturesUnordered<_>>()
            .fold(FilterResult::NotContains, |acc, x| acc + x)
            .await;
        res
    }

    fn check_filter_fast(&self, item: &Self::Key) -> FilterResult {
        if self.iter_possible_childs(item.as_ref()).next().is_some() {
            FilterResult::NeedAdditionalCheck
        } else {
            FilterResult::NotContains
        }
    }

    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        let mut freed = 0;
        let mut parents = BTreeSet::new();
        for child in self.children.values_mut() {
            if freed >= needed_memory {
                return freed;
            }
            parents.insert(child.parent);
            freed += child
                .data
                .offload_buffer(needed_memory - freed, level)
                .await;
        }
        match level.cmp(&self.level) {
            std::cmp::Ordering::Less => freed,
            _ => {
                while !parents.is_empty() {
                    let mut new_parents = BTreeSet::new();
                    for parent in parents.iter() {
                        if freed >= needed_memory {
                            return freed;
                        }
                        match self.inner.get_mut(parent) {
                            Some(HierarchicalBloomInner::Node(node)) => {
                                freed += node
                                    .filter
                                    .as_mut()
                                    .map(|x| x.offload_from_memory())
                                    .unwrap_or_default();
                                if let Some(parent) = &node.parent {
                                    new_parents.insert(*parent);
                                }
                            }
                            _ => {}
                        }
                    }
                    parents = new_parents;
                }
                freed
            }
        }
    }

    async fn get_filter(&self) -> Option<Bloom> {
        match &self.inner.get(&self.root) {
            Some(HierarchicalBloomInner::Node(node)) => node.filter.clone(),
            _ => None,
        }
    }

    async fn filter_memory_allocated(&self) -> usize {
        let mut allocated = 0;
        for value in self.inner.values() {
            if let HierarchicalBloomInner::Node(InnerNode {
                filter: Some(filter),
                ..
            }) = value
            {
                allocated += filter.memory_allocated();
            }
        }
        for child in self.children.values() {
            allocated += child.data.filter_memory_allocated().await;
        }
        allocated
    }

    fn get_filter_fast(&self) -> Option<&Bloom> {
        match &self.inner.get(&self.root) {
            Some(HierarchicalBloomInner::Node(node)) => node.filter.as_ref(),
            _ => None,
        }
    }
}

impl<Child> HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
    <Child as BloomProvider>::Key: AsRef<[u8]>,
{
    /// Create collection from vec
    pub async fn from_vec(group_size: usize, level: usize, childs: Vec<Child>) -> Self {
        let mut ret = Self::new(group_size, level);
        for child in childs {
            ret.push(child).await;
        }
        ret
    }

    async fn get_filter_from_child(item: &Child) -> Option<Cow<'_, Bloom>> {
        if let Some(filter) = item.get_filter_fast() {
            Some(Cow::Borrowed(filter))
        } else {
            let filter = item.get_filter().await;
            filter.map(|x| Cow::Owned(x))
        }
    }

    /// Reload all filters and recreate container
    pub async fn reload(&mut self) {
        let values = self.clear_and_get_values();
        self.extend(values).await;
    }

    /// Add child to collection
    pub async fn push(&mut self, child: Child) -> ChildId {
        if self.children_id < ChildId(self.group_size as u64) {
            let res = self.add_child(self.root, child).await;
            if self.children_id >= ChildId(self.group_size as u64) {
                let new_root_id = self.inner_id.get_inc();
                let filter = {
                    let root = self.root_mut();
                    root.parent = Some(new_root_id);
                    root.filter.clone()
                };
                let new_root = HierarchicalBloomInner::Node(InnerNode {
                    filter,
                    children: vec![self.root],
                    parent: None,
                });
                self.root = new_root_id;
                self.inner.insert(new_root_id, new_root);
            }
            res
        } else {
            let mut id = self.last_inner_container().unwrap();
            if self.get(id).children.len() >= self.group_size {
                id = self.new_inner_container();
            }
            self.add_child(id, child).await
        }
    }

    async fn add_child(&mut self, container: InnerId, child: Child) -> ChildId {
        let item_filter = Self::get_filter_from_child(&child).await;
        let inner_id = self.inner_id.get_inc();
        let child_id = self.children_id.get_inc();
        self.inner.insert(
            inner_id,
            HierarchicalBloomInner::Leaf(InnerLeaf {
                parent: container,
                leaf: child_id,
            }),
        );

        let mut parent = {
            let node = self.get_mut(container);
            if node.children.is_empty() {
                Self::init_filter_from_cow(&mut node.filter, &item_filter);
            } else {
                Self::add_filter_from_item(&mut node.filter, &item_filter);
            }
            node.children.push(inner_id);
            node.parent
        };

        while let Some(id) = parent {
            let node = self.get_mut(id);
            Self::add_filter_from_item(&mut node.filter, &item_filter);
            parent = node.parent;
        }

        drop(item_filter);
        let child = Leaf {
            data: child,
            parent: container,
        };
        self.children.insert(child_id, child);
        child_id
    }

    fn last_inner_container(&self) -> Option<InnerId> {
        self.root().children.last().copied()
    }

    /// Extends conatiner with values
    pub async fn extend(&mut self, values: Vec<Child>) {
        for child in values {
            self.push(child).await;
        }
    }
}

impl<Child> HierarchicalBloom<Child> {
    /// Iter over childs which may contain key
    pub fn iter_possible_childs<'a>(&'a self, key: &'a [u8]) -> PossibleRevIter<'a, Child> {
        PossibleRevIter::new(self, key, false)
    }

    /// Iter over childs which may contain key in reverse order
    pub fn iter_possible_childs_rev<'a>(&'a self, key: &'a [u8]) -> PossibleRevIter<'a, Child> {
        PossibleRevIter::new(self, key, true)
    }

    /// Returns children elements as Vec
    pub fn into_vec(self) -> Vec<Leaf<Child>> {
        self.children.into_values().collect()
    }

    /// Returns mutable reference to inner container.
    pub fn children_mut(&mut self) -> impl Iterator<Item = (&ChildId, &mut Leaf<Child>)> {
        self.children.iter_mut()
    }

    /// Returns reference to inner container.
    pub fn children(&self) -> impl Iterator<Item = (&ChildId, &Leaf<Child>)> {
        self.children.iter()
    }

    fn root(&self) -> &InnerNode {
        self.get(self.root)
    }

    fn root_mut(&mut self) -> &mut InnerNode {
        self.get_mut(self.root)
    }

    fn get_mut(&mut self, id: InnerId) -> &mut InnerNode {
        match self.inner.get_mut(&id) {
            Some(HierarchicalBloomInner::Node(node)) => node,
            _ => panic!("node not found"),
        }
    }

    fn get(&self, id: InnerId) -> &InnerNode {
        match self.inner.get(&id) {
            Some(HierarchicalBloomInner::Node(node)) => node,
            _ => panic!("node not found"),
        }
    }

    fn new_inner_container(&mut self) -> InnerId {
        let container = HierarchicalBloomInner::Node(InnerNode {
            parent: Some(self.root),
            children: vec![],
            filter: None,
        });
        let container_id = self.inner_id.get_inc();
        self.root_mut().children.push(container_id);
        self.inner.insert(container_id, container);
        container_id
    }

    /// Remove last child from collection
    pub fn pop(&mut self) -> Option<Child> {
        if let Some(id) = self.children.keys().next_back().cloned() {
            self.remove(id)
        } else {
            None
        }
    }

    /// Remove child by id
    pub fn remove(&mut self, id: ChildId) -> Option<Child> {
        self.children.remove(&id).map(|leaf| leaf.data)
    }

    /// Children keys
    pub fn children_keys(&self) -> std::collections::btree_map::Keys<'_, ChildId, Leaf<Child>> {
        self.children.keys()
    }

    /// Get child by id
    pub fn get_child(&self, id: ChildId) -> Option<&Leaf<Child>> {
        self.children.get(&id)
    }

    /// Get mutable child by id
    pub fn get_child_mut(&mut self, id: ChildId) -> Option<&mut Leaf<Child>> {
        self.children.get_mut(&id)
    }

    fn add_filter_from_item(dest: &mut Option<Bloom>, item: &Option<Cow<'_, Bloom>>) {
        HierarchicalBloomInner::merge_filters(dest, item.as_ref().map(|x| x.as_ref()));
    }

    fn init_filter_from_cow(dest: &mut Option<Bloom>, item: &Option<Cow<'_, Bloom>>) {
        if let Some(filter) = item {
            *dest = Some(filter.clone().into_owned());
        } else {
            *dest = None;
        }
    }

    /// Returns a iterator over the childs
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, ChildId, Leaf<Child>> {
        self.children.iter()
    }

    /// Returns a iterator over the childs
    pub fn iter_mut(&mut self) -> std::collections::btree_map::IterMut<'_, ChildId, Leaf<Child>> {
        self.children.iter_mut()
    }

    /// Returns iterator over childs data
    pub fn iter_mut_data(&mut self) -> impl Iterator<Item = &mut Child> {
        self.children.iter_mut().map(|x| &mut x.1.data)
    }

    /// Returns iterator over childs data
    pub fn iter_data(&self) -> impl Iterator<Item = &Child> {
        self.children.iter().map(|x| &x.1.data)
    }

    /// Returns id of next child
    pub fn next_id(&self) -> u64 {
        self.children_id.0
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
    pub fn new(group_size: usize, level: usize) -> Self {
        let mut inner_id = InnerId::default();
        let root = inner_id.get_inc();
        let mut inner = BTreeMap::<_, _>::default();
        inner.insert(root, Default::default());
        Self {
            inner,
            children: Default::default(),
            inner_id,
            children_id: Default::default(),
            root,
            group_size,
            level,
        }
    }

    /// Count of childs in container
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Clear container
    pub fn clear(&mut self) {
        *self = Self::new(self.group_size, self.level);
    }

    /// Clear container and return children values as Vec
    pub fn clear_and_get_values(&mut self) -> Vec<Child> {
        let values = std::mem::replace(self, Self::new(self.group_size, self.level))
            .children
            .into_values()
            .map(|v| v.data)
            .collect();
        values
    }
}

impl<Child> IntoIterator for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    type Item = (ChildId, Leaf<Child>);

    type IntoIter = std::collections::btree_map::IntoIter<ChildId, Leaf<Child>>;

    fn into_iter(self) -> Self::IntoIter {
        self.children.into_iter()
    }
}

#[derive(Debug)]
/// PossibleIter
pub struct PossibleRevIter<'a, Child> {
    this: &'a HierarchicalBloom<Child>,
    stack: Vec<(usize, &'a HierarchicalBloomInner)>,
    key: &'a [u8],
    rev: bool,
}

impl<'a, Child> PossibleRevIter<'a, Child> {
    fn new(this: &'a HierarchicalBloom<Child>, key: &'a [u8], rev: bool) -> Self {
        Self {
            stack: vec![(0, this.inner.get(&this.root).expect("should exist"))],
            this,
            key,
            rev,
        }
    }
}

impl<'a, Child> Iterator for PossibleRevIter<'a, Child> {
    type Item = (ChildId, &'a Leaf<Child>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((index, HierarchicalBloomInner::Node(node))) = self.stack.last().copied() {
            let len = node.children.len();
            if index >= len {
                self.stack.pop();
            } else {
                self.stack.last_mut().map(|(id, _)| *id += 1);
                if let Some(inner) = self
                    .this
                    .inner
                    .get(&node.children[if self.rev { len - index - 1 } else { index }])
                {
                    match inner {
                        HierarchicalBloomInner::Node(node)
                            if node
                                .filter
                                .as_ref()
                                .map(|f| f.contains_in_memory(&self.key))
                                .flatten()
                                == Some(FilterResult::NotContains) => {}
                        HierarchicalBloomInner::Leaf(leaf)
                            if !self.this.children.contains_key(&leaf.leaf) => {}
                        _ => {
                            self.stack.push((0, inner));
                        }
                    }
                }
            }
        }
        if let Some((_, HierarchicalBloomInner::Leaf(leaf))) = self.stack.pop() {
            self.this.children.get(&leaf.leaf).map(|x| (leaf.leaf, x))
        } else {
            None
        }
    }
}
