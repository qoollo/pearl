use std::{borrow::Cow, collections::BTreeSet};

use super::*;

type InnerId = usize;
/// Child id
pub type ChildId = usize;

/// Container for types which is support bloom filtering
pub struct HierarchicalFilters<Key, Filter, Child> {
    inner: Vec<Option<Inner<Key, Filter>>>,
    children: Vec<Option<Leaf<Child>>>,
    root: usize,
    group_size: usize,
    level: usize,
}

impl<K, F, T> Debug for HierarchicalFilters<K, F, T>
where
    F: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct DebugInner<'a, K, F, T>(&'a HierarchicalFilters<K, F, T>, InnerId);
        impl<'a, K, F: Debug, T> Debug for DebugInner<'a, K, F, T> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                match self.0.get_inner(self.1) {
                    Some(Inner::Node(node)) => f
                        .debug_struct("Node")
                        .field("filter", &node.filter)
                        .field(
                            "children",
                            &node
                                .children
                                .iter()
                                .map(|x| DebugInner(self.0, *x))
                                .collect::<Vec<_>>(),
                        )
                        .finish(),
                    Some(Inner::Leaf(leaf)) => {
                        f.debug_struct("Leaf").field("leaf", &leaf.leaf).finish()
                    }
                    None => f.debug_struct("Empty").finish(),
                }
            }
        }
        f.debug_struct("HierarchicalBloom")
            .field("inner", &DebugInner(self, self.root))
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
struct InnerNode<Key, Filter> {
    filter: Option<Filter>,
    children: Vec<InnerId>,
    parent: Option<InnerId>,
    _marker: PhantomData<Key>,
}

impl<Key, Filter> Default for InnerNode<Key, Filter> {
    fn default() -> Self {
        InnerNode {
            filter: None,
            children: vec![],
            parent: Default::default(),
            _marker: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct InnerLeaf {
    parent: InnerId,
    leaf: ChildId,
}

#[derive(Debug, Clone)]
enum Inner<Key, Filter> {
    Node(InnerNode<Key, Filter>),
    Leaf(InnerLeaf),
}

impl<Key, Filter> Default for Inner<Key, Filter> {
    fn default() -> Self {
        Self::Node(Default::default())
    }
}

impl<Key, Filter> Inner<Key, Filter> {
    fn parent_id(&self) -> Option<InnerId> {
        match &self {
            Self::Node(node) => node.parent.clone(),
            Self::Leaf(leaf) => Some(leaf.parent.clone()),
        }
    }
}

impl<Key, Filter> Inner<Key, Filter>
where
    Key: Sync + Send,
    Filter: FilterTrait<Key>,
{
    fn add_to_filter(&mut self, item: &Key) {
        match self {
            Self::Node(node) => {
                if let Some(filter) = &mut node.filter {
                    let _ = filter.add(item);
                }
            }
            _ => {}
        }
    }

    fn merge_filters(dest: &mut Option<Filter>, source: Option<&Filter>) {
        if !dest
            .as_mut()
            .zip(source)
            .map(|(dest, source)| dest.checked_add_assign(source))
            .unwrap_or(false)
        {
            *dest = None;
        }
    }
}

#[async_trait::async_trait]
impl<Key, Filter, Child> BloomProvider<Key> for HierarchicalFilters<Key, Filter, Child>
where
    Key: Sync + Send,
    Child: BloomProvider<Key>,
    Filter: FilterTrait<Key>,
{
    type Filter = Filter;
    async fn check_filter(&self, item: &Key) -> FilterResult {
        let res = self
            .iter_possible_childs(item)
            .map(|(_, leaf)| leaf.data.check_filter(item))
            .collect::<FuturesUnordered<_>>()
            .fold(FilterResult::NotContains, |acc, x| acc + x)
            .await;
        res
    }

    fn check_filter_fast(&self, item: &Key) -> FilterResult {
        if self.iter_possible_childs(item).next().is_some() {
            FilterResult::NeedAdditionalCheck
        } else {
            FilterResult::NotContains
        }
    }

    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        let mut freed = 0;
        let mut parents = BTreeSet::new();
        // Firstly try to free needed amount of memory by offloading childs
        for child in self.children.iter_mut().flatten() {
            if freed >= needed_memory {
                return freed;
            }
            // Collect parents of childs to go through them further
            // Skip if level is low
            if level >= self.level {
                parents.insert(child.parent);
            }
            freed += child
                .data
                .offload_buffer(needed_memory - freed, level)
                .await;
        }
        match level.cmp(&self.level) {
            // Level of current container is higher than we need to offload
            std::cmp::Ordering::Less => freed,
            // Offload filters from nodes starting from deeper ones
            _ => {
                while !parents.is_empty() {
                    let mut new_parents = BTreeSet::new();
                    for parent in parents.iter().copied() {
                        if freed >= needed_memory {
                            return freed;
                        }
                        match self.get_inner_mut(parent) {
                            Some(Inner::Node(node)) => {
                                freed += node
                                    .filter
                                    .as_mut()
                                    .map(|x| x.offload_filter())
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

    async fn get_filter(&self) -> Option<Filter> {
        match &self.get_inner(self.root) {
            Some(Inner::Node(node)) => node.filter.clone(),
            _ => None,
        }
    }

    async fn filter_memory_allocated(&self) -> usize {
        let mut allocated = 0;
        for value in self.inner.iter().flatten() {
            if let Inner::Node(InnerNode {
                filter: Some(filter),
                ..
            }) = value
            {
                allocated += filter.memory_allocated();
            }
        }
        for child in self.children.iter().flatten() {
            allocated += child.data.filter_memory_allocated().await;
        }
        allocated
    }

    fn get_filter_fast(&self) -> Option<&Filter> {
        match &self.get_inner(self.root) {
            Some(Inner::Node(node)) => node.filter.as_ref(),
            _ => None,
        }
    }
}

impl<Key, Filter, Child> HierarchicalFilters<Key, Filter, Child>
where
    Key: Sync + Send,
    Child: BloomProvider<Key, Filter = Filter>,
    Filter: FilterTrait<Key>,
{
    /// Create collection from vec
    pub async fn from_vec(group_size: usize, level: usize, childs: Vec<Child>) -> Self {
        let mut ret = Self::new(group_size, level);
        for child in childs {
            ret.push(child).await;
        }
        ret
    }

    async fn get_filter_from_child<'a>(item: &'a Child) -> Option<Cow<'a, Filter>>
    where
        Filter: 'a,
    {
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
        if self.children.len() < self.group_size {
            // Add child to root child list
            let res = self.add_child(self.root, child).await;
            if self.children.len() >= self.group_size {
                // Replace root with new node and push to it
                let new_root_id = self.inner.len();
                let filter = {
                    let root = self.root_mut();
                    root.parent = Some(new_root_id);
                    root.filter.clone()
                };
                let new_root = Inner::Node(InnerNode {
                    filter,
                    children: vec![self.root],
                    ..Default::default()
                });
                self.root = new_root_id;
                self.inner.push(Some(new_root));
            }
            res
        } else {
            // Push child to node
            let mut id = self.last_inner_node().unwrap();
            if self.get(id).children.len() >= self.group_size {
                // Create new node if needed
                id = self.new_inner_node();
            }
            self.add_child(id, child).await
        }
    }

    // Add child to node and adds child filter data to node and it's parents
    async fn add_child(&mut self, node: InnerId, child: Child) -> ChildId {
        let item_filter = Self::get_filter_from_child(&child).await;
        let inner_id = self.inner.len();
        let child_id = self.children.len();
        self.inner.push(Some(Inner::Leaf(InnerLeaf {
            parent: node,
            leaf: child_id,
        })));

        let mut parent = {
            let node = self.get_mut(node);
            if node.children.is_empty() {
                Self::init_filter_from_cow(&mut node.filter, &item_filter);
            } else {
                Self::add_filter_from_cow(&mut node.filter, &item_filter);
            }
            node.children.push(inner_id);
            node.parent
        };

        while let Some(id) = parent {
            let node = self.get_mut(id);
            Self::add_filter_from_cow(&mut node.filter, &item_filter);
            parent = node.parent;
        }

        drop(item_filter);
        let child = Leaf {
            data: child,
            parent: node,
        };
        self.children.push(Some(child));
        child_id
    }

    fn last_inner_node(&self) -> Option<InnerId> {
        self.root().children.last().copied()
    }

    /// Extends conatiner with values
    pub async fn extend(&mut self, values: Vec<Child>) {
        for child in values {
            self.push(child).await;
        }
    }
}

impl<Key, Filter, Child> HierarchicalFilters<Key, Filter, Child>
where
    Key: Sync + Send,
    Filter: FilterTrait<Key>,
{
    /// Iter over childs which may contain key
    pub fn iter_possible_childs<'a>(
        &'a self,
        key: &'a Key,
    ) -> PossibleRevIter<'a, Key, Filter, Child> {
        PossibleRevIter::new(self, key, false)
    }

    /// Iter over childs which may contain key in reverse order
    pub fn iter_possible_childs_rev<'a>(
        &'a self,
        key: &'a Key,
    ) -> PossibleRevIter<'a, Key, Filter, Child> {
        PossibleRevIter::new(self, key, true)
    }

    /// Add key to all parents in collection
    pub fn add_to_parents(&mut self, child_id: ChildId, item: &Key) {
        if let Some(child) = self.get_child(child_id) {
            let mut id = child.parent;
            loop {
                let curr = self.get_inner_mut(id);
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
    fn add_filter_from_cow(dest: &mut Option<Filter>, item: &Option<Cow<'_, Filter>>)
    where
        Filter: Clone,
    {
        Inner::merge_filters(dest, item.as_ref().map(|x| x.as_ref()));
    }
}

impl<Key, Filter, Child> HierarchicalFilters<Key, Filter, Child>
where
    Filter: Clone,
{
    fn init_filter_from_cow(dest: &mut Option<Filter>, item: &Option<Cow<'_, Filter>>) {
        if let Some(filter) = item {
            *dest = Some(filter.clone().into_owned());
        } else {
            *dest = None;
        }
    }
}

impl<Key, Filter, Child> HierarchicalFilters<Key, Filter, Child> {
    /// Returns children elements as Vec
    pub fn into_vec(self) -> Vec<Leaf<Child>> {
        self.children.into_iter().flatten().collect()
    }

    fn root(&self) -> &InnerNode<Key, Filter> {
        self.get(self.root)
    }

    fn root_mut(&mut self) -> &mut InnerNode<Key, Filter> {
        self.get_mut(self.root)
    }

    fn get_mut(&mut self, id: InnerId) -> &mut InnerNode<Key, Filter> {
        match self.get_inner_mut(id) {
            Some(Inner::Node(node)) => node,
            _ => panic!("node not found"),
        }
    }

    fn get(&self, id: InnerId) -> &InnerNode<Key, Filter> {
        match self.get_inner(id) {
            Some(Inner::Node(node)) => node,
            _ => panic!("node not found"),
        }
    }

    fn new_inner_node(&mut self) -> InnerId {
        let node = Inner::Node(InnerNode {
            parent: Some(self.root),
            ..Default::default()
        });
        let node_id = self.inner.len();
        self.root_mut().children.push(node_id);
        self.inner.push(Some(node));
        node_id
    }

    /// Remove last child from collection
    pub fn pop(&mut self) -> Option<Child> {
        let mut last = self.children.len().checked_sub(1)?;
        while let Some(None) = self.children.get(last) {
            last = last.checked_sub(1)?;
        }
        self.remove(last)
    }

    /// Remove child by id
    pub fn remove(&mut self, id: ChildId) -> Option<Child> {
        if let Some(child) = self.children.get_mut(id) {
            let child = std::mem::replace(child, None);
            child.map(|x| x.data)
        } else {
            None
        }
    }

    /// Get id of last child
    pub fn last_id(&self) -> Option<ChildId> {
        let mut last = self.children.len().checked_sub(1)?;
        while let Some(None) = self.children.get(last) {
            last = last.checked_sub(1)?;
        }
        Some(last)
    }

    /// Get last child
    pub fn last(&self) -> Option<&Child> {
        if let Some(last) = self.last_id() {
            self.get_child(last).map(|x| &x.data)
        } else {
            None
        }
    }

    /// Get child by id
    pub fn get_child(&self, id: ChildId) -> Option<&Leaf<Child>> {
        self.children.get(id).map(|x| x.as_ref()).flatten()
    }

    /// Get mutable child by id
    pub fn get_child_mut(&mut self, id: ChildId) -> Option<&mut Leaf<Child>> {
        self.children.get_mut(id).map(|x| x.as_mut()).flatten()
    }

    fn get_inner(&self, id: InnerId) -> Option<&Inner<Key, Filter>> {
        self.inner.get(id).map(|x| x.as_ref()).flatten()
    }

    fn get_inner_mut(&mut self, id: InnerId) -> Option<&mut Inner<Key, Filter>> {
        self.inner.get_mut(id).map(|x| x.as_mut()).flatten()
    }

    /// Returns a iterator over the childs
    pub fn iter(&self) -> impl Iterator<Item = &Child> {
        self.children
            .iter()
            .map(|x| x.as_ref())
            .flatten()
            .map(|x| &x.data)
    }

    /// Returns a iterator over the childs
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Child> {
        self.children
            .iter_mut()
            .map(|x| x.as_mut())
            .flatten()
            .map(|x| &mut x.data)
    }

    /// Create new container
    pub fn new(group_size: usize, level: usize) -> Self {
        Self {
            inner: vec![Some(Default::default())],
            children: Default::default(),
            root: 0,
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
            .into_iter()
            .flatten()
            .map(|v| v.data)
            .collect();
        values
    }
}

impl<Key, Filter, Child> IntoIterator for HierarchicalFilters<Key, Filter, Child>
where
    Key: Sync + Send,
    Child: BloomProvider<Key>,
{
    type Item = Leaf<Child>;

    type IntoIter = core::iter::Flatten<std::vec::IntoIter<Option<Leaf<Child>>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.children.into_iter().flatten()
    }
}

#[derive(Debug)]
/// PossibleIter
pub struct PossibleRevIter<'a, Key, Filter, Child> {
    this: &'a HierarchicalFilters<Key, Filter, Child>,
    stack: Vec<(usize, &'a Inner<Key, Filter>)>,
    key: &'a Key,
    rev: bool,
}

impl<'a, Key, Filter, Child> PossibleRevIter<'a, Key, Filter, Child>
where
    Key: Sync + Send,
    Filter: FilterTrait<Key>,
{
    fn new(this: &'a HierarchicalFilters<Key, Filter, Child>, key: &'a Key, rev: bool) -> Self {
        Self {
            stack: vec![(0, this.get_inner(this.root).expect("should exist"))],
            this,
            key,
            rev,
        }
    }
}

impl<'a, Key, Filter, Child> Iterator for PossibleRevIter<'a, Key, Filter, Child>
where
    Key: Sync + Send,
    Filter: FilterTrait<Key>,
{
    type Item = (ChildId, &'a Leaf<Child>);

    /// Postorder traversal implementation for HierarchicalFilters
    /// Ignores nodes which not contains key and returns leafs
    fn next(&mut self) -> Option<Self::Item> {
        // Iter to next leaf
        while let Some((index, Inner::Node(node))) = self.stack.last().copied() {
            let len = node.children.len();
            if index >= len {
                // Remove last node from stack if all it's childs was seen
                self.stack.pop();
            } else {
                // Get next child of node and push it to stack
                // Iterate in reverse order if `self.rev` flag is `true`
                self.stack.last_mut().map(|(id, _)| *id += 1);
                if let Some(inner) = self
                    .this
                    .get_inner(node.children[if self.rev { len - index - 1 } else { index }])
                {
                    match inner {
                        // Ignore nodes which is not contains key
                        Inner::Node(node)
                            if node.filter.as_ref().map(|f| f.contains_fast(&self.key))
                                == Some(FilterResult::NotContains) => {}
                        // Ignore if leaf was removed
                        Inner::Leaf(leaf) if self.this.get_child(leaf.leaf).is_none() => {}
                        _ => {
                            self.stack.push((0, inner));
                        }
                    }
                }
            }
        }
        if let Some((_, Inner::Leaf(leaf))) = self.stack.pop() {
            self.this.get_child(leaf.leaf).map(|x| (leaf.leaf, x))
        } else {
            None
        }
    }
}
