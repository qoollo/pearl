use std::iter::FromIterator;

use super::*;

/// Container for types which is support bloom filtering
#[derive(Debug)]
pub struct HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    inner: HierarchicalBloomInner,
    children: Vec<Child>,
    group_size: usize,
}

#[derive(Debug)]
enum HierarchicalBloomInner {
    /// Inner filters
    Vec(Option<Bloom>, Vec<HierarchicalBloomInner>),
    /// Leaf
    Item(usize),
}

impl Default for HierarchicalBloomInner {
    fn default() -> Self {
        Self::Vec(Default::default(), Default::default())
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

impl HierarchicalBloomInner {
    #[async_recursion::async_recursion]
    async fn check_filter<Child, Key>(&self, children: &[Child], item: &Key) -> Result<Option<bool>>
    where
        Child: BloomProvider<Key = Key> + Send + Sync,
        Key: Send + Sync + AsRef<[u8]> + ?Sized,
    {
        match self {
            HierarchicalBloomInner::Vec(filter, values) => {
                if let Some(filter) = filter {
                    if let Ok(true) = filter.contains_in_memory(item) {
                        return Ok(Some(true));
                    }
                }
                let mut falses = false;
                for filter in values {
                    match filter.check_filter(children, item).await {
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
            HierarchicalBloomInner::Item(child) => {
                children.get(*child).unwrap().check_filter(item).await
            }
        }
    }

    #[async_recursion::async_recursion]
    async fn offload_buffer<Child, Key>(
        &mut self,
        children: &mut [Child],
        needed_memory: usize,
    ) -> usize
    where
        Child: BloomProvider<Key = Key> + Send + Sync,
        Key: Send + Sync + AsRef<[u8]> + ?Sized,
    {
        match self {
            Self::Vec(_, values) => {
                let mut freed = 0;
                for filter in values {
                    if freed >= needed_memory {
                        return freed;
                    }
                    freed += filter.offload_buffer(children, needed_memory).await;
                }
                freed
            }
            Self::Item(child) => {
                children
                    .get_mut(*child)
                    .unwrap()
                    .offload_buffer(needed_memory)
                    .await
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
                    *dest = None;
                }
            }
            None => *dest = None,
        }
    }

    fn push_group(&mut self, value: Self) {
        match self {
            Self::Vec(filter, values) => {
                match &value {
                    Self::Vec(child_filter, _) => {
                        Self::merge_filters(filter, child_filter);
                    }
                    _ => unreachable!(),
                }
                values.push(value);
            }
            _ => unreachable!(),
        }
    }

    fn push_item(&mut self, id: usize, child_filter: &Option<Bloom>) {
        match self {
            Self::Vec(filter, values) => {
                values.push(Self::Item(id));
                Self::merge_filters(filter, child_filter)
            }
            _ => unreachable!(),
        }
    }

    fn push(&mut self, id: usize, filter: &Option<Bloom>, group_size: usize) {
        match self {
            Self::Vec(_, values) => {
                if values.is_empty() {
                    values.push(Default::default())
                }
                let last = values.last_mut().expect("Have values");
                last.push_item(id, filter);
                if last.len() >= group_size {
                    drop(last);
                    let last = values.pop().expect("Have values");
                    if values.is_empty() {
                        values.push(Default::default())
                    }
                    let pred_last = values.last_mut().expect("Have values");
                    pred_last.push_group(last);
                    values.push(Default::default());
                }
            }
            _ => unreachable!(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Vec(_, values) => values.len(),
            _ => 0,
        }
    }

    fn add_to_intermediate_filters(&mut self, id: usize, item: &[u8]) -> Result<bool> {
        match self {
            Self::Vec(filter, values) => {
                if let Some(filter) = filter {
                    let mut should_add = false;
                    for value in values.iter_mut() {
                        should_add = should_add || value.add_to_intermediate_filters(id, item)?;
                        if should_add {
                            filter.add(item)?;
                        }
                    }
                    Ok(should_add)
                } else {
                    Ok(false)
                }
            }
            Self::Item(self_id) => Ok(*self_id == id),
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

    async fn check_filter(&self, item: &Self::Key) -> Result<Option<bool>> {
        self.inner.check_filter(&self.children, item).await
    }

    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        self.inner
            .offload_buffer(&mut self.children, needed_memory)
            .await
    }

    async fn get_filter(&self) -> Option<Bloom> {
        match &self.inner {
            HierarchicalBloomInner::Vec(filter, _) => filter.clone(),
            _ => unreachable!(),
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

    /// Returns an iterator over the childs
    pub fn iter(&self) -> std::slice::Iter<'_, Child> {
        self.children.iter()
    }

    /// Returns a mutable iterator over the childs
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Child> {
        self.children.iter_mut()
    }

    /// Returns last child
    pub fn last(&self) -> Option<&Child> {
        self.children.last()
    }

    /// Add child to collection
    pub fn push(&mut self, item: Child) {
        let id = self.children.len();
        self.inner.push(id, &None, self.group_size);
        self.children.push(item);
    }

    /// Returns children elements as Vec
    pub fn into_vec(self) -> Vec<Child> {
        self.children
    }

    /// Returns mutable reference to inner container. Use carefully
    pub fn children_mut(&mut self) -> &mut Vec<Child> {
        &mut self.children
    }

    /// Returns reference to inner container.
    pub fn children(&self) -> &Vec<Child> {
        &self.children
    }

    /// Extends conatiner with values
    pub fn extend(&mut self, values: Vec<Child>) {
        for child in values {
            self.push(child);
        }
    }

    /// Adds key to intermediate filters for children with index `id`
    pub fn add_to_intermediate_filters(&mut self, id: usize, item: impl AsRef<[u8]>) {
        self.inner
            .add_to_intermediate_filters(id, item.as_ref())
            .expect("Intermediate filters can't offload inner buffer");
    }
}

impl<Child> From<Vec<Child>> for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn from(children: Vec<Child>) -> Self {
        let mut res = Self::default();
        for child in children {
            res.push(child);
        }
        res
    }
}

impl<Child> FromIterator<Child> for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    fn from_iter<T: IntoIterator<Item = Child>>(iter: T) -> Self {
        let mut res = Self::default();
        for child in iter {
            res.push(child);
        }
        res
    }
}

impl<Child> IntoIterator for HierarchicalBloom<Child>
where
    Child: BloomProvider + Send + Sync,
{
    type Item = Child;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.children.into_iter()
    }
}
