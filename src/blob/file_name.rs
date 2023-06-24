use super::prelude::*;

#[derive(Debug, Clone)]
pub struct FileName {
    id: usize,
    path: Box<Path>
}

impl FileName {
    pub fn new(name_prefix: &str, id: usize, extension: &str, dir: &Path) -> Self {
        Self {
            id,
            path: dir.join(format!("{}.{}.{}", name_prefix, id, extension)).into_boxed_path()
        }
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        Self::try_from_path(path).ok_or_else(|| Error::file_pattern(path.to_owned()).into())
    }

    fn try_from_path(path: &Path) -> Option<Self> {
        // Extension should be non-empty
        if path.extension()?.is_empty() {
            return None;
        }
        let stem = path.file_stem()?;
        let mut parts = stem.to_str()?.splitn(2, '.');
        let _name_prefix = parts.next()?;
        let id: usize = parts.next()?.parse().ok()?;
        Some(Self {
            id,
            path: path.into()
        })
    }

    pub fn as_path(&self) -> &Path {
        self.path.as_ref()
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn with_extension(&self, new_ext: &str) -> FileName {
        Self { 
            id: self.id, 
            path: self.path.with_extension(new_ext).into_boxed_path() 
        }
    }
}

impl Display for FileName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.path.display(), f)
    }
}
