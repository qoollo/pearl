use std::fmt::{Display, Formatter, Result as FmtResult};

include!(concat!(env!("OUT_DIR"), "/build_time.rs"));

/// Contains info about current build:
/// name, version, commit and build time (if built with `--release` flag).
/// To get current info, use `build_info` helper.
#[derive(Debug, Clone)]
pub struct BuildInfo {
    name: &'static str,
    version: &'static str,
    commit: &'static str,
    build_time: &'static str,
}

impl BuildInfo {
    /// Creates struct with basic build information.
    pub fn new() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            commit: option_env!("PEARL_COMMIT_HASH").unwrap_or("hash-undefined"),
            build_time: BUILD_TIME,
        }
    }

    /// Get a reference to the build info's name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get a reference to the build info's version.
    pub fn version(&self) -> &'static str {
        self.version
    }

    /// Get a reference to the build info's commit.
    pub fn commit(&self) -> &'static str {
        self.commit
    }

    /// Get a reference to the build info's build time.
    pub fn build_time(&self) -> &'static str {
        self.build_time
    }
}

impl Display for BuildInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(
            f,
            "{} {} (commit: {}, built on: {})",
            self.name, self.version, self.commit, self.build_time
        )
    }
}

#[test]
fn print_build_info() {
    println!("{}", BuildInfo::new());
}
