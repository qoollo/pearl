/// Pearl crate build time, currently undefined.
pub const BUILD_TIME: &str = "time-undefined";

/// Formatted build info.
/// To get specific info use `BUILD_TIME`, `version` and `commit`.
pub fn build_info() -> String {
    format!(
        "pearl {} (commit: {}, built on: {})",
        version(),
        commit(),
        BUILD_TIME
    )
}

/// Returns package version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Returns current commit hash.
pub fn commit() -> &'static str {
    option_env!("PEARL_COMMIT_HASH").unwrap_or("hash-undefined")
}

#[test]
fn print_build_info() {
    println!("{}", build_info());
}
