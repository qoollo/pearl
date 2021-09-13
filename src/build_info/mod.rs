mod build_time;

/// Formatted build info.
/// To get specific info use helpers `build_time`, `version` and `commit`.
pub fn build_info() -> String {
    format!(
        "pearl {} (commit: {}, built on: {})",
        version(),
        commit(),
        build_time()
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

/// Returns build time.
pub fn build_time() -> &'static str {
    build_time::BUILD_TIME
}

#[test]
fn print_build_info() {
    println!("{}", build_info());
}
