use chrono::Local;

fn main() {
    let time = Local::now();
    let content = format!(
        "pub(crate) const BUILD_TIME: &str = \"{}\";",
        time.format("%d-%m-%Y %H:%M:%S")
    );
    let path = format!("{}/build_time.rs", std::env::var("OUT_DIR").unwrap());
    if let Err(e) = std::fs::write(path, content) {
        println!("failed to write build time: {}", e);
    }
    println!("cargo:rerun-if-changed=build.rs");

    // aHash build script
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH was not set");
    if arch.eq_ignore_ascii_case("x86_64")
        || arch.eq_ignore_ascii_case("aarch64")
        || arch.eq_ignore_ascii_case("mips64")
        || arch.eq_ignore_ascii_case("powerpc64")
        || arch.eq_ignore_ascii_case("riscv64gc")
        || arch.eq_ignore_ascii_case("s390x")
    {
        println!("cargo:rustc-cfg=feature=\"folded_multiply\"");
    }
}
