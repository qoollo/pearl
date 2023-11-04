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
    let os = std::env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS was not set");
    if os.eq_ignore_ascii_case("linux")
        || os.eq_ignore_ascii_case("android")
        || os.eq_ignore_ascii_case("windows")
        || os.eq_ignore_ascii_case("macos")
        || os.eq_ignore_ascii_case("ios")
        || os.eq_ignore_ascii_case("freebsd")
        || os.eq_ignore_ascii_case("openbsd")
        || os.eq_ignore_ascii_case("dragonfly")
        || os.eq_ignore_ascii_case("solaris")
        || os.eq_ignore_ascii_case("illumos")
        || os.eq_ignore_ascii_case("fuchsia")
        || os.eq_ignore_ascii_case("redox")
        || os.eq_ignore_ascii_case("cloudabi")
        || os.eq_ignore_ascii_case("haiku")
        || os.eq_ignore_ascii_case("vxworks")
        || os.eq_ignore_ascii_case("emscripten")
        || os.eq_ignore_ascii_case("wasi")
    {
        println!("cargo:rustc-cfg=feature=\"runtime-rng\"");
    }
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
