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
}
