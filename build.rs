fn main() {
    // When the `opus-encoding` feature is enabled, tell cargo where to find libopus
    if std::env::var("CARGO_FEATURE_OPUS_ENCODING").is_ok() {
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
        println!("cargo:rustc-link-search=native=/opt/homebrew/opt/opus/lib");
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-search=native=/usr/lib");
        println!("cargo:rustc-link-lib=opus");
    }
}
