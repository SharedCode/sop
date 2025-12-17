use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let lib_dir = PathBuf::from(manifest_dir).join("lib");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();

    let lib_name = match (target_os.as_str(), target_arch.as_str()) {
        ("macos", "x86_64") => "jsondb_amd64darwin",
        ("macos", "aarch64") => "jsondb_arm64darwin",
        ("linux", "x86_64") => "jsondb_amd64linux",
        ("linux", "aarch64") => "jsondb_arm64linux",
        ("windows", "x86_64") => "jsondb_amd64windows",
        _ => panic!("Unsupported target: {}-{}", target_os, target_arch),
    };

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static={}", lib_name);

    // Link system libraries required by the Go runtime
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    } else if target_os == "linux" {
        println!("cargo:rustc-link-lib=pthread");
    }

    println!("cargo:rerun-if-changed=lib");
}
