fn main() {
    // Tell Cargo where to find the library.
    // We assume the library is in the ../main directory relative to this crate.
    println!("cargo:rustc-link-search=native=../main");

    // Tell Cargo to link the "jsondb" library.
    println!("cargo:rustc-link-lib=dylib=jsondb");

    // Tell Cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=../main/libjsondb.h");
}
