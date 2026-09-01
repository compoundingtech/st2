fn main() {
    let target = std::env::var("TARGET").expect("Cargo always supplies TARGET to build scripts");
    println!("cargo:rustc-env=ST2_WASIP2_TARGET={target}");
    println!("cargo:rerun-if-env-changed=ST2_EXECUTOR_BUILD_IDENTITY");
}
