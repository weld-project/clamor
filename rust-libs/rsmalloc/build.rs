use std::env;
use std::process::Command;

/// Write the build ID into an environment variable.
fn register_build_id() {
    // Set the build ID, which is either unknown or the git hash
    let unknown_build = String::from("unknown");
    let build_id = match Command::new("git")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .output() {
            Ok(output) => String::from_utf8(output.stdout).unwrap_or(unknown_build),
            Err(_) => unknown_build,
        };
    println!("cargo:rustc-env=BUILD_ID={}", build_id);
}

/// Link the C++ libraries.
fn link_clamor(clamor_dir: &str) {
    println!("cargo:rustc-link-lib=dylib=smalloc");
    println!("cargo:rustc-link-search=native={}/smalloc", clamor_dir);
}

fn main() {
    let ref project_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let ref clamor_dir = env::var("CLAMOR_HOME").unwrap();

    // Build ID
    register_build_id();

    // Dependencies for Clamor.
    link_clamor(clamor_dir);
}
