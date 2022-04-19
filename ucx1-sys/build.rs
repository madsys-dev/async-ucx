use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Tell cargo to tell rustc to link the library.
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=ucp");
    // println!("cargo:rustc-link-lib=uct");
    // println!("cargo:rustc-link-lib=ucs");
    // println!("cargo:rustc-link-lib=ucm");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    build_from_source();

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        .clang_arg(format!("-I{}", dst.join("include").display()))
        // The input header we would like to generate bindings for.
        .header("wrapper.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // .parse_callbacks(Box::new(ignored_macros))
        .allowlist_function("uc[tsmp]_.*")
        .allowlist_var("uc[tsmp]_.*")
        .allowlist_var("UC[TSMP]_.*")
        .allowlist_type("uc[tsmp]_.*")
        .rustified_enum(".*")
        .bitfield_enum("ucp_feature")
        .bitfield_enum(".*_field")
        .bitfield_enum(".*_flags(_t)?")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn build_from_source() {
    let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Return if the outputs exist.
    if dst.join("lib/libuct.a").exists()
        && dst.join("lib/libucs.a").exists()
        && dst.join("lib/libucm.a").exists()
        && dst.join("lib/libucp.a").exists()
    {
        return;
    }

    // Initialize git submodule if necessary.
    if !Path::new("ucx/.git").exists() {
        let _ = Command::new("git")
            .args(&["submodule", "update", "--init"])
            .status();
    }

    // Create build directory.
    let _ = std::fs::create_dir(&dst.join("build"));

    // Copy source.
    if !dst.join("ucx").exists() {
        let _ = Command::new("cp")
            .arg("-r")
            .arg("ucx")
            .arg(dst.join("ucx"))
            .status();
    }

    // autogen.sh
    Command::new("bash")
        .current_dir(&dst.join("ucx"))
        .arg("./autogen.sh")
        .status()
        .expect("failed to run autogen.sh");

    // configure
    Command::new("bash")
        .current_dir(&dst.join("build"))
        .arg(&dst.join("ucx/contrib/configure-release"))
        .arg(&format!("--prefix={}", dst.display()))
        .status()
        .expect("failed to configure");

    // make
    Command::new("make")
        .current_dir(&dst.join("build"))
        .arg(&format!("-j{}", env::var("NUM_JOBS").unwrap()))
        .status()
        .expect("failed to make");

    // make install
    Command::new("make")
        .current_dir(&dst.join("build"))
        .arg("install")
        .status()
        .expect("failed to make install");
}
