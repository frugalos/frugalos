#[macro_use]
extern crate trackable;

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

macro_rules! track_any_err_unwrap {
    ($expr:expr) => {
        track_try_unwrap!(track_any_err!($expr))
    };
}

fn main() {
    /*
     * for `PROFILE`, `RUSTC`, and `OUT_DIR` options, see the following:
     * https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
     */
    let profile = track_any_err_unwrap!(env::var("PROFILE"));
    let rustc = track_any_err_unwrap!(env::var("RUSTC"));
    let version = track_any_err_unwrap!(Command::new(rustc).arg("--version").output()).stdout;
    let version = track_any_err_unwrap!(String::from_utf8(version));
    let version = version.trim();
    let out_dir = track_any_err_unwrap!(env::var("OUT_DIR"));
    let dest_path = Path::new(&out_dir).join("build_information.rs");
    let mut file = track_any_err_unwrap!(File::create(&dest_path));

    track_any_err_unwrap!(file.write_all(
        format!(
            "
/// This takes one of \"debug\" or \"release\", that we've used to build this frugalos.
pub static BUILD_PROFILE: &'static str = \"{}\";\n\n

/// This means the rustc version that we've used to build this frugalos.
pub static BUILD_VERSION: &'static str = \"{}\";
",
            profile, version
        )
        .as_bytes(),
    ));
}
