use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn main() -> std::result::Result<(), std::io::Error> {
    /*
     * for `PROFILE` and `RUSTC` options, see the following:
     * https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
     */
    let profile = env::var("PROFILE").expect("PROFILE must exist");
    let rustc = env::var("RUSTC").expect("RUSTC must exist");
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .expect("RUSTC --version should succeed")
        .stdout;
    let version =
        String::from_utf8(version).expect("RUSTC --version should output a readable text");
    let version = version.trim();
    let dest_path = Path::new("src").join("build_informations.rs");
    let mut file = File::create(&dest_path).expect("src/build_informations.rs should not exist");

    file.write_all(
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
    )
}
