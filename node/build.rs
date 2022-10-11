use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../ionian-rust");

    let status = Command::new("cargo")
        .current_dir("../ionian-rust")
        .args(vec!["build", "--release"])
        .status()
        .unwrap();

    println!("build ionian-rust with status {}", status);
}
