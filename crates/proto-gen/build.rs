//! Compiles every contract in `/proto` into Rust with protox (no `protoc`
//! needed) and generates gRPC clients and servers with tonic.

use std::path::PathBuf;

const FILES: &[&str] = &[
    "events/v1/events.proto",
    "accounts/v1/accounts.proto",
    "risk/v1/risk.proto",
    "matching/v1/matching.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../proto");
    println!("cargo:rerun-if-changed={}", root.display());
    for f in FILES {
        println!("cargo:rerun-if-changed={}", root.join(f).display());
    }

    let fds = protox::compile(FILES, [&root])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(fds)?;
    Ok(())
}
