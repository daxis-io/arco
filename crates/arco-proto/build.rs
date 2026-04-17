//! Build script for compiling protobuf definitions and protobuf-JSON serde support.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/arco/v1/common.proto",
        "../../proto/arco/v1/request.proto",
        "../../proto/arco/v1/event.proto",
        "../../proto/arco/v1/catalog.proto",
        "../../proto/arco/v1/flow.proto",
        "../../proto/arco/v1/orchestration.proto",
        "../../proto/arco/v1/transactions.proto",
    ];

    let includes = ["../../proto"];
    let descriptor_path = PathBuf::from(env::var("OUT_DIR")?).join("arco_descriptor.bin");

    tonic_prost_build::configure()
        .codec_path("crate::ProstCodec")
        // Use BTreeMap for deterministic ordering in serde
        .btree_map(".")
        // Keep protobuf JSON mapping for Timestamp without forcing all google types through
        // pbjson-types, which conflicts with prost-build 0.14's Eq/Hash auto-derives.
        .extern_path(".google.protobuf.Timestamp", "crate::Timestamp")
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&proto_files, &includes)?;

    let descriptor_set = std::fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .extern_path(".google.protobuf.Timestamp", "crate::Timestamp")
        .btree_map(["."])
        .register_descriptors(&descriptor_set)?
        .build(&[".arco.v1"])?;

    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-changed=../../proto/buf/validate/validate.proto");

    Ok(())
}
