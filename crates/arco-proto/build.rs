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
    ];

    let includes = ["../../proto"];
    let descriptor_path = PathBuf::from(env::var("OUT_DIR")?).join("arco_descriptor.bin");

    prost_build::Config::new()
        // Use BTreeMap for deterministic ordering in generated map fields.
        .btree_map(["."])
        // Override protobuf well-known types with serde-capable pbjson-types.
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&proto_files, &includes)?;

    let descriptor_set = std::fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .extern_path(".google.protobuf", "::pbjson_types")
        .btree_map(["."])
        .register_descriptors(&descriptor_set)?
        .build(&[".arco.v1"])?;

    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-changed=../../proto/buf/validate/validate.proto");

    Ok(())
}
