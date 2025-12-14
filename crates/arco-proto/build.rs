//! Build script for compiling protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/arco/v1/common.proto",
        "../../proto/arco/v1/request.proto",
        "../../proto/arco/v1/event.proto",
        "../../proto/arco/v1/catalog.proto",
        "../../proto/arco/v1/flow.proto",
    ];

    let includes = ["../../proto"];

    // Use basic prost compilation without serde for now
    // (prost_types::Timestamp doesn't support serde out of the box)
    prost_build::Config::new().compile_protos(&proto_files, &includes)?;

    // Rerun if proto files change
    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }

    Ok(())
}
