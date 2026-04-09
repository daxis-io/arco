//! Build script for compiling protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/arco/common/v1/common.proto",
        "../../proto/arco/catalog/v1/catalog.proto",
        "../../proto/arco/orchestration/v1/orchestration.proto",
        "../../proto/arco/controlplane/v1/transactions.proto",
    ];

    tonic_prost_build::configure()
        .codec_path("crate::ProstCodec")
        .btree_map(".")
        .type_attribute(
            ".arco.common.v1.PartitionDimension",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.PartitionDimension",
            "#[serde(rename_all = \"camelCase\")]",
        )
        .type_attribute(
            ".arco.common.v1.PartitionKey",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.PartitionKey",
            "#[serde(rename_all = \"camelCase\")]",
        )
        .type_attribute(
            ".arco.common.v1.ScalarValue",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.ScalarValue",
            "#[serde(rename_all = \"camelCase\")]",
        )
        .type_attribute(
            ".arco.common.v1.NullValue",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.TableFormat",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.FileEntry",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".arco.common.v1.FileEntry",
            "#[serde(rename_all = \"camelCase\")]",
        )
        .compile_protos(&proto_files, &["../../proto"])?;

    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-changed=../../proto");

    Ok(())
}
