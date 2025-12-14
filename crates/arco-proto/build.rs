//! Build script for compiling protobuf definitions.

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

    prost_build::Config::new()
        // Use BTreeMap for deterministic ordering in serde
        .btree_map(["."])
        // Add serde derives only to types that don't contain Timestamp
        // ID types
        .type_attribute(".arco.v1.TenantId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.TenantId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.WorkspaceId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.WorkspaceId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.AssetId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.AssetId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.RunId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.RunId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.TaskId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.TaskId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.PartitionId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.PartitionId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.MaterializationId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.MaterializationId", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.SnapshotId", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.SnapshotId", "#[serde(rename_all = \"camelCase\")]")
        // Core data types
        .type_attribute(".arco.v1.PartitionKey", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.PartitionKey", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.ScalarValue", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.ScalarValue", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.AssetKey", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.AssetKey", "#[serde(rename_all = \"camelCase\")]")
        // FileEntry for output tracking
        .type_attribute(".arco.v1.FileEntry", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.FileEntry", "#[serde(rename_all = \"camelCase\")]")
        // Orchestration types without Timestamp
        .type_attribute(".arco.v1.ResourceRequirements", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.ResourceRequirements", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.DependencyEdge", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.DependencyEdge", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.TaskMetrics", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.TaskMetrics", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute(".arco.v1.TaskError", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".arco.v1.TaskError", "#[serde(rename_all = \"camelCase\")]")
        .compile_protos(&proto_files, &includes)?;

    // Rerun if proto files change
    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }

    Ok(())
}
