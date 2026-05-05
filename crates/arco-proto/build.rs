//! Build script for compiling protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/arco/common/v1/common.proto",
        "../../proto/arco/catalog/v1/catalog.proto",
        "../../proto/arco/catalog/v1/metastore.proto",
        "../../proto/arco/orchestration/v1/orchestration.proto",
        "../../proto/arco/controlplane/v1/transactions.proto",
    ];

    let mut config = tonic_prost_build::configure()
        .codec_path("crate::ProstCodec")
        .btree_map(".");

    for ty in [
        ".arco.common.v1.PartitionDimension",
        ".arco.common.v1.PartitionKey",
        ".arco.common.v1.ScalarValue",
        ".arco.common.v1.NullValue",
        ".arco.catalog.v1.Grant",
        ".arco.catalog.v1.StorageCredential",
        ".arco.catalog.v1.ExternalLocation",
        ".arco.catalog.v1.WorkspaceBinding",
        ".arco.catalog.v1.GovernanceAttachment",
        ".arco.catalog.v1.Volume",
        ".arco.catalog.v1.Function",
        ".arco.catalog.v1.RegisteredModel",
        ".arco.catalog.v1.ModelVersion",
    ] {
        config = config.type_attribute(ty, "#[derive(serde::Serialize, serde::Deserialize)]");
    }

    config = configure_metastore_serde(config);

    for ty in [
        ".arco.catalog.v1.Catalog",
        ".arco.catalog.v1.Schema",
        ".arco.catalog.v1.Table",
        ".arco.catalog.v1.TableFormat",
        ".arco.catalog.v1.ColumnDefinition",
        ".arco.catalog.v1.CreateCatalogOp",
        ".arco.catalog.v1.CreateSchemaOp",
        ".arco.catalog.v1.RegisterTableOp",
        ".arco.catalog.v1.UpdateTableOp",
        ".arco.catalog.v1.DropTableOp",
        ".arco.catalog.v1.RenameTableOp",
        ".arco.catalog.v1.CatalogDdlOperation",
    ] {
        config = config.type_attribute(ty, "#[derive(serde::Serialize)]");
    }

    for ty in [
        ".arco.common.v1.PartitionDimension",
        ".arco.common.v1.PartitionKey",
        ".arco.common.v1.ScalarValue",
        ".arco.catalog.v1.Grant",
        ".arco.catalog.v1.StorageCredential",
        ".arco.catalog.v1.ExternalLocation",
        ".arco.catalog.v1.WorkspaceBinding",
        ".arco.catalog.v1.GovernanceAttachment",
        ".arco.catalog.v1.Volume",
        ".arco.catalog.v1.Function",
        ".arco.catalog.v1.RegisteredModel",
        ".arco.catalog.v1.ModelVersion",
        ".arco.catalog.v1.Catalog",
        ".arco.catalog.v1.Schema",
        ".arco.catalog.v1.Table",
        ".arco.catalog.v1.ColumnDefinition",
        ".arco.catalog.v1.CreateCatalogOp",
        ".arco.catalog.v1.CreateSchemaOp",
        ".arco.catalog.v1.RegisterTableOp",
        ".arco.catalog.v1.UpdateTableOp",
        ".arco.catalog.v1.DropTableOp",
        ".arco.catalog.v1.RenameTableOp",
        ".arco.catalog.v1.CatalogDdlOperation",
    ] {
        config = config.type_attribute(ty, "#[serde(rename_all = \"camelCase\")]");
    }

    for field in [
        ".arco.catalog.v1.Catalog.created_at",
        ".arco.catalog.v1.Catalog.updated_at",
        ".arco.catalog.v1.Schema.created_at",
        ".arco.catalog.v1.Schema.updated_at",
        ".arco.catalog.v1.Table.created_at",
        ".arco.catalog.v1.Table.updated_at",
    ] {
        config = config.field_attribute(
            field,
            "#[serde(skip_serializing_if = \"Option::is_none\", serialize_with = \"crate::serde_helpers::serialize_optional_timestamp\")]",
        );
    }

    config = configure_metastore_serde_fields(config);

    config.compile_protos(&proto_files, &["../../proto"])?;

    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-changed=../../proto");

    Ok(())
}

fn configure_metastore_serde(config: tonic_prost_build::Builder) -> tonic_prost_build::Builder {
    config
        .message_attribute(
            ".arco.catalog.v1.GrantMutation",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(transparent)]",
        )
        .enum_attribute(
            ".arco.catalog.v1.GrantMutation.op",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
        .message_attribute(
            ".arco.catalog.v1.MetastoreMutation",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(transparent)]",
        )
        .enum_attribute(
            ".arco.catalog.v1.MetastoreMutation.op",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
}

fn configure_metastore_serde_fields(
    mut config: tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
    for field in [
        ".arco.catalog.v1.Grant.created_at",
        ".arco.catalog.v1.StorageCredential.created_at",
        ".arco.catalog.v1.StorageCredential.updated_at",
        ".arco.catalog.v1.ExternalLocation.created_at",
        ".arco.catalog.v1.ExternalLocation.updated_at",
        ".arco.catalog.v1.WorkspaceBinding.created_at",
        ".arco.catalog.v1.GovernanceAttachment.created_at",
        ".arco.catalog.v1.Volume.created_at",
        ".arco.catalog.v1.Volume.updated_at",
        ".arco.catalog.v1.Function.created_at",
        ".arco.catalog.v1.Function.updated_at",
        ".arco.catalog.v1.RegisteredModel.created_at",
        ".arco.catalog.v1.RegisteredModel.updated_at",
        ".arco.catalog.v1.ModelVersion.created_at",
        ".arco.catalog.v1.ModelVersion.updated_at",
    ] {
        config = config.field_attribute(
            field,
            "#[serde(default, skip_deserializing, skip_serializing_if = \"Option::is_none\", serialize_with = \"crate::serde_helpers::serialize_optional_timestamp\")]",
        );
    }

    config
}
