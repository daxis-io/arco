#![allow(clippy::expect_used)]
#![allow(missing_docs)]

use arco_catalog::asset::{
    Asset as ModuleAsset, AssetFormat as ModuleAssetFormat, AssetKey as ModuleAssetKey,
};
use arco_catalog::prelude::{
    Asset as PreludeAsset, AssetFormat as PreludeAssetFormat, AssetKey as PreludeAssetKey,
    CreateAssetRequest as PreludeCreateAssetRequest,
};
use arco_catalog::{Asset, AssetFormat, AssetKey, CreateAssetRequest, Namespace, Schema};

#[test]
fn asset_module_and_root_reexports_remain_public() {
    let key = AssetKey::new("raw", "events").expect("valid asset key");
    let asset = Asset::new(key.clone(), "s3://bucket/raw/events", AssetFormat::Parquet);
    let request = CreateAssetRequest::simple(key, "s3://bucket/raw/events");
    let module_asset = ModuleAsset::new(
        ModuleAssetKey::new("raw", "events").expect("valid module asset key"),
        "s3://bucket/raw/events",
        ModuleAssetFormat::Parquet,
    );

    assert_eq!(asset.format, AssetFormat::Parquet);
    assert_eq!(request.format, AssetFormat::Parquet);
    assert_eq!(module_asset.format, ModuleAssetFormat::Parquet);
}

#[test]
fn prelude_asset_exports_and_namespace_alias_remain_public() {
    let key = PreludeAssetKey::new("raw", "events").expect("valid prelude asset key");
    let asset = PreludeAsset::new(
        key.clone(),
        "s3://bucket/raw/events",
        PreludeAssetFormat::Parquet,
    );
    let request = PreludeCreateAssetRequest::simple(key, "s3://bucket/raw/events");
    let namespace = Namespace {
        id: "schema-01".to_string(),
        catalog_id: Some("catalog-01".to_string()),
        name: "sales".to_string(),
        description: Some("sales schema".to_string()),
        created_at: 1,
        updated_at: 2,
    };
    let schema: Schema = namespace.clone();

    assert_eq!(asset.format, PreludeAssetFormat::Parquet);
    assert_eq!(request.format, PreludeAssetFormat::Parquet);
    assert_eq!(schema.name, "sales");
}
