//! Integration tests verifying public API exports compile correctly.
//!
//! This test ensures that all public types and functions exported by arco-iceberg
//! are accessible and have stable interfaces. It catches breaking changes to the
//! crate's public API surface at compile time.

use std::mem::size_of;

#[test]
fn test_crate_public_exports() {
    use arco_iceberg::{
        error::IcebergError,
        pointer::IcebergTablePointer,
        router::iceberg_router,
        schema_projection::SchemaProjector,
        state::{IcebergConfig, IcebergState},
        types::{
            ConfigResponse, ListNamespacesResponse, ListTablesResponse, LoadTableResponse,
            StorageCredential,
        },
    };

    let _ = size_of::<IcebergError>();
    let _ = size_of::<IcebergTablePointer>();
    let _ = size_of::<SchemaProjector>();
    let _ = size_of::<IcebergState>();
    let _ = size_of::<ConfigResponse>();
    let _ = size_of::<ListNamespacesResponse>();
    let _ = size_of::<ListTablesResponse>();
    let _ = size_of::<LoadTableResponse>();
    let _ = size_of::<StorageCredential>();
    let _ = IcebergConfig::default();
    let _router_fn: fn(IcebergState) -> axum::Router = iceberg_router;
}
