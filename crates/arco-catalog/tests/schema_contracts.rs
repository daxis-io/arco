//! Parquet schema contract tests per Task 3.6.
//!
//! These tests ensure that Parquet schemas remain backward compatible.
//! Golden schema files are checked into the repository and serve as the
//! contract that must not be broken.
//!
//! # Schema Evolution Rules (per ADR-006)
//!
//! 1. **Additive-only**: New fields must be nullable
//! 2. **No field removal**: Deprecated fields remain in schema
//! 3. **No type changes**: Field types are immutable
//! 4. **Nullable -> non-nullable**: Breaking change (not allowed)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use arrow::datatypes::{DataType, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Serializable field definition for golden schema files.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct GoldenField {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Serializable schema definition for golden files.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoldenSchema {
    name: String,
    version: u32,
    fields: Vec<GoldenField>,
}

/// Convert Arrow `DataType` to a string for comparison.
fn data_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Utf8 => "Utf8".to_string(),
        DataType::Boolean => "Boolean".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => "Timestamp(Millisecond)".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => {
            format!("Timestamp(Millisecond, {tz})")
        }
        other => format!("{other:?}"),
    }
}

/// Convert Arrow `Schema` to `GoldenSchema`.
fn schema_to_golden(name: &str, schema: &Schema) -> GoldenSchema {
    GoldenSchema {
        name: name.to_string(),
        version: 1,
        fields: schema
            .fields()
            .iter()
            .map(|f| GoldenField {
                name: f.name().clone(),
                data_type: data_type_to_string(f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect(),
    }
}

/// Load golden schema from checked-in JSON file.
fn load_golden_schema(name: &str) -> GoldenSchema {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Path::new(manifest_dir)
        .join("tests")
        .join("golden_schemas")
        .join(format!("{name}.schema.json"));

    let json = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "Golden schema not found: {}. Error: {}. \
             Generate with: cargo test --test schema_contracts -- --ignored generate_golden_schemas",
            path.display(),
            e
        )
    });

    serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("Invalid golden schema JSON at {}: {}", path.display(), e))
}

/// Check if new schema is backward compatible with golden.
///
/// Rules:
/// 1. All golden fields must exist in current
/// 2. Field types must match exactly
/// 3. Nullable -> non-nullable is breaking
/// 4. New fields must be nullable
fn is_backward_compatible(golden: &GoldenSchema, current: &GoldenSchema) -> Result<(), String> {
    let current_fields: HashMap<_, _> = current
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    // Rule 1, 2, 3: All golden fields must exist with same types
    for golden_field in &golden.fields {
        match current_fields.get(golden_field.name.as_str()) {
            Some(current_field) => {
                // Rule 2: Field types must match
                if golden_field.data_type != current_field.data_type {
                    return Err(format!(
                        "Field '{}' type changed: {} -> {}",
                        golden_field.name, golden_field.data_type, current_field.data_type
                    ));
                }
                // Rule 3: Nullable -> non-nullable is breaking
                if golden_field.nullable && !current_field.nullable {
                    return Err(format!(
                        "Field '{}' changed from nullable to non-nullable (breaking)",
                        golden_field.name
                    ));
                }
            }
            None => {
                return Err(format!(
                    "Field '{}' was removed from schema (breaking)",
                    golden_field.name
                ));
            }
        }
    }

    let golden_field_names: std::collections::HashSet<_> =
        golden.fields.iter().map(|f| f.name.as_str()).collect();

    // Rule 4: New fields must be nullable
    for current_field in &current.fields {
        if !golden_field_names.contains(current_field.name.as_str()) && !current_field.nullable {
            return Err(format!(
                "New field '{}' must be nullable for backward compatibility",
                current_field.name
            ));
        }
    }

    Ok(())
}

/// Get current namespaces schema from code.
fn current_namespaces_schema() -> GoldenSchema {
    schema_to_golden(
        "namespaces",
        &arco_catalog::parquet_util::namespace_schema(),
    )
}

/// Get current catalogs schema from code.
fn current_catalogs_schema() -> GoldenSchema {
    schema_to_golden("catalogs", &arco_catalog::parquet_util::catalog_schema())
}

/// Get current tables schema from code.
fn current_tables_schema() -> GoldenSchema {
    schema_to_golden("tables", &arco_catalog::parquet_util::table_schema())
}

/// Get current columns schema from code.
fn current_columns_schema() -> GoldenSchema {
    schema_to_golden("columns", &arco_catalog::parquet_util::column_schema())
}

/// Get current `lineage_edges` schema from code.
fn current_lineage_edges_schema() -> GoldenSchema {
    schema_to_golden(
        "lineage_edges",
        &arco_catalog::parquet_util::lineage_edge_schema(),
    )
}

// ============================================================================
// Golden Schema Generator (run with --ignored)
// ============================================================================

#[test]
#[ignore]
fn generate_golden_schemas() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let golden_dir = Path::new(manifest_dir).join("tests").join("golden_schemas");

    let schemas = [
        ("catalogs", current_catalogs_schema()),
        ("namespaces", current_namespaces_schema()),
        ("tables", current_tables_schema()),
        ("columns", current_columns_schema()),
        ("lineage_edges", current_lineage_edges_schema()),
    ];

    for (name, schema) in schemas {
        let path = golden_dir.join(format!("{name}.schema.json"));
        let json = serde_json::to_string_pretty(&schema).expect("serialize schema");
        fs::write(&path, json).expect("write golden file");
    }
}

// ============================================================================
// Schema Backward Compatibility Tests
// ============================================================================

#[test]
fn contract_namespaces_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("namespaces");
    let current = current_namespaces_schema();

    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!(
            "Namespaces schema is NOT backward compatible with golden:\n{msg}\n\
             See docs/adr/adr-006-schema-evolution.md for migration guidance."
        );
    }
}

#[test]
fn contract_catalogs_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("catalogs");
    let current = current_catalogs_schema();

    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!(
            "Catalogs schema is NOT backward compatible with golden:\n{msg}\n\
             See docs/adr/adr-006-schema-evolution.md for migration guidance."
        );
    }
}

#[test]
fn contract_tables_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("tables");
    let current = current_tables_schema();

    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!(
            "Tables schema is NOT backward compatible with golden:\n{msg}\n\
             See docs/adr/adr-006-schema-evolution.md for migration guidance."
        );
    }
}

#[test]
fn contract_columns_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("columns");
    let current = current_columns_schema();

    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!(
            "Columns schema is NOT backward compatible with golden:\n{msg}\n\
             See docs/adr/adr-006-schema-evolution.md for migration guidance."
        );
    }
}

#[test]
fn contract_lineage_edges_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("lineage_edges");
    let current = current_lineage_edges_schema();

    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!(
            "Lineage edges schema is NOT backward compatible with golden:\n{msg}\n\
             See docs/adr/adr-006-schema-evolution.md for migration guidance."
        );
    }
}

// ============================================================================
// Schema Structure Tests (Golden files must be valid)
// ============================================================================

#[test]
fn golden_schemas_are_valid_and_parseable() {
    // This test fails fast if any golden file is missing or malformed
    let schemas = [
        "catalogs",
        "namespaces",
        "tables",
        "columns",
        "lineage_edges",
    ];

    for name in schemas {
        let schema = load_golden_schema(name);
        assert!(
            !schema.fields.is_empty(),
            "Golden schema '{name}' must have at least one field"
        );
    }
}

#[test]
fn current_schemas_match_golden_field_count() {
    // This is a sanity check - field count should match or current has more (new nullable fields)
    let test_cases = [
        ("catalogs", current_catalogs_schema()),
        ("namespaces", current_namespaces_schema()),
        ("tables", current_tables_schema()),
        ("columns", current_columns_schema()),
        ("lineage_edges", current_lineage_edges_schema()),
    ];

    for (name, current) in test_cases {
        let golden = load_golden_schema(name);
        let current_len = current.fields.len();
        let golden_len = golden.fields.len();
        assert!(
            current_len >= golden_len,
            "Schema '{name}' has fewer fields than golden: {current_len} < {golden_len}"
        );
    }
}

// ============================================================================
// Required Fields Tests
// ============================================================================

#[test]
fn contract_catalogs_required_fields() {
    let schema = arco_catalog::parquet_util::catalog_schema();
    let required_fields = ["id", "name", "created_at", "updated_at"];

    for field_name in required_fields {
        assert!(
            schema.field_with_name(field_name).is_ok(),
            "Catalogs schema missing required field: {field_name}"
        );
    }

    // id and name should be non-nullable
    let id_field = schema.field_with_name("id").unwrap();
    assert!(
        !id_field.is_nullable(),
        "Catalog 'id' field should not be nullable"
    );

    let name_field = schema.field_with_name("name").unwrap();
    assert!(
        !name_field.is_nullable(),
        "Catalog 'name' field should not be nullable"
    );
}

#[test]
fn contract_namespaces_required_fields() {
    let schema = arco_catalog::parquet_util::namespace_schema();
    let required_fields = ["id", "catalog_id", "name", "created_at", "updated_at"];

    for field_name in required_fields {
        assert!(
            schema.field_with_name(field_name).is_ok(),
            "Namespaces schema missing required field: {field_name}"
        );
    }

    // id and name should be non-nullable
    let id_field = schema.field_with_name("id").unwrap();
    assert!(
        !id_field.is_nullable(),
        "Namespace 'id' field should not be nullable"
    );

    // catalog_id is nullable for backward compatibility with older snapshots.
    let catalog_id_field = schema.field_with_name("catalog_id").unwrap();
    assert!(
        catalog_id_field.is_nullable(),
        "Namespace 'catalog_id' field should be nullable"
    );

    let name_field = schema.field_with_name("name").unwrap();
    assert!(
        !name_field.is_nullable(),
        "Namespace 'name' field should not be nullable"
    );
}

#[test]
fn contract_tables_required_fields() {
    let schema = arco_catalog::parquet_util::table_schema();
    let required_fields = ["id", "namespace_id", "name", "created_at", "updated_at"];

    for field_name in required_fields {
        assert!(
            schema.field_with_name(field_name).is_ok(),
            "Tables schema missing required field: {field_name}"
        );
    }
}

#[test]
fn contract_columns_required_fields() {
    let schema = arco_catalog::parquet_util::column_schema();
    let required_fields = [
        "id",
        "table_id",
        "name",
        "data_type",
        "is_nullable",
        "ordinal",
    ];

    for field_name in required_fields {
        assert!(
            schema.field_with_name(field_name).is_ok(),
            "Columns schema missing required field: {field_name}"
        );
    }
}

#[test]
fn contract_lineage_edges_required_fields() {
    let schema = arco_catalog::parquet_util::lineage_edge_schema();
    let required_fields = ["id", "source_id", "target_id", "edge_type", "created_at"];

    for field_name in required_fields {
        assert!(
            schema.field_with_name(field_name).is_ok(),
            "Lineage edges schema missing required field: {field_name}"
        );
    }
}

// ============================================================================
// Schema Field Type Tests
// ============================================================================

#[test]
fn contract_timestamp_fields_use_milliseconds() {
    // All timestamp fields should use Int64 (milliseconds since epoch) for portability
    // across different query engines (DuckDB, Datafusion, etc.)
    let test_cases = [
        ("catalogs", arco_catalog::parquet_util::catalog_schema()),
        ("namespaces", arco_catalog::parquet_util::namespace_schema()),
        ("tables", arco_catalog::parquet_util::table_schema()),
        (
            "lineage_edges",
            arco_catalog::parquet_util::lineage_edge_schema(),
        ),
    ];

    for (name, schema) in test_cases {
        for field in schema.fields() {
            if field.name().contains("_at") || field.name().contains("timestamp") {
                let field_name = field.name();
                match field.data_type() {
                    // Int64 is used to store milliseconds since epoch for portability
                    DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {}
                    other => panic!(
                        "Schema '{name}' field '{field_name}' should use Int64 or Timestamp(Millisecond), got {other:?}"
                    ),
                }
            }
        }
    }
}

#[test]
fn contract_id_fields_are_strings() {
    // All ID fields should be strings (UUIDs/ULIDs stored as text)
    let test_cases = [
        ("catalogs", arco_catalog::parquet_util::catalog_schema()),
        ("namespaces", arco_catalog::parquet_util::namespace_schema()),
        ("tables", arco_catalog::parquet_util::table_schema()),
        ("columns", arco_catalog::parquet_util::column_schema()),
        (
            "lineage_edges",
            arco_catalog::parquet_util::lineage_edge_schema(),
        ),
    ];

    for (name, schema) in test_cases {
        for field in schema.fields() {
            if field.name().ends_with("_id") || field.name() == "id" {
                let field_name = field.name();
                assert_eq!(
                    field.data_type(),
                    &DataType::Utf8,
                    "Schema '{name}' field '{field_name}' should be Utf8 (string)"
                );
            }
        }
    }
}

// ============================================================================
// Roundtrip Tests (Write -> Read preserves schema)
// ============================================================================

#[test]
fn contract_namespaces_roundtrip_preserves_schema() {
    use arco_catalog::parquet_util::{NamespaceRecord, read_namespaces, write_namespaces};

    let records = vec![
        NamespaceRecord {
            id: "ns_001".into(),
            catalog_id: None,
            name: "default".into(),
            description: Some("Default namespace".into()),
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_000,
        },
        NamespaceRecord {
            id: "ns_002".into(),
            catalog_id: Some("cat_001".into()),
            name: "analytics".into(),
            description: None,
            created_at: 1_700_000_001_000,
            updated_at: 1_700_000_001_000,
        },
    ];

    let result = write_namespaces(&records).expect("write should succeed");
    let read_back = read_namespaces(&result).expect("read should succeed");

    assert_eq!(read_back.len(), 2);
    let first = read_back.first().expect("first namespace record");
    let second = read_back.get(1).expect("second namespace record");
    assert_eq!(first.id, "ns_001");
    assert_eq!(first.catalog_id, None);
    assert_eq!(first.name, "default");
    assert_eq!(first.description, Some("Default namespace".into()));
    assert_eq!(second.id, "ns_002");
    assert_eq!(second.catalog_id, Some("cat_001".into()));
    assert_eq!(second.description, None);
}

#[test]
fn contract_catalogs_roundtrip_preserves_schema() {
    use arco_catalog::parquet_util::{CatalogRecord, read_catalogs, write_catalogs};

    let records = vec![
        CatalogRecord {
            id: "cat_001".into(),
            name: "default".into(),
            description: Some("Default catalog".into()),
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_000,
        },
        CatalogRecord {
            id: "cat_002".into(),
            name: "analytics".into(),
            description: None,
            created_at: 1_700_000_001_000,
            updated_at: 1_700_000_001_000,
        },
    ];

    let result = write_catalogs(&records).expect("write should succeed");
    let read_back = read_catalogs(&result).expect("read should succeed");

    assert_eq!(read_back.len(), 2);
    let first = read_back.first().expect("first catalog record");
    let second = read_back.get(1).expect("second catalog record");
    assert_eq!(first.id, "cat_001");
    assert_eq!(first.name, "default");
    assert_eq!(first.description, Some("Default catalog".into()));
    assert_eq!(second.id, "cat_002");
    assert_eq!(second.description, None);
}

#[test]
fn contract_tables_roundtrip_preserves_schema() {
    use arco_catalog::parquet_util::{TableRecord, read_tables, write_tables};

    let records = vec![TableRecord {
        id: "tbl_001".into(),
        namespace_id: "ns_001".into(),
        name: "users".into(),
        description: Some("User table".into()),
        location: Some("s3://bucket/users".into()),
        format: Some("parquet".into()),
        created_at: 1_700_000_000_000,
        updated_at: 1_700_000_000_000,
    }];

    let result = write_tables(&records).expect("write should succeed");
    let read_back = read_tables(&result).expect("read should succeed");

    assert_eq!(read_back.len(), 1);
    let first = read_back.first().expect("table record");
    assert_eq!(first.id, "tbl_001");
    assert_eq!(first.location, Some("s3://bucket/users".into()));
}

#[test]
fn contract_columns_roundtrip_preserves_schema() {
    use arco_catalog::parquet_util::{ColumnRecord, read_columns, write_columns};

    let records = vec![
        ColumnRecord {
            id: "col_001".into(),
            table_id: "tbl_001".into(),
            name: "user_id".into(),
            data_type: "INT64".into(),
            is_nullable: false,
            ordinal: 0,
            description: Some("Primary key".into()),
        },
        ColumnRecord {
            id: "col_002".into(),
            table_id: "tbl_001".into(),
            name: "email".into(),
            data_type: "STRING".into(),
            is_nullable: true,
            ordinal: 1,
            description: None,
        },
    ];

    let result = write_columns(&records).expect("write should succeed");
    let read_back = read_columns(&result).expect("read should succeed");

    assert_eq!(read_back.len(), 2);
    let first = read_back.first().expect("first column record");
    let second = read_back.get(1).expect("second column record");
    assert_eq!(first.id, "col_001");
    assert_eq!(first.name, "user_id");
    assert_eq!(first.description, Some("Primary key".into()));
    assert_eq!(second.id, "col_002");
    assert!(second.is_nullable);
    assert_eq!(second.description, None);
}

#[test]
fn contract_lineage_edges_roundtrip_preserves_schema() {
    use arco_catalog::parquet_util::{LineageEdgeRecord, read_lineage_edges, write_lineage_edges};

    let records = vec![LineageEdgeRecord {
        id: "edge_001".into(),
        source_id: "tbl_a".into(),
        target_id: "tbl_b".into(),
        edge_type: "derives_from".into(),
        run_id: Some("run_123".into()),
        created_at: 1_700_000_000_000,
    }];

    let result = write_lineage_edges(&records).expect("write should succeed");
    let read_back = read_lineage_edges(&result).expect("read should succeed");

    assert_eq!(read_back.len(), 1);
    let first = read_back.first().expect("lineage edge record");
    assert_eq!(first.id, "edge_001");
    assert_eq!(first.run_id, Some("run_123".into()));
}

#[test]
fn contract_namespaces_parquet_write_is_deterministic() {
    use arco_catalog::parquet_util::{NamespaceRecord, write_namespaces};

    let records = vec![
        NamespaceRecord {
            id: "ns_001".into(),
            catalog_id: None,
            name: "default".into(),
            description: Some("Default namespace".into()),
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_000,
        },
        NamespaceRecord {
            id: "ns_002".into(),
            catalog_id: Some("cat_001".into()),
            name: "analytics".into(),
            description: None,
            created_at: 1_700_000_001_000,
            updated_at: 1_700_000_001_000,
        },
    ];

    let bytes_one = write_namespaces(&records).expect("write one");
    let bytes_two = write_namespaces(&records).expect("write two");

    assert_eq!(bytes_one, bytes_two);
}

#[test]
fn contract_catalogs_parquet_write_is_deterministic() {
    use arco_catalog::parquet_util::{CatalogRecord, write_catalogs};

    let records = vec![
        CatalogRecord {
            id: "cat_001".into(),
            name: "default".into(),
            description: Some("Default catalog".into()),
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_000,
        },
        CatalogRecord {
            id: "cat_002".into(),
            name: "analytics".into(),
            description: None,
            created_at: 1_700_000_001_000,
            updated_at: 1_700_000_001_000,
        },
    ];

    let bytes_one = write_catalogs(&records).expect("write one");
    let bytes_two = write_catalogs(&records).expect("write two");

    assert_eq!(bytes_one, bytes_two);
}

#[test]
fn contract_tables_parquet_write_is_deterministic() {
    use arco_catalog::parquet_util::{TableRecord, write_tables};

    let records = vec![TableRecord {
        id: "tbl_001".into(),
        namespace_id: "ns_001".into(),
        name: "users".into(),
        description: Some("User table".into()),
        location: Some("s3://bucket/users".into()),
        format: Some("parquet".into()),
        created_at: 1_700_000_000_000,
        updated_at: 1_700_000_000_000,
    }];

    let bytes_one = write_tables(&records).expect("write one");
    let bytes_two = write_tables(&records).expect("write two");

    assert_eq!(bytes_one, bytes_two);
}

#[test]
fn contract_columns_parquet_write_is_deterministic() {
    use arco_catalog::parquet_util::{ColumnRecord, write_columns};

    let records = vec![
        ColumnRecord {
            id: "col_001".into(),
            table_id: "tbl_001".into(),
            name: "user_id".into(),
            data_type: "INT64".into(),
            is_nullable: false,
            ordinal: 0,
            description: Some("Primary key".into()),
        },
        ColumnRecord {
            id: "col_002".into(),
            table_id: "tbl_001".into(),
            name: "email".into(),
            data_type: "STRING".into(),
            is_nullable: true,
            ordinal: 1,
            description: None,
        },
    ];

    let bytes_one = write_columns(&records).expect("write one");
    let bytes_two = write_columns(&records).expect("write two");

    assert_eq!(bytes_one, bytes_two);
}

#[test]
fn contract_lineage_edges_parquet_write_is_deterministic() {
    use arco_catalog::parquet_util::{LineageEdgeRecord, write_lineage_edges};

    let records = vec![LineageEdgeRecord {
        id: "edge_001".into(),
        source_id: "tbl_a".into(),
        target_id: "tbl_b".into(),
        edge_type: "derives_from".into(),
        run_id: Some("run_123".into()),
        created_at: 1_700_000_000_000,
    }];

    let bytes_one = write_lineage_edges(&records).expect("write one");
    let bytes_two = write_lineage_edges(&records).expect("write two");

    assert_eq!(bytes_one, bytes_two);
}
