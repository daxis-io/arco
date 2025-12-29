//! Schema projection from Iceberg to Arco catalog.
//!
//! This module converts Iceberg table schemas to Arco [`ColumnRecord`] format
//! for the schema registry projection.
//!
//! # Design Notes
//!
//! - Iceberg schema is authoritative for tables with `format = ICEBERG`
//! - Arco schema registry is a projection with bounded staleness
//! - Complex types (struct, list, map) are represented as type strings

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Iceberg primitive types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IcebergPrimitiveType {
    /// Boolean type.
    Boolean,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    Long,
    /// 32-bit IEEE 754 floating point.
    Float,
    /// 64-bit IEEE 754 floating point.
    Double,
    /// Calendar date without time.
    Date,
    /// Time of day without date.
    Time,
    /// Timestamp without timezone.
    Timestamp,
    /// Timestamp with timezone.
    Timestamptz,
    /// Arbitrary-length character sequences.
    String,
    /// Universally unique identifier.
    Uuid,
    /// Fixed-length byte array.
    #[serde(rename = "fixed")]
    Fixed(usize),
    /// Arbitrary-length byte array.
    Binary,
    /// Arbitrary-precision decimal.
    #[serde(rename = "decimal")]
    Decimal {
        /// Total number of digits.
        precision: u8,
        /// Number of fractional digits.
        scale: u8,
    },
}

/// Iceberg complex types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IcebergComplexType {
    /// Nested structure with named fields.
    Struct {
        /// The nested fields.
        fields: Vec<IcebergField>,
    },
    /// Ordered collection of elements.
    List {
        /// Field ID for element.
        #[serde(rename = "element-id")]
        element_id: i32,
        /// Element type.
        element: Box<IcebergType>,
        /// Whether elements are required.
        #[serde(rename = "element-required")]
        element_required: bool,
    },
    /// Key-value collection.
    Map {
        /// Field ID for key.
        #[serde(rename = "key-id")]
        key_id: i32,
        /// Key type.
        key: Box<IcebergType>,
        /// Field ID for value.
        #[serde(rename = "value-id")]
        value_id: i32,
        /// Value type.
        value: Box<IcebergType>,
        /// Whether values are required.
        #[serde(rename = "value-required")]
        value_required: bool,
    },
}

/// An Iceberg type (primitive or complex).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IcebergType {
    /// A primitive type.
    Primitive(IcebergPrimitiveType),
    /// A complex type.
    Complex(IcebergComplexType),
}

/// An Iceberg schema field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergField {
    /// Unique field ID within the schema.
    pub id: i32,
    /// Field name.
    pub name: String,
    /// Whether the field is required.
    pub required: bool,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: IcebergType,
    /// Optional documentation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// An Iceberg table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergSchema {
    /// Schema ID for evolution tracking.
    #[serde(rename = "schema-id", default)]
    pub schema_id: i32,
    /// Optional schema identifier type.
    #[serde(rename = "type", default)]
    pub schema_type: Option<String>,
    /// Top-level fields.
    pub fields: Vec<IcebergField>,
    /// Optional identifier fields (primary key).
    #[serde(rename = "identifier-field-ids", default)]
    pub identifier_field_ids: Vec<i32>,
}

// ============================================================================
// Type Mapping (Task 10)
// ============================================================================

/// Maps Iceberg types to Arco type strings.
///
/// This implements the type conversion for schema projection from
/// Iceberg's rich type system to Arco's simpler representation.
pub struct IcebergTypeMapper;

impl IcebergTypeMapper {
    /// Maps an Iceberg type to its Arco string representation.
    ///
    /// # Primitive Types
    /// - `boolean` -> `"boolean"`
    /// - `int` -> `"int32"`
    /// - `long` -> `"int64"`
    /// - `float` -> `"float32"`
    /// - `double` -> `"float64"`
    /// - `date` -> `"date"`
    /// - `time` -> `"time"`
    /// - `timestamp` -> `"timestamp"`
    /// - `timestamptz` -> `"timestamptz"`
    /// - `string` -> `"string"`
    /// - `uuid` -> `"uuid"`
    /// - `fixed[n]` -> `"fixed[n]"`
    /// - `binary` -> `"binary"`
    /// - `decimal(p,s)` -> `"decimal(p,s)"`
    ///
    /// # Complex Types
    /// - `struct` -> `"struct<field1:type1, field2:type2, ...>"`
    /// - `list` -> `"list<element_type>"`
    /// - `map` -> `"map<key_type, value_type>"`
    #[must_use]
    pub fn map_type(iceberg_type: &IcebergType) -> String {
        match iceberg_type {
            IcebergType::Primitive(p) => Self::map_primitive(p),
            IcebergType::Complex(c) => Self::map_complex(c),
        }
    }

    /// Maps an Iceberg primitive type to its Arco string representation.
    #[must_use]
    pub fn map_primitive(ptype: &IcebergPrimitiveType) -> String {
        match ptype {
            IcebergPrimitiveType::Boolean => "boolean".to_string(),
            IcebergPrimitiveType::Int => "int32".to_string(),
            IcebergPrimitiveType::Long => "int64".to_string(),
            IcebergPrimitiveType::Float => "float32".to_string(),
            IcebergPrimitiveType::Double => "float64".to_string(),
            IcebergPrimitiveType::Date => "date".to_string(),
            IcebergPrimitiveType::Time => "time".to_string(),
            IcebergPrimitiveType::Timestamp => "timestamp".to_string(),
            IcebergPrimitiveType::Timestamptz => "timestamptz".to_string(),
            IcebergPrimitiveType::String => "string".to_string(),
            IcebergPrimitiveType::Uuid => "uuid".to_string(),
            IcebergPrimitiveType::Fixed(n) => format!("fixed[{n}]"),
            IcebergPrimitiveType::Binary => "binary".to_string(),
            IcebergPrimitiveType::Decimal { precision, scale } => {
                format!("decimal({precision},{scale})")
            }
        }
    }

    /// Maps an Iceberg complex type to its Arco string representation.
    #[must_use]
    pub fn map_complex(ctype: &IcebergComplexType) -> String {
        match ctype {
            IcebergComplexType::Struct { fields } => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|f| format!("{}:{}", f.name, Self::map_type(&f.field_type)))
                    .collect();
                format!("struct<{}>", field_strs.join(", "))
            }
            IcebergComplexType::List { element, .. } => {
                format!("list<{}>", Self::map_type(element))
            }
            IcebergComplexType::Map { key, value, .. } => {
                format!("map<{}, {}>", Self::map_type(key), Self::map_type(value))
            }
        }
    }
}

// ============================================================================
// Column Record (Task 11 preparation)
// ============================================================================

/// A column record for the Arco schema registry.
///
/// This is the projected representation of an Iceberg field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRecord {
    /// Stable column ID derived from `table_uuid` + `field_id`.
    pub column_id: Uuid,
    /// Table this column belongs to.
    pub table_uuid: Uuid,
    /// Original Iceberg field ID.
    pub field_id: i32,
    /// Column name.
    pub name: String,
    /// Arco type string.
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Optional description from Iceberg doc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether this column is part of the identifier (primary key).
    pub is_identifier: bool,
    /// Schema version this column belongs to.
    pub schema_id: i32,
}

impl ColumnRecord {
    /// Generates a stable column ID from table UUID and field ID.
    ///
    /// Uses a deterministic hash of table UUID + field ID to ensure
    /// stability across projections.
    #[must_use]
    pub fn generate_column_id(table_uuid: Uuid, field_id: i32) -> Uuid {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(table_uuid.as_bytes());
        hasher.update(field_id.to_le_bytes());
        let hash = hasher.finalize();

        // Use first 16 bytes of hash as UUID (version 4 variant)
        // SHA-256 always produces 32 bytes, so this slice is safe
        let mut bytes = [0u8; 16];
        #[allow(clippy::indexing_slicing)]
        bytes.copy_from_slice(&hash[..16]);

        // Set version (4) and variant (RFC 4122) bits
        bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
        bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant RFC 4122

        Uuid::from_bytes(bytes)
    }
}

// ============================================================================
// Schema Projector (Task 11)
// ============================================================================

/// Projects an Iceberg schema to Arco [`ColumnRecord`]s.
pub struct SchemaProjector;

impl SchemaProjector {
    /// Projects an Iceberg schema to a list of [`ColumnRecord`]s.
    ///
    /// # Arguments
    ///
    /// * `table_uuid` - The table this schema belongs to
    /// * `schema` - The Iceberg schema to project
    ///
    /// # Returns
    ///
    /// A list of [`ColumnRecord`]s representing the projected schema.
    #[must_use]
    pub fn project(table_uuid: Uuid, schema: &IcebergSchema) -> Vec<ColumnRecord> {
        let identifier_set: std::collections::HashSet<i32> =
            schema.identifier_field_ids.iter().copied().collect();

        schema
            .fields
            .iter()
            .map(|field| ColumnRecord {
                column_id: ColumnRecord::generate_column_id(table_uuid, field.id),
                table_uuid,
                field_id: field.id,
                name: field.name.clone(),
                data_type: IcebergTypeMapper::map_type(&field.field_type),
                nullable: !field.required,
                description: field.doc.clone(),
                is_identifier: identifier_set.contains(&field.id),
                schema_id: schema.schema_id,
            })
            .collect()
    }

    /// Projects nested fields from a struct type.
    ///
    /// This flattens nested structures using dot notation for names.
    /// Example: `address.city` for a nested field.
    #[must_use]
    pub fn project_nested(
        table_uuid: Uuid,
        schema: &IcebergSchema,
        prefix: &str,
    ) -> Vec<ColumnRecord> {
        let identifier_set: std::collections::HashSet<i32> =
            schema.identifier_field_ids.iter().copied().collect();

        Self::project_fields(
            table_uuid,
            &schema.fields,
            prefix,
            schema.schema_id,
            &identifier_set,
        )
    }

    /// Recursively projects fields with optional prefix.
    fn project_fields(
        table_uuid: Uuid,
        fields: &[IcebergField],
        prefix: &str,
        schema_id: i32,
        identifier_set: &std::collections::HashSet<i32>,
    ) -> Vec<ColumnRecord> {
        let mut records = Vec::new();

        for field in fields {
            let name = if prefix.is_empty() {
                field.name.clone()
            } else {
                format!("{}.{}", prefix, field.name)
            };

            records.push(ColumnRecord {
                column_id: ColumnRecord::generate_column_id(table_uuid, field.id),
                table_uuid,
                field_id: field.id,
                name,
                data_type: IcebergTypeMapper::map_type(&field.field_type),
                nullable: !field.required,
                description: field.doc.clone(),
                is_identifier: identifier_set.contains(&field.id),
                schema_id,
            });

            // Recursively project nested struct fields
            if let IcebergType::Complex(IcebergComplexType::Struct {
                fields: nested_fields,
            }) = &field.field_type
            {
                let nested_prefix = if prefix.is_empty() {
                    field.name.clone()
                } else {
                    format!("{}.{}", prefix, field.name)
                };
                records.extend(Self::project_fields(
                    table_uuid,
                    nested_fields,
                    &nested_prefix,
                    schema_id,
                    identifier_set,
                ));
            }
        }

        records
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Task 10: Type Mapping Tests
    // ========================================================================

    #[test]
    fn test_map_boolean() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Boolean);
        assert_eq!(IcebergTypeMapper::map_type(&t), "boolean");
    }

    #[test]
    fn test_map_int() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Int);
        assert_eq!(IcebergTypeMapper::map_type(&t), "int32");
    }

    #[test]
    fn test_map_long() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Long);
        assert_eq!(IcebergTypeMapper::map_type(&t), "int64");
    }

    #[test]
    fn test_map_float() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Float);
        assert_eq!(IcebergTypeMapper::map_type(&t), "float32");
    }

    #[test]
    fn test_map_double() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Double);
        assert_eq!(IcebergTypeMapper::map_type(&t), "float64");
    }

    #[test]
    fn test_map_date() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Date);
        assert_eq!(IcebergTypeMapper::map_type(&t), "date");
    }

    #[test]
    fn test_map_time() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Time);
        assert_eq!(IcebergTypeMapper::map_type(&t), "time");
    }

    #[test]
    fn test_map_timestamp() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Timestamp);
        assert_eq!(IcebergTypeMapper::map_type(&t), "timestamp");
    }

    #[test]
    fn test_map_timestamptz() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Timestamptz);
        assert_eq!(IcebergTypeMapper::map_type(&t), "timestamptz");
    }

    #[test]
    fn test_map_string() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::String);
        assert_eq!(IcebergTypeMapper::map_type(&t), "string");
    }

    #[test]
    fn test_map_uuid() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Uuid);
        assert_eq!(IcebergTypeMapper::map_type(&t), "uuid");
    }

    #[test]
    fn test_map_fixed() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Fixed(16));
        assert_eq!(IcebergTypeMapper::map_type(&t), "fixed[16]");
    }

    #[test]
    fn test_map_binary() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Binary);
        assert_eq!(IcebergTypeMapper::map_type(&t), "binary");
    }

    #[test]
    fn test_map_decimal() {
        let t = IcebergType::Primitive(IcebergPrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        assert_eq!(IcebergTypeMapper::map_type(&t), "decimal(10,2)");
    }

    #[test]
    fn test_map_struct() {
        let t = IcebergType::Complex(IcebergComplexType::Struct {
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "x".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(IcebergPrimitiveType::Int),
                    doc: None,
                },
                IcebergField {
                    id: 2,
                    name: "y".to_string(),
                    required: false,
                    field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                    doc: None,
                },
            ],
        });
        assert_eq!(IcebergTypeMapper::map_type(&t), "struct<x:int32, y:string>");
    }

    #[test]
    fn test_map_list() {
        let t = IcebergType::Complex(IcebergComplexType::List {
            element_id: 1,
            element: Box::new(IcebergType::Primitive(IcebergPrimitiveType::String)),
            element_required: true,
        });
        assert_eq!(IcebergTypeMapper::map_type(&t), "list<string>");
    }

    #[test]
    fn test_map_map() {
        let t = IcebergType::Complex(IcebergComplexType::Map {
            key_id: 1,
            key: Box::new(IcebergType::Primitive(IcebergPrimitiveType::String)),
            value_id: 2,
            value: Box::new(IcebergType::Primitive(IcebergPrimitiveType::Long)),
            value_required: false,
        });
        assert_eq!(IcebergTypeMapper::map_type(&t), "map<string, int64>");
    }

    #[test]
    fn test_map_nested_complex() {
        // list<struct<name:string, age:int32>>
        let t = IcebergType::Complex(IcebergComplexType::List {
            element_id: 1,
            element: Box::new(IcebergType::Complex(IcebergComplexType::Struct {
                fields: vec![
                    IcebergField {
                        id: 2,
                        name: "name".to_string(),
                        required: true,
                        field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                        doc: None,
                    },
                    IcebergField {
                        id: 3,
                        name: "age".to_string(),
                        required: false,
                        field_type: IcebergType::Primitive(IcebergPrimitiveType::Int),
                        doc: None,
                    },
                ],
            })),
            element_required: true,
        });
        assert_eq!(
            IcebergTypeMapper::map_type(&t),
            "list<struct<name:string, age:int32>>"
        );
    }

    // ========================================================================
    // Task 11: Schema Projection Tests
    // ========================================================================

    #[test]
    fn test_column_id_generation_is_stable() {
        let table_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let field_id = 42;

        let id1 = ColumnRecord::generate_column_id(table_uuid, field_id);
        let id2 = ColumnRecord::generate_column_id(table_uuid, field_id);

        assert_eq!(id1, id2, "Column ID should be deterministic");
    }

    #[test]
    fn test_column_id_differs_by_field() {
        let table_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let id1 = ColumnRecord::generate_column_id(table_uuid, 1);
        let id2 = ColumnRecord::generate_column_id(table_uuid, 2);

        assert_ne!(
            id1, id2,
            "Different field IDs should produce different column IDs"
        );
    }

    #[test]
    fn test_column_id_differs_by_table() {
        let table1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let table2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

        let id1 = ColumnRecord::generate_column_id(table1, 1);
        let id2 = ColumnRecord::generate_column_id(table2, 1);

        assert_ne!(
            id1, id2,
            "Different tables should produce different column IDs"
        );
    }

    #[test]
    fn test_project_simple_schema() {
        let table_uuid = Uuid::new_v4();
        let schema = IcebergSchema {
            schema_id: 1,
            schema_type: Some("struct".to_string()),
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(IcebergPrimitiveType::Long),
                    doc: Some("Primary key".to_string()),
                },
                IcebergField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        };

        let columns = SchemaProjector::project(table_uuid, &schema);

        assert_eq!(columns.len(), 2);

        // Check first column
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, "int64");
        assert!(!columns[0].nullable);
        assert!(columns[0].is_identifier);
        assert_eq!(columns[0].description, Some("Primary key".to_string()));

        // Check second column
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].data_type, "string");
        assert!(columns[1].nullable);
        assert!(!columns[1].is_identifier);
    }

    #[test]
    fn test_project_with_complex_types() {
        let table_uuid = Uuid::new_v4();
        let schema = IcebergSchema {
            schema_id: 2,
            schema_type: None,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "tags".to_string(),
                    required: false,
                    field_type: IcebergType::Complex(IcebergComplexType::List {
                        element_id: 2,
                        element: Box::new(IcebergType::Primitive(IcebergPrimitiveType::String)),
                        element_required: true,
                    }),
                    doc: None,
                },
                IcebergField {
                    id: 3,
                    name: "metadata".to_string(),
                    required: false,
                    field_type: IcebergType::Complex(IcebergComplexType::Map {
                        key_id: 4,
                        key: Box::new(IcebergType::Primitive(IcebergPrimitiveType::String)),
                        value_id: 5,
                        value: Box::new(IcebergType::Primitive(IcebergPrimitiveType::String)),
                        value_required: false,
                    }),
                    doc: None,
                },
            ],
            identifier_field_ids: vec![],
        };

        let columns = SchemaProjector::project(table_uuid, &schema);

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].data_type, "list<string>");
        assert_eq!(columns[1].data_type, "map<string, string>");
    }

    #[test]
    fn test_project_nested_flattens_struct() {
        let table_uuid = Uuid::new_v4();
        let schema = IcebergSchema {
            schema_id: 3,
            schema_type: None,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(IcebergPrimitiveType::Long),
                    doc: None,
                },
                IcebergField {
                    id: 2,
                    name: "address".to_string(),
                    required: false,
                    field_type: IcebergType::Complex(IcebergComplexType::Struct {
                        fields: vec![
                            IcebergField {
                                id: 3,
                                name: "street".to_string(),
                                required: true,
                                field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                                doc: None,
                            },
                            IcebergField {
                                id: 4,
                                name: "city".to_string(),
                                required: true,
                                field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                                doc: None,
                            },
                        ],
                    }),
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        };

        let columns = SchemaProjector::project_nested(table_uuid, &schema, "");

        // Should have 4 columns: id, address (struct type), address.street, address.city
        assert_eq!(columns.len(), 4);

        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "address");
        assert_eq!(columns[1].data_type, "struct<street:string, city:string>");
        assert_eq!(columns[2].name, "address.street");
        assert_eq!(columns[3].name, "address.city");
    }

    #[test]
    fn test_iceberg_schema_deserialize() {
        let json = r#"{
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                },
                {
                    "id": 2,
                    "name": "data",
                    "required": false,
                    "type": "string",
                    "doc": "Data field"
                }
            ],
            "identifier-field-ids": [1]
        }"#;

        let schema: IcebergSchema = serde_json::from_str(json).expect("deserialize");
        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.identifier_field_ids, vec![1]);
    }

    #[test]
    fn test_decimal_type_deserialize() {
        // Iceberg uses "decimal(p, s)" string format in JSON
        // Note: Full JSON deserialization would need custom deserializer
        // For now, test the programmatic construction
        let t = IcebergPrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert_eq!(IcebergTypeMapper::map_primitive(&t), "decimal(10,2)");
    }

    #[test]
    fn test_column_record_serialization() {
        let table_uuid = Uuid::new_v4();
        let record = ColumnRecord {
            column_id: ColumnRecord::generate_column_id(table_uuid, 1),
            table_uuid,
            field_id: 1,
            name: "id".to_string(),
            data_type: "int64".to_string(),
            nullable: false,
            description: Some("Primary key".to_string()),
            is_identifier: true,
            schema_id: 0,
        };

        let json = serde_json::to_string(&record).expect("serialize");
        let parsed: ColumnRecord = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(record.column_id, parsed.column_id);
        assert_eq!(record.name, parsed.name);
        assert_eq!(record.is_identifier, parsed.is_identifier);
    }
}
