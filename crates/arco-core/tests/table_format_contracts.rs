//! Contract tests for canonical table-format handling.

#![allow(clippy::expect_used)]

use arco_core::table_format::TableFormat;

#[test]
fn parses_supported_formats_case_insensitively() {
    assert_eq!(
        TableFormat::parse("DELTA").expect("delta format"),
        TableFormat::Delta
    );
    assert_eq!(
        TableFormat::parse("Iceberg").expect("iceberg format"),
        TableFormat::Iceberg
    );
    assert_eq!(
        TableFormat::parse("parquet").expect("parquet format"),
        TableFormat::Parquet
    );
}

#[test]
fn canonical_strings_are_lowercase() {
    assert_eq!(TableFormat::Delta.as_str(), "delta");
    assert_eq!(TableFormat::Iceberg.as_str(), "iceberg");
    assert_eq!(TableFormat::Parquet.as_str(), "parquet");
    assert_eq!(
        TableFormat::normalize("DeLtA").expect("normalized delta format"),
        "delta"
    );
}

#[test]
fn effective_format_uses_legacy_parquet_fallback() {
    assert_eq!(
        TableFormat::effective(None).expect("legacy format fallback"),
        TableFormat::Parquet
    );
    assert_eq!(
        TableFormat::effective(Some("ICEBERG")).expect("effective iceberg format"),
        TableFormat::Iceberg
    );
}

#[test]
fn new_table_default_is_delta_without_rewriting_legacy_rows() {
    assert_eq!(TableFormat::default_for_new_tables(), TableFormat::Delta);
    assert_eq!(
        TableFormat::effective(None).expect("legacy format fallback"),
        TableFormat::Parquet
    );
}

#[test]
fn parse_rejects_unknown_values() {
    assert!(TableFormat::parse("orc").is_err());
    assert!(TableFormat::normalize("avro").is_err());
}
