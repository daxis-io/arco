//! Contract tests for canonical table-format handling.

use arco_core::table_format::TableFormat;

#[test]
fn parses_supported_formats_case_insensitively() {
    assert_eq!(TableFormat::parse("DELTA").unwrap(), TableFormat::Delta);
    assert_eq!(TableFormat::parse("Iceberg").unwrap(), TableFormat::Iceberg);
    assert_eq!(TableFormat::parse("parquet").unwrap(), TableFormat::Parquet);
}

#[test]
fn canonical_strings_are_lowercase() {
    assert_eq!(TableFormat::Delta.as_str(), "delta");
    assert_eq!(TableFormat::Iceberg.as_str(), "iceberg");
    assert_eq!(TableFormat::Parquet.as_str(), "parquet");
    assert_eq!(TableFormat::normalize("DeLtA").unwrap(), "delta");
}

#[test]
fn effective_format_uses_legacy_parquet_fallback() {
    assert_eq!(TableFormat::effective(None).unwrap(), TableFormat::Parquet);
    assert_eq!(
        TableFormat::effective(Some("ICEBERG")).unwrap(),
        TableFormat::Iceberg
    );
}

#[test]
fn parse_rejects_unknown_values() {
    assert!(TableFormat::parse("orc").is_err());
    assert!(TableFormat::normalize("avro").is_err());
}
