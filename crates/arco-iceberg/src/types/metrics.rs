//! Metrics reporting types for the Iceberg REST API.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request body for `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`.
///
/// Reports scan or commit metrics for observability. The server accepts the report
/// and returns 204 No Content. Arco currently stores no metrics (fire-and-forget).
///
/// Per the Iceberg spec, this is a union type where different report types have
/// different required fields. We keep `report_type` required for validation but
/// make other fields optional for forward compatibility with engine variations.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReportMetricsRequest {
    /// Report type: "scan-report" or "commit-report".
    #[serde(rename = "report-type")]
    pub report_type: String,

    /// Table name (optional, varies by report type).
    #[serde(
        rename = "table-name",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub table_name: Option<String>,

    /// Snapshot ID associated with this report (optional, varies by report type).
    #[serde(
        rename = "snapshot-id",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_id: Option<i64>,

    /// Additional fields vary by report type (schema-id, filter, metrics, etc.).
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_report_deserialization() {
        let json = r#"{
            "report-type": "scan-report",
            "table-name": "accounting.tax.paid",
            "snapshot-id": 123456789,
            "filter": {"type": "true"},
            "schema-id": 0,
            "projected-field-ids": [1, 2, 3],
            "projected-field-names": ["id", "name", "amount"],
            "metrics": {
                "total-planning-duration": {
                    "count": 1,
                    "time-unit": "nanoseconds",
                    "total-duration": 2644235116
                }
            }
        }"#;

        let request: ReportMetricsRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.report_type, "scan-report");
        assert_eq!(request.table_name, Some("accounting.tax.paid".to_string()));
        assert_eq!(request.snapshot_id, Some(123456789));
        assert!(request.extra.contains_key("filter"));
        assert!(request.extra.contains_key("metrics"));
    }

    #[test]
    fn test_commit_report_deserialization() {
        let json = r#"{
            "report-type": "commit-report",
            "table-name": "accounting.tax.paid",
            "snapshot-id": 123456790,
            "sequence-number": 5,
            "operation": "append",
            "metrics": {
                "total-duration": {
                    "count": 1,
                    "time-unit": "nanoseconds",
                    "total-duration": 500000000
                }
            }
        }"#;

        let request: ReportMetricsRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.report_type, "commit-report");
        assert_eq!(request.snapshot_id, Some(123456790));
        assert!(request.extra.contains_key("sequence-number"));
        assert!(request.extra.contains_key("operation"));
    }

    #[test]
    fn test_minimal_report_deserialization() {
        let json = r#"{"report-type": "scan-report"}"#;

        let request: ReportMetricsRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.report_type, "scan-report");
        assert_eq!(request.table_name, None);
        assert_eq!(request.snapshot_id, None);
    }
}
