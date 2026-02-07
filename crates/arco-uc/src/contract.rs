//! Unity Catalog contract helpers backed by the pinned OpenAPI fixture.

use std::sync::OnceLock;

use axum::http::Method;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Parameter,
}

#[derive(Debug, Clone)]
struct EndpointPattern {
    method: Method,
    path_template: String,
    segments: Vec<Segment>,
}

static ENDPOINT_PATTERNS: OnceLock<Vec<EndpointPattern>> = OnceLock::new();
const UC_MOUNT_PREFIX: &str = "/api/2.1/unity-catalog";

fn normalize_path(path: &str) -> &str {
    if path != "/" && path.ends_with('/') {
        path.trim_end_matches('/')
    } else {
        path
    }
}

fn canonicalize_request_path(path: &str) -> &str {
    if path == UC_MOUNT_PREFIX {
        "/"
    } else if let Some(stripped) = path.strip_prefix(UC_MOUNT_PREFIX) {
        if stripped.starts_with('/') {
            stripped
        } else {
            path
        }
    } else {
        path
    }
}

fn parse_segments(path: &str) -> Vec<Segment> {
    normalize_path(path)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') && segment.len() > 2 {
                Segment::Parameter
            } else {
                Segment::Literal(segment.to_string())
            }
        })
        .collect()
}

fn methods_from_path_item(path_item: &serde_json::Value) -> Vec<Method> {
    let Some(path_item) = path_item.as_object() else {
        return Vec::new();
    };

    path_item
        .keys()
        .filter_map(|key| match key.as_str() {
            "get" => Some(Method::GET),
            "post" => Some(Method::POST),
            "put" => Some(Method::PUT),
            "patch" => Some(Method::PATCH),
            "delete" => Some(Method::DELETE),
            "head" => Some(Method::HEAD),
            "options" => Some(Method::OPTIONS),
            "trace" => Some(Method::TRACE),
            _ => None,
        })
        .collect()
}

fn load_endpoint_patterns() -> Vec<EndpointPattern> {
    let yaml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/unitycatalog-openapi.yaml"
    ));
    let parsed_yaml: serde_yaml::Value =
        serde_yaml::from_str(yaml).expect("UC fixture should parse as YAML");
    let parsed_json =
        serde_json::to_value(parsed_yaml).expect("UC fixture should convert to JSON value");
    let paths = parsed_json
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("UC fixture should contain OpenAPI `paths` object");

    let mut patterns = Vec::new();
    for (path_template, path_item) in paths {
        let methods = methods_from_path_item(path_item);
        for method in methods {
            patterns.push(EndpointPattern {
                method,
                path_template: path_template.clone(),
                segments: parse_segments(path_template),
            });
        }
    }
    patterns
}

fn endpoint_patterns() -> &'static [EndpointPattern] {
    ENDPOINT_PATTERNS
        .get_or_init(load_endpoint_patterns)
        .as_slice()
}

fn segments_match(expected: &[Segment], actual: &[&str]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }

    expected
        .iter()
        .zip(actual.iter())
        .all(
            |(expected_segment, actual_segment)| match expected_segment {
                Segment::Literal(expected_literal) => expected_literal == actual_segment,
                Segment::Parameter => !actual_segment.is_empty(),
            },
        )
}

/// Returns true when the supplied method/path pair exists in the pinned UC OpenAPI spec.
#[must_use]
pub fn is_known_operation(method: &Method, path: &str) -> bool {
    let normalized = normalize_path(canonicalize_request_path(path));
    let actual_segments: Vec<&str> = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    endpoint_patterns()
        .iter()
        .filter(|pattern| &pattern.method == method)
        .any(|pattern| segments_match(&pattern.segments, &actual_segments))
}

/// Returns true if a route is in the pinned spec and explicitly in Scope A.
#[must_use]
pub fn is_scope_a_operation(method: &Method, path: &str) -> bool {
    let normalized = normalize_path(canonicalize_request_path(path));
    let actual_segments: Vec<&str> = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    let scope_a_definitions: &[(&Method, &str)] = &[
        (&Method::GET, "/catalogs"),
        (&Method::GET, "/catalogs/{name}"),
        (&Method::GET, "/schemas"),
        (&Method::GET, "/schemas/{full_name}"),
        (&Method::GET, "/tables"),
        (&Method::GET, "/tables/{full_name}"),
        (&Method::GET, "/permissions/{securable_type}/{full_name}"),
        (&Method::POST, "/temporary-table-credentials"),
        (&Method::POST, "/temporary-path-credentials"),
        (&Method::GET, "/delta/preview/commits"),
        (&Method::POST, "/delta/preview/commits"),
    ];

    endpoint_patterns().iter().any(|pattern| {
        scope_a_definitions
            .iter()
            .any(|(allowed_method, allowed_path)| {
                &pattern.method == *allowed_method
                    && pattern.path_template == *allowed_path
                    && &pattern.method == method
                    && segments_match(&pattern.segments, &actual_segments)
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_operation_matches_literal_path() {
        assert!(is_known_operation(&Method::GET, "/catalogs"));
    }

    #[test]
    fn test_known_operation_matches_parameterized_path() {
        assert!(is_known_operation(
            &Method::GET,
            "/permissions/table/main.default.some_table"
        ));
        assert!(is_known_operation(&Method::GET, "/catalogs/some-catalog"));
    }

    #[test]
    fn test_unknown_method_on_known_path_is_not_known_operation() {
        assert!(!is_known_operation(&Method::PUT, "/catalogs"));
    }

    #[test]
    fn test_unknown_path_is_not_known_operation() {
        assert!(!is_known_operation(&Method::GET, "/definitely-not-in-spec"));
    }

    #[test]
    fn test_known_operation_matches_when_mounted_under_prefix() {
        assert!(is_known_operation(
            &Method::POST,
            "/api/2.1/unity-catalog/catalogs"
        ));
        assert!(is_scope_a_operation(
            &Method::GET,
            "/api/2.1/unity-catalog/catalogs"
        ));
    }

    #[test]
    fn test_scope_a_detection() {
        assert!(is_scope_a_operation(&Method::GET, "/catalogs"));
        assert!(is_scope_a_operation(&Method::GET, "/catalogs/some-name"));
        assert!(is_scope_a_operation(
            &Method::GET,
            "/permissions/table/main.default.some_table"
        ));
        assert!(!is_scope_a_operation(&Method::POST, "/catalogs"));
        assert!(!is_scope_a_operation(
            &Method::POST,
            "/temporary-volume-credentials"
        ));
    }
}
