//! OpenAPI compliance tests against the official Iceberg REST spec.

use std::collections::HashSet;

use arco_iceberg::openapi::openapi;

fn load_iceberg_spec() -> serde_json::Value {
    let yaml = include_str!("fixtures/iceberg-rest-openapi.yaml");
    let spec: serde_yaml::Value = serde_yaml::from_str(yaml).expect("parse Iceberg spec");
    serde_json::to_value(spec).expect("convert Iceberg spec")
}

fn resolve_ref<'a>(spec: &'a serde_json::Value, reference: &str) -> Option<&'a serde_json::Value> {
    let path = reference.strip_prefix("#/")?;
    path.split('/').try_fold(spec, |value, key| value.get(key))
}

fn param_name(spec: &serde_json::Value, param: &serde_json::Value) -> Option<String> {
    if let Some(name) = param.get("name").and_then(serde_json::Value::as_str) {
        return Some(name.to_string());
    }

    let reference = param.get("$ref").and_then(serde_json::Value::as_str)?;
    resolve_ref(spec, reference)
        .and_then(|resolved| resolved.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn collect_param_names(
    spec: &serde_json::Value,
    path_item: &serde_json::Value,
    operation: &serde_json::Value,
) -> HashSet<String> {
    let mut names = HashSet::new();
    for params in [path_item.get("parameters"), operation.get("parameters")] {
        let Some(params) = params.and_then(serde_json::Value::as_array) else {
            continue;
        };
        for param in params {
            if let Some(name) = param_name(spec, param) {
                names.insert(name);
            }
        }
    }
    names
}

fn collect_response_codes(operation: &serde_json::Value) -> HashSet<String> {
    operation
        .get("responses")
        .and_then(serde_json::Value::as_object)
        .map(|responses| {
            responses
                .keys()
                .filter(|code| code.chars().all(|c| c.is_ascii_digit()))
                .map(|code| code.to_string())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn test_openapi_paths_align_with_official_spec() {
    let ours = serde_json::to_value(openapi()).expect("serialize openapi");
    let spec = load_iceberg_spec();

    let endpoints = [
        ("/v1/config", "get"),
        ("/v1/{prefix}/namespaces", "get"),
        ("/v1/{prefix}/namespaces", "post"),
        ("/v1/{prefix}/namespaces/{namespace}", "get"),
        ("/v1/{prefix}/namespaces/{namespace}", "head"),
        ("/v1/{prefix}/namespaces/{namespace}", "delete"),
        ("/v1/{prefix}/namespaces/{namespace}/properties", "post"),
        ("/v1/{prefix}/namespaces/{namespace}/tables", "get"),
        ("/v1/{prefix}/namespaces/{namespace}/tables", "post"),
        ("/v1/{prefix}/namespaces/{namespace}/tables/{table}", "get"),
        ("/v1/{prefix}/namespaces/{namespace}/tables/{table}", "head"),
        ("/v1/{prefix}/namespaces/{namespace}/tables/{table}", "delete"),
        ("/v1/{prefix}/namespaces/{namespace}/tables/{table}", "post"),
        ("/v1/{prefix}/namespaces/{namespace}/register", "post"),
        (
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
            "get",
        ),
    ];

    for (path, method) in endpoints {
        let ours_paths = ours
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("paths");
        let spec_paths = spec
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("paths");

        let ours_path_item = ours_paths
            .get(path)
            .unwrap_or_else(|| panic!("missing {method} {path} in generated spec"));
        let spec_path_item = spec_paths
            .get(path)
            .unwrap_or_else(|| panic!("missing {method} {path} in Iceberg spec"));

        let ours_op = ours_path_item
            .get(method)
            .unwrap_or_else(|| panic!("missing {method} {path} operation"));
        let spec_op = spec_path_item
            .get(method)
            .unwrap_or_else(|| panic!("missing {method} {path} operation in spec"));

        let ours_params = collect_param_names(&ours, ours_path_item, ours_op);
        let spec_params = collect_param_names(&spec, spec_path_item, spec_op);
        assert!(
            spec_params.is_subset(&ours_params),
            "parameter mismatch for {method} {path}: missing {:?}",
            spec_params.difference(&ours_params).collect::<Vec<_>>()
        );

        let ours_responses = collect_response_codes(ours_op);
        let spec_responses = collect_response_codes(spec_op);
        assert!(
            spec_responses.is_subset(&ours_responses),
            "response mismatch for {method} {path}: missing {:?}",
            spec_responses
                .difference(&ours_responses)
                .collect::<Vec<_>>()
        );
    }
}
