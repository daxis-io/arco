//! OpenAPI compliance tests against the pinned Unity Catalog OSS spec.

use std::collections::HashSet;

use arco_uc::openapi::openapi;

fn load_uc_spec() -> serde_json::Value {
    let yaml = include_str!("fixtures/unitycatalog-openapi.yaml");
    let spec: serde_yaml::Value = serde_yaml::from_str(yaml).expect("parse UC spec");
    serde_json::to_value(spec).expect("convert UC spec")
}

fn assert_uc_spec_is_pinned(yaml: &str) {
    if yaml.contains("PLACEHOLDER") || yaml.contains("REPLACE_ME") {
        panic!(
            "unitycatalog-openapi.yaml is a placeholder; replace it with a pinned upstream api/all.yaml and record the commit hash"
        );
    }
    if !yaml
        .lines()
        .any(|line| line.starts_with("# Upstream commit: "))
    {
        panic!("unitycatalog-openapi.yaml must include a '# Upstream commit: <hash>' header");
    }
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

fn is_http_method(key: &str) -> bool {
    matches!(
        key,
        "get" | "post" | "put" | "delete" | "patch" | "head" | "options" | "trace"
    )
}

#[test]
fn test_vendored_uc_spec_is_parseable() {
    let _spec = load_uc_spec();
}

#[test]
fn test_openapi_paths_align_with_vendored_spec() {
    let ours = serde_json::to_value(openapi()).expect("serialize openapi");

    let yaml = include_str!("fixtures/unitycatalog-openapi.yaml");
    assert_uc_spec_is_pinned(yaml);
    let spec = load_uc_spec();

    let ours_paths = ours
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("paths");
    let spec_paths = spec
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("paths");

    for (path, ours_path_item) in ours_paths {
        if path == "/openapi.json" {
            // Arco-local endpoint; not part of Unity Catalog upstream contract.
            continue;
        }

        let spec_path_item = spec_paths
            .get(path)
            .unwrap_or_else(|| panic!("generated path {path} is missing from UC vendored spec"));

        let spec_path_item = spec_path_item
            .as_object()
            .unwrap_or_else(|| panic!("expected object for path item {path}"));
        let ours_path_item = ours_path_item
            .as_object()
            .unwrap_or_else(|| panic!("expected object for generated path item {path}"));

        for (method, ours_op) in ours_path_item {
            if !is_http_method(method.as_str()) {
                continue;
            }

            let spec_op = spec_path_item
                .get(method)
                .unwrap_or_else(|| panic!("missing {method} {path} operation in UC vendored spec"));

            let ours_params = collect_param_names(
                &ours,
                &serde_json::Value::Object(ours_path_item.clone()),
                ours_op,
            );
            let spec_params = collect_param_names(
                &spec,
                &serde_json::Value::Object(spec_path_item.clone()),
                spec_op,
            );
            assert!(
                ours_params.is_subset(&spec_params),
                "parameter mismatch for {method} {path}: generated params not in upstream spec: {:?}",
                ours_params.difference(&spec_params).collect::<Vec<_>>()
            );

            let ours_responses = collect_response_codes(ours_op);
            let spec_responses = collect_response_codes(spec_op);
            assert!(
                ours_responses.is_subset(&spec_responses),
                "response mismatch for {method} {path}: generated responses not in upstream spec: {:?}",
                ours_responses
                    .difference(&spec_responses)
                    .collect::<Vec<_>>()
            );
        }
    }
}
