//! `OpenAPI` compliance tests against the pinned Unity Catalog OSS spec.

use std::collections::HashSet;

use arco_uc::openapi::openapi;

fn load_uc_spec() -> Result<serde_json::Value, String> {
    let yaml = include_str!("fixtures/unitycatalog-openapi.yaml");
    let spec: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|err| format!("parse UC spec: {err}"))?;
    serde_json::to_value(spec).map_err(|err| format!("convert UC spec to JSON: {err}"))
}

fn assert_uc_spec_is_pinned(yaml: &str) {
    assert!(
        !(yaml.contains("PLACEHOLDER") || yaml.contains("REPLACE_ME")),
        "unitycatalog-openapi.yaml is a placeholder; replace it with a pinned upstream api/all.yaml and record the commit hash"
    );
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
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_request_body<'a>(
    spec: &'a serde_json::Value,
    operation: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    let request_body = operation.get("requestBody")?;
    if let Some(reference) = request_body.get("$ref").and_then(serde_json::Value::as_str) {
        return resolve_ref(spec, reference);
    }
    Some(request_body)
}

fn collect_request_body_content_types(
    spec: &serde_json::Value,
    operation: &serde_json::Value,
) -> HashSet<String> {
    resolve_request_body(spec, operation)
        .and_then(|request_body| request_body.get("content"))
        .and_then(serde_json::Value::as_object)
        .map(|content| content.keys().cloned().collect())
        .unwrap_or_default()
}

fn is_http_method(key: &str) -> bool {
    matches!(
        key,
        "get" | "post" | "put" | "delete" | "patch" | "head" | "options" | "trace"
    )
}

const V1_TARGET_OPERATIONS: &[(&str, &str)] = &[
    ("get", "/catalogs"),
    ("post", "/catalogs"),
    ("get", "/schemas"),
    ("post", "/schemas"),
    ("get", "/tables"),
    ("post", "/tables"),
    ("get", "/delta/preview/commits"),
    ("post", "/delta/preview/commits"),
    ("post", "/temporary-table-credentials"),
    ("post", "/temporary-path-credentials"),
];

#[test]
fn test_vendored_uc_spec_is_parseable() -> Result<(), String> {
    let _spec = load_uc_spec()?;
    Ok(())
}

#[test]
fn test_openapi_paths_align_with_vendored_spec() -> Result<(), String> {
    let ours =
        serde_json::to_value(openapi()).map_err(|err| format!("serialize openapi: {err}"))?;

    let yaml = include_str!("fixtures/unitycatalog-openapi.yaml");
    assert_uc_spec_is_pinned(yaml);
    let spec = load_uc_spec()?;

    let ours_paths = ours
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "generated spec is missing paths".to_string())?;
    let spec_paths = spec
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "pinned UC spec is missing paths".to_string())?;

    for &(method, path) in V1_TARGET_OPERATIONS {
        let ours_path_item = ours_paths
            .get(path)
            .ok_or_else(|| format!("missing {path} in generated spec"))?;
        let spec_path_item = spec_paths
            .get(path)
            .ok_or_else(|| format!("missing {path} in pinned UC spec"))?;

        let spec_path_item = spec_path_item
            .as_object()
            .ok_or_else(|| format!("expected object for path item {path}"))?;
        let ours_path_item = ours_path_item
            .as_object()
            .ok_or_else(|| format!("expected object for generated path item {path}"))?;

        assert!(
            is_http_method(method),
            "unsupported method in test list: {method}"
        );
        let spec_op = spec_path_item
            .get(method)
            .ok_or_else(|| format!("missing {method} {path} in pinned UC spec"))?;
        let ours_op = ours_path_item
            .get(method)
            .ok_or_else(|| format!("missing {method} {path} operation"))?;

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

        let spec_body_types = collect_request_body_content_types(&spec, spec_op);
        let ours_body_types = collect_request_body_content_types(&ours, ours_op);
        if !spec_body_types.is_empty() {
            assert!(
                !ours_body_types.is_empty(),
                "request body missing for {method} {path}: pinned UC spec defines one"
            );
            assert!(
                spec_body_types.is_subset(&ours_body_types),
                "request body content-type mismatch for {method} {path}: missing {:?}",
                spec_body_types
                    .difference(&ours_body_types)
                    .collect::<Vec<_>>()
            );
        }
    }

    Ok(())
}
