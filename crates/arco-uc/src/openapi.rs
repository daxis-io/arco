//! `OpenAPI` (3.1) specification generation for the Unity Catalog facade.

use std::sync::OnceLock;

/// Marker type exposing the vendored UC OpenAPI contract.
pub struct UnityCatalogApiDoc;

impl UnityCatalogApiDoc {
    /// Returns the vendored OpenAPI contract.
    #[must_use]
    pub fn openapi() -> serde_json::Value {
        openapi()
    }
}

fn load_vendored_openapi() -> serde_json::Value {
    let yaml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/unitycatalog-openapi.yaml"
    ));
    let yaml_value: serde_yaml::Value =
        serde_yaml::from_str(yaml).expect("vendored UC fixture should parse as YAML");
    serde_json::to_value(yaml_value).expect("vendored UC fixture should convert to JSON")
}

static OPENAPI_CACHE: OnceLock<serde_json::Value> = OnceLock::new();

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> serde_json::Value {
    OPENAPI_CACHE.get_or_init(load_vendored_openapi).clone()
}

static OPENAPI_JSON_CACHE: OnceLock<String> = OnceLock::new();

/// Returns the generated `OpenAPI` spec serialized as pretty JSON.
///
/// # Errors
///
/// Returns an error if JSON serialization fails (should not happen).
pub fn openapi_json() -> Result<String, serde_json::Error> {
    if let Some(spec) = OPENAPI_JSON_CACHE.get() {
        return Ok(spec.clone());
    }

    let spec = serde_json::to_string_pretty(&openapi())?;
    let _ = OPENAPI_JSON_CACHE.set(spec.clone());
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openapi_generation() {
        let spec = openapi();
        assert_eq!(
            spec.get("info")
                .and_then(|info| info.get("title"))
                .and_then(serde_json::Value::as_str),
            Some("Unity Catalog API")
        );
        assert!(
            spec.get("paths")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|paths| paths.contains_key("/catalogs"))
        );
    }
}
