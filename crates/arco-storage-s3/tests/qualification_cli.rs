//! Qualification command inventory and fail-closed admission.
#![cfg(feature = "qualification")]
#![allow(clippy::unwrap_used)]
use std::process::Command;

#[test]
fn inventory_and_missing_configuration() {
    let executable = env!("CARGO_BIN_EXE_arco-state-store-qualification");
    let inventory = Command::new(executable).arg("inventory").output().unwrap();
    assert!(inventory.status.success());
    let inventory: serde_json::Value = serde_json::from_slice(&inventory.stdout).unwrap();
    assert!(inventory["common"].as_array().unwrap().len() >= 3);
    assert_eq!(inventory["provider"]["scenarios"], inventory["common"]);
    assert_eq!(inventory["provider"]["repetitions"], 5);
    for command in ["provider", "pilot", "unknown"] {
        assert!(
            !Command::new(executable)
                .arg(command)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}

#[test]
fn credential_free_catalog_restore_and_maintenance_scenarios() {
    let output = Command::new(env!("CARGO_BIN_EXE_arco-state-store-qualification"))
        .arg("local")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = String::from_utf8(output.stdout).unwrap();
    let last: serde_json::Value = serde_json::from_str(records.lines().last().unwrap()).unwrap();
    assert_eq!(last["kind"], "completed");
}

#[test]
fn loopback_rejects_inherited_endpoint_and_credential_overrides_before_traffic() {
    for key in [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_PROFILE",
        "AWS_SESSION_TOKEN",
        "HTTP_PROXY",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_arco-state-store-qualification"))
            .env_clear()
            .env("AWS_ENDPOINT", "http://127.0.0.1:9")
            .env("AWS_ACCESS_KEY_ID", "gate7-test")
            .env("AWS_SECRET_ACCESS_KEY", "gate7-test")
            .env("AWS_ALLOW_HTTP", "true")
            .env("AWS_REGION", "us-east-1")
            .env(key, "http://127.0.0.1:9")
            .arg("loopback")
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unexpected loopback configuration"),
            "{key}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
