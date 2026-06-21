use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

#[test]
fn duplicate_adr_ids_are_rejected() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let adr_dir = workspace.path().join("docs/adr");
    fs::create_dir_all(&adr_dir).expect("create ADR directory");

    let mut readme = String::from(
        "# Architecture Decision Records\n\n## Index\n\n| ADR | Title | Status |\n|-----|-------|--------|\n",
    );
    for (id, filename, title) in [
        (
            1,
            "adr-001-parquet-metadata.md",
            "Parquet-first metadata storage",
        ),
        (2, "adr-002-id-strategy.md", "ID strategy by entity type"),
        (
            3,
            "adr-003-manifest-domains.md",
            "Manifest domain names and contention",
        ),
        (
            4,
            "adr-004-event-envelope.md",
            "Event envelope format and evolution",
        ),
        (5, "adr-005-storage-layout.md", "Canonical storage layout"),
        (
            6,
            "adr-006-schema-evolution.md",
            "Parquet schema evolution policy",
        ),
    ] {
        write_adr(&adr_dir.join(&filename), id, &title);
        readme.push_str(&format!("| [{id:03}]({filename}) | {title} | Accepted |\n"));
    }

    write_adr(&adr_dir.join("adr-032-one.md"), 32, "First duplicate ADR");
    write_adr(&adr_dir.join("adr-032-two.md"), 32, "Second duplicate ADR");
    readme.push_str("| [032](adr-032-one.md) | First duplicate ADR | Accepted |\n");
    readme.push_str("| [032](adr-032-two.md) | Second duplicate ADR | Accepted |\n");
    fs::write(adr_dir.join("README.md"), readme).expect("write ADR README");

    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("adr-check")
        .current_dir(workspace.path())
        .output()
        .expect("run xtask adr-check");
    let output_text = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        !output.status.success(),
        "duplicate ADR IDs should fail adr-check:\n{output_text}"
    );
    assert!(
        output_text.contains("duplicate ADR ID 032"),
        "failure should identify the duplicate ADR ID:\n{output_text}"
    );
}

fn write_adr(path: &Path, id: u32, title: &str) {
    fs::write(
        path,
        format!(
            "# ADR-{id:03}: {title}\n\n## Status\n\nAccepted\n\n## Context\n\nContext.\n\n## Decision\n\nDecision.\n\n## Consequences\n\nConsequences.\n"
        ),
    )
    .expect("write ADR");
}
