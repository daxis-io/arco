use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

// Built by concatenation so this test file does not trip the hygiene check's
// own forbidden-path-marker scan when it runs over the real repository.
fn plans_dir() -> String {
    format!("{}/{}", "docs", "plans")
}

fn spec_dir() -> String {
    format!("{}/{}", "docs", "spec")
}

#[test]
fn dangling_doc_references_are_rejected() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let plans = plans_dir();
    let spec = spec_dir();

    write_minimal_hygiene_workspace(workspace.path());
    fs::write(
        workspace.path().join(&spec).join("present-contract.md"),
        "# Present Contract\n\nThis contract exists.\n",
    )
    .expect("write present spec contract");

    let landed_plan = format!(
        "# Landed Plan\n\n\
         Cited roadmap: `{plans}/2026-06-27-missing-roadmap.md` (absent).\n\n\
         Cited contract: [contract]({spec}/missing-contract.md) (absent).\n\n\
         Cited upstream copy: https://example.com/{plans}/ignored-remote-plan.md\n\n\
         Cited existing contract: `{spec}/present-contract.md`.\n"
    );
    fs::write(
        workspace
            .path()
            .join(&plans)
            .join("2026-07-05-landed-plan.md"),
        landed_plan,
    )
    .expect("write landed plan");

    let (success, output_text) = run_repo_hygiene_check(workspace.path());

    assert!(
        !success,
        "dangling doc references should fail repo-hygiene-check:\n{output_text}"
    );
    assert!(
        output_text.contains(&format!(
            "dangling doc reference '{plans}/2026-06-27-missing-roadmap.md'"
        )),
        "failure should list the missing plan citation:\n{output_text}"
    );
    assert!(
        output_text.contains(&format!(
            "dangling doc reference '{spec}/missing-contract.md'"
        )),
        "failure should list the missing spec citation:\n{output_text}"
    );
    assert!(
        !output_text.contains("ignored-remote-plan.md"),
        "URL citations must not be flagged:\n{output_text}"
    );
    assert!(
        !output_text.contains(&format!(
            "dangling doc reference '{spec}/present-contract.md'"
        )),
        "citations of existing files must not be flagged:\n{output_text}"
    );
}

#[test]
fn resolvable_doc_references_pass() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let plans = plans_dir();
    let spec = spec_dir();

    write_minimal_hygiene_workspace(workspace.path());
    fs::write(
        workspace
            .path()
            .join(&plans)
            .join("2026-06-27-existing-roadmap.md"),
        "# Existing Roadmap\n",
    )
    .expect("write existing roadmap");
    fs::write(
        workspace.path().join(&spec).join("present-contract.md"),
        "# Present Contract\n",
    )
    .expect("write present spec contract");
    fs::create_dir_all(workspace.path().join(&spec).join("contracts"))
        .expect("create spec contracts directory");
    fs::write(
        workspace
            .path()
            .join(&spec)
            .join("contracts")
            .join("token-shapes.md"),
        "# Token Shapes\n",
    )
    .expect("write nested spec contract");

    let landed_plan = format!(
        "# Landed Plan\n\n\
         Cited roadmap: `{plans}/2026-06-27-existing-roadmap.md`.\n\n\
         Cited contract: [contract]({spec}/present-contract.md).\n\n\
         Cited contract directory: `{spec}/contracts`.\n"
    );
    fs::write(
        workspace
            .path()
            .join(&plans)
            .join("2026-07-05-landed-plan.md"),
        landed_plan,
    )
    .expect("write landed plan");

    let (success, output_text) = run_repo_hygiene_check(workspace.path());

    assert!(
        success,
        "resolvable doc references should pass repo-hygiene-check:\n{output_text}"
    );
}

/// Creates the minimum tracked layout the hygiene check expects: an mdBook
/// summary without links plus empty plan/spec directories.
fn write_minimal_hygiene_workspace(workspace: &Path) {
    fs::create_dir_all(workspace.join("docs/guide/src")).expect("create guide source directory");
    fs::write(workspace.join("docs/guide/src/SUMMARY.md"), "# Summary\n")
        .expect("write mdBook summary");
    fs::create_dir_all(workspace.join(plans_dir())).expect("create plans directory");
    fs::create_dir_all(workspace.join(spec_dir())).expect("create spec directory");
}

/// Tracks every workspace file with git (the hygiene check enumerates tracked
/// files with `git ls-files`) and runs `xtask repo-hygiene-check` there.
fn run_repo_hygiene_check(workspace: &Path) -> (bool, String) {
    for args in [["init", "--quiet"], ["add", "--all"]] {
        let status = Command::new("git")
            .args(&args)
            .current_dir(workspace)
            .output()
            .expect("run git in temporary workspace");
        assert!(
            status.status.success(),
            "git {args:?} failed:\n{}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("repo-hygiene-check")
        .current_dir(workspace)
        .output()
        .expect("run xtask repo-hygiene-check");
    let output_text = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), output_text)
}
