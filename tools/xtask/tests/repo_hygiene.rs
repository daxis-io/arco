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

    let (success, output_text) = run_repo_hygiene_check_fatal(workspace.path());

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

#[test]
fn dangling_doc_references_warn_without_failing_by_default() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let plans = plans_dir();

    write_minimal_hygiene_workspace(workspace.path());
    let landed_plan = format!(
        "# Landed Plan\n\n\
         Cited roadmap: `{plans}/2026-06-27-missing-roadmap.md` (absent).\n"
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
        "dangling doc references must be warn-only until the branches that add \
         the cited documents land:\n{output_text}"
    );
    assert!(
        output_text.contains(&format!(
            "dangling doc reference '{plans}/2026-06-27-missing-roadmap.md'"
        )),
        "the warning must still name every dangling citation:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_HYGIENE_DANGLING_FATAL"),
        "the warning must document how to make the check fatal:\n{output_text}"
    );
}

#[test]
fn fenced_blocks_templates_and_deletion_records_are_not_citations() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let plans = plans_dir();

    write_minimal_hygiene_workspace(workspace.path());
    let landed_plan = format!(
        "# Landed Plan\n\n\
         ## Integration baseline\n\n\
         ```text\n\
         HEAD: 0000000000000000000000000000000000000000\n\
         status: D {plans}/2026-06-27-fenced-deleted-plan.md\n\
         ```\n\n\
         - Root checkout observed before worktree creation: `[ahead 1, behind 2]`\n  \
         with tracked deletion\n  \
         `{plans}/2026-06-27-wrapped-deleted-plan.md`; root was\n  \
         not modified.\n\n\
         - Deletion on one line: with tracked deletion \
         `{plans}/2026-06-27-inline-deleted-plan.md`.\n\n\
         Child plans are named `{plans}/YYYY-MM-DD-<slice-name>.md`.\n"
    );
    fs::write(
        workspace
            .path()
            .join(&plans)
            .join("2026-07-15-landed-plan.md"),
        landed_plan,
    )
    .expect("write landed plan");

    let (success, output_text) = run_repo_hygiene_check_fatal(workspace.path());

    assert!(
        success,
        "fenced blocks, deletion records, and naming templates must not be \
         treated as citations:\n{output_text}"
    );
    for absent in [
        "fenced-deleted-plan.md",
        "wrapped-deleted-plan.md",
        "inline-deleted-plan.md",
        "YYYY-MM-DD",
    ] {
        assert!(
            !output_text.contains(absent),
            "'{absent}' must not be reported as a dangling reference:\n{output_text}"
        );
    }
}

#[test]
fn citations_after_a_fenced_block_are_still_checked() {
    let workspace = TempDir::new().expect("create temporary workspace");
    let plans = plans_dir();

    write_minimal_hygiene_workspace(workspace.path());
    let landed_plan = format!(
        "# Landed Plan\n\n\
         ```text\n\
         status: D {plans}/2026-06-27-fenced-deleted-plan.md\n\
         ```\n\n\
         Cited roadmap: `{plans}/2026-06-27-missing-roadmap.md`.\n"
    );
    fs::write(
        workspace
            .path()
            .join(&plans)
            .join("2026-07-15-landed-plan.md"),
        landed_plan,
    )
    .expect("write landed plan");

    let (success, output_text) = run_repo_hygiene_check_fatal(workspace.path());

    assert!(
        !success,
        "closing a fence must re-enable citation scanning:\n{output_text}"
    );
    assert!(
        output_text.contains("2026-06-27-missing-roadmap.md"),
        "the citation after the fence must still be flagged:\n{output_text}"
    );
    assert!(
        !output_text.contains("fenced-deleted-plan.md"),
        "the fenced deletion record must stay unflagged:\n{output_text}"
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

/// Runs the check in its shipped (warn-only) dangling-reference mode.
fn run_repo_hygiene_check(workspace: &Path) -> (bool, String) {
    run_hygiene(workspace, false)
}

/// Runs the check with `ARCO_HYGIENE_DANGLING_FATAL=1`, the mode that
/// `DANGLING_DOC_REFERENCES_FATAL` will make the default once the branches
/// carrying the cited design documents land.
fn run_repo_hygiene_check_fatal(workspace: &Path) -> (bool, String) {
    run_hygiene(workspace, true)
}

/// Tracks every workspace file with git (the hygiene check enumerates tracked
/// files with `git ls-files`) and runs `xtask repo-hygiene-check` there.
fn run_hygiene(workspace: &Path, dangling_fatal: bool) -> (bool, String) {
    for args in [["init", "--quiet"], ["add", "--all"]] {
        let status = Command::new("git")
            .args(args)
            .current_dir(workspace)
            .output()
            .expect("run git in temporary workspace");
        assert!(
            status.status.success(),
            "git {args:?} failed:\n{}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command.arg("repo-hygiene-check").current_dir(workspace);
    if dangling_fatal {
        command.env("ARCO_HYGIENE_DANGLING_FATAL", "1");
    } else {
        command.env_remove("ARCO_HYGIENE_DANGLING_FATAL");
    }
    let output = command.output().expect("run xtask repo-hygiene-check");
    let output_text = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), output_text)
}
