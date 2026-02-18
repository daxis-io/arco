# Gate 1 Verification Notes (Slim Evidence Model)

- Generated UTC: 2026-02-18T19:32:09Z
- Tag: v0.1.4
- Tag object: 24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a
- Tag commit: b26c3d872f82d4cee3252a0d373f0cc56e13997c
- Repo: daxis-io/arco

These notes capture command, exit code, and key output only.
Canonical remote evidence is the linked GitHub run/release URLs.

## shell_syntax_gate1_scripts

- timestamp_utc: 2026-02-18T19:32:09Z
- command: bash -n tools/check-release-tag-discipline.sh tools/collect_release_evidence.sh tools/test_release_gate1_hardening.sh
- exit_code: 0
- expectation: zero
- result: PASS

```text

```

## verify_signed_tag

- timestamp_utc: 2026-02-18T19:32:10Z
- command: git -c gpg.format=ssh -c gpg.ssh.allowedSignersFile=.github/release-signers.allowed tag -v v0.1.4
- exit_code: 0
- expectation: zero
- result: PASS

```text
Good "git" signature for ethanurbanski@gmail.com with ED25519 key SHA256:lsKDw3E6bcKes/hsqqibRtyaHywPt0V1fLx5uYxeeFg
object b26c3d872f82d4cee3252a0d373f0cc56e13997c
type commit
tag v0.1.4
tagger Ethan Urbanski <ethanurbanski@gmail.com> 1771440986 -0500

v0.1.4
```

## discipline_pass_v0_1_4

- timestamp_utc: 2026-02-18T19:32:10Z
- command: bash tools/check-release-tag-discipline.sh --tag v0.1.4
- exit_code: 0
- expectation: zero
- result: PASS

```text
release-tag discipline checks passed for v0.1.4
```

## discipline_expected_fail_v0_1_4

- timestamp_utc: 2026-02-18T19:32:10Z
- command: bash tools/check-release-tag-discipline.sh --tag v0.1.4 --release-notes '/var/folders/q3/qdy84b_d5vj5scsr8njl55300000gn/T/tmp.Zk0QMd1haW'
- exit_code: 1
- expectation: nonzero
- result: PASS

```text
release notes title must be '# Release Notes for v0.1.4'
```

## collector_create_pack

- timestamp_utc: 2026-02-18T19:32:10Z
- command: bash tools/collect_release_evidence.sh --tag v0.1.4
- exit_code: 0
- expectation: zero
- result: PASS

```text
collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c
```

## collector_expected_path

- timestamp_utc: 2026-02-18T19:32:10Z
- expected: collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c
- observed: collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c
- result: PASS

## collector_manifest_verify

- timestamp_utc: 2026-02-18T19:32:10Z
- command: cd 'collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c' && sha256sum -c manifest.sha256
- exit_code: 0
- expectation: zero
- result: PASS

```text
changelog-section.md: OK
metadata.env: OK
release-notes.md: OK
release-process.md: OK
release-signers.allowed: OK
sbom-artifact-linkage.txt: OK
tag-object.txt: OK
tag-ref.txt: OK
workflow-ci.yml: OK
workflow-release-sbom.yml: OK
```

## collector_allow_existing

- timestamp_utc: 2026-02-18T19:32:10Z
- command: bash tools/collect_release_evidence.sh --tag v0.1.4 --allow-existing
- exit_code: 0
- expectation: zero
- result: PASS

```text
collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c
```

## release_sbom_run

- timestamp_utc: 2026-02-18T19:32:10Z
- command: gh run list --repo daxis-io/arco --workflow release-sbom.yml --event push --json databaseId,headSha,status,conclusion,url,createdAt,updatedAt --limit 30 | jq '[.[] | select(.headSha=="b26c3d872f82d4cee3252a0d373f0cc56e13997c")]'
- exit_code: 0
- expectation: zero
- result: PASS

```text
[
  {
    "conclusion": "success",
    "createdAt": "2026-02-18T18:56:29Z",
    "databaseId": 22153366267,
    "headSha": "b26c3d872f82d4cee3252a0d373f0cc56e13997c",
    "status": "completed",
    "updatedAt": "2026-02-18T18:57:20Z",
    "url": "https://github.com/daxis-io/arco/actions/runs/22153366267"
  }
]
```

## ci_run

- timestamp_utc: 2026-02-18T19:32:11Z
- command: gh run list --repo daxis-io/arco --workflow ci.yml --event push --json databaseId,headSha,status,conclusion,url,createdAt,updatedAt --limit 30 | jq '[.[] | select(.headSha=="b26c3d872f82d4cee3252a0d373f0cc56e13997c")]'
- exit_code: 0
- expectation: zero
- result: PASS

```text
[
  {
    "conclusion": "success",
    "createdAt": "2026-02-18T18:56:29Z",
    "databaseId": 22153366241,
    "headSha": "b26c3d872f82d4cee3252a0d373f0cc56e13997c",
    "status": "completed",
    "updatedAt": "2026-02-18T19:19:56Z",
    "url": "https://github.com/daxis-io/arco/actions/runs/22153366241"
  }
]
```

## release_tag_discipline_check_run

- timestamp_utc: 2026-02-18T19:32:12Z
- command: gh api repos/daxis-io/arco/commits/b26c3d872f82d4cee3252a0d373f0cc56e13997c/check-runs --jq '.check_runs[] | {name,status,conclusion,html_url,started_at,completed_at} | select(.name=="Release Tag Discipline")'
- exit_code: 0
- expectation: zero
- result: PASS

```text
{"completed_at":"2026-02-18T18:56:47Z","conclusion":"success","html_url":"https://github.com/daxis-io/arco/actions/runs/22153366241/job/64050488799","name":"Release Tag Discipline","started_at":"2026-02-18T18:56:32Z","status":"completed"}
```

## release_sbom_job_steps

- timestamp_utc: 2026-02-18T19:32:13Z
- command: gh api repos/daxis-io/arco/actions/runs/22153366267/jobs --jq '.jobs[] | {name,conclusion,html_url,steps:[.steps[] | select(.name=="Validate annotated signed release tag" or .name=="Publish build provenance attestation for SBOM artifacts" or .name=="Upload release SBOM artifact pack (retained)" or .name=="Attach SBOM files to GitHub release" or .name=="Upload release evidence pack artifact (retained)") | {name,conclusion}]}'
- exit_code: 0
- expectation: zero
- result: PASS

```text
{"conclusion":"success","html_url":"https://github.com/daxis-io/arco/actions/runs/22153366267/job/64050488918","name":"sbom","steps":[{"conclusion":"success","name":"Validate annotated signed release tag"},{"conclusion":"success","name":"Upload release SBOM artifact pack (retained)"},{"conclusion":"success","name":"Publish build provenance attestation for SBOM artifacts"},{"conclusion":"success","name":"Attach SBOM files to GitHub release"},{"conclusion":"success","name":"Upload release evidence pack artifact (retained)"}]}
```

## release_sbom_artifacts

- timestamp_utc: 2026-02-18T19:32:13Z
- command: gh api repos/daxis-io/arco/actions/runs/22153366267/artifacts | jq '{total_count,artifacts:[.artifacts[] | {name,expired,created_at,expires_at,size_in_bytes,id}]}'
- exit_code: 0
- expectation: zero
- result: PASS

```text
{
  "total_count": 2,
  "artifacts": [
    {
      "name": "release-sbom-v0.1.4",
      "expired": false,
      "created_at": "2026-02-18T18:57:13Z",
      "expires_at": "2026-05-19T18:56:30Z",
      "size_in_bytes": 128662,
      "id": 5561078714
    },
    {
      "name": "release-evidence-v0.1.4",
      "expired": false,
      "created_at": "2026-02-18T18:57:17Z",
      "expires_at": "2026-05-19T18:56:30Z",
      "size_in_bytes": 8173,
      "id": 5561079754
    }
  ]
}
```

## release_assets_v0_1_4

- timestamp_utc: 2026-02-18T19:32:13Z
- command: gh release view v0.1.4 --repo daxis-io/arco --json tagName,url,publishedAt,assets | jq '{tagName,url,publishedAt,assets:[.assets[] | {name,size,url}]}'
- exit_code: 0
- expectation: zero
- result: PASS

```text
{
  "tagName": "v0.1.4",
  "url": "https://github.com/daxis-io/arco/releases/tag/v0.1.4",
  "publishedAt": "2026-02-18T18:57:16Z",
  "assets": [
    {
      "name": "arco-v0.1.4.cyclonedx.json",
      "size": 547629,
      "url": "https://github.com/daxis-io/arco/releases/download/v0.1.4/arco-v0.1.4.cyclonedx.json"
    },
    {
      "name": "arco-v0.1.4.sbom.sha256",
      "size": 181,
      "url": "https://github.com/daxis-io/arco/releases/download/v0.1.4/arco-v0.1.4.sbom.sha256"
    },
    {
      "name": "arco-v0.1.4.spdx.json",
      "size": 741753,
      "url": "https://github.com/daxis-io/arco/releases/download/v0.1.4/arco-v0.1.4.spdx.json"
    }
  ]
}
```

## gate_tracker_json_valid

- timestamp_utc: 2026-02-18T19:32:14Z
- command: jq -e . docs/audits/2026-02-12-prod-readiness/gate-tracker.json >/dev/null
- exit_code: 0
- expectation: zero
- result: PASS

```text

```

## cleanup

- collector-packs/ generated during local verification is intentionally not committed.
- Canonical retained collector evidence for v0.1.4 is the release-evidence-v0.1.4 workflow artifact listed in release_sbom_artifacts.

## gate_tracker_json_valid_final

- timestamp_utc: 2026-02-18T19:34:00Z
- command: jq -e . docs/audits/2026-02-12-prod-readiness/gate-tracker.json >/dev/null
- exit_code: 0
- expectation: zero
- result: PASS

```text

```

## full_release_gate1_hardening_suite

- timestamp_utc: 2026-02-18T19:35:59Z
- command: bash tools/test_release_gate1_hardening.sh
- exit_code: 0
- expectation: zero
- result: PASS

```text
PASS: Workflow resolves RELEASE_TAG from dispatch input or pushed tag ref
PASS: Workflow checks out exact release tag ref
PASS: Workflow waits for successful tag CI before publishing SBOM
PASS: Workflow queries CI workflow runs by commit SHA
PASS: Workflow checks CI jobs for Release Tag Discipline result
PASS: Workflow verifies signed tags against repository allowed-signers file
PASS: Workflow captures deterministic tag verification transcript
PASS: Attestation action pinned by full commit SHA
PASS: Collector handles pre-existing deterministic pack directory explicitly
PASS: Collector verifies existing manifest in allow-existing mode
PASS: Collector documents no-rewrite semantics for allow-existing mode
PASS: Collector snapshots release signer trust roots into immutable evidence packs
PASS: allow-existing does not rewrite manifest for existing deterministic pack
PASS: allow-existing leaves manifest contents unchanged
All release gate-1 hardening checks passed.
```

## gate_tracker_json_valid_latest

- timestamp_utc: 2026-02-18T19:36:22Z
- command: jq -e . docs/audits/2026-02-12-prod-readiness/gate-tracker.json >/dev/null
- exit_code: 0
- expectation: zero
- result: PASS

```text

```
