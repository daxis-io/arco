# Evidence Policy

Repository history should contain durable artifacts only.

## Policy

- Transient verification evidence belongs in CI artifacts, release assets, or external build logs.
- Long-lived claims in docs must reference stable sources:
  - code paths,
  - tests,
  - ADRs,
  - CI workflow definitions.
- Do not rely on ephemeral local output as normative proof.

## Required for “Implemented” Claims

Any implementation claim should be backed by:

1. A concrete code reference.
2. At least one test reference.
3. The CI job/command that executes that test.

## Retention Guidance

- Keep runbooks, ADRs, and policy docs in-repo.
- Store per-run logs and release evidence in CI/release systems.
- Link to durable workflow definitions rather than copying large evidence dumps into the repository.
