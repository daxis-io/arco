# Contributing to Arco

Thanks for contributing. Arco is an Apache-2.0 project that values clear proposals, reproducible verification, and respectful review.

## Ground Rules

- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- Keep changes scoped and reviewable.
- Add or update tests for behavior changes.
- Update documentation when public behavior or interfaces change.
- Sign commits with DCO: `git commit -s`.

## Development Setup

### Prerequisites

- Rust 1.85+
- `protoc`
- `mdbook` (for docs)

### Bootstrap

```bash
git clone https://github.com/daxis-io/arco.git
cd arco
cargo check --workspace --all-features
```

## Contribution Workflow

1. Open an issue or discussion for non-trivial changes.
2. Create a branch from `main`.
3. Implement changes with tests.
4. Run local verification:
   ```bash
   cargo fmt --check
   cargo clippy --workspace --all-features -- -D warnings
   cargo xtask adr-check
   cargo xtask parity-matrix-check
   cargo check --workspace --all-features
   cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
   ```
5. Build docs:
   ```bash
   cd docs/guide && mdbook build
   ```
6. Open a pull request with summary, motivation, risk, and verification notes.

## Pull Request Expectations

- CI must pass before merge.
- API, schema, or contract changes must include migration notes.
- Architectural changes should reference an ADR in `docs/adr/`.
- Keep PRs focused; split unrelated work.

## Commit Conventions

Use conventional commits where practical:

```text
feat(scope): short summary
fix(scope): short summary
docs(scope): short summary
```

## Documentation Standards

- `docs/guide/` is the canonical user documentation surface.
- ADRs in `docs/adr/` are canonical for architecture decisions.
- Do not treat transient local artifacts as normative references.

## Security Reports

Do not open public issues for suspected vulnerabilities. Follow [SECURITY.md](SECURITY.md).

## Getting Help

- Usage and design questions: [COMMUNITY.md](COMMUNITY.md)
- Support channels: [SUPPORT.md](SUPPORT.md)
