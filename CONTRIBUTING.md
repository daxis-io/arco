# Contributing to Arco

Thank you for your interest in contributing to Arco! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Code Style](#code-style)
- [Testing](#testing)
- [Documentation](#documentation)
- [Getting Help](#getting-help)

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- **Rust**: 1.85+ (Edition 2024)
- **Protocol Buffers**: `protoc` compiler
- **Git**: With DCO sign-off capability

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/arco.git
cd arco
git remote add upstream https://github.com/daxis-io/arco.git
```

## Development Setup

```bash
# Install Rust toolchain
rustup update stable
rustup component add rustfmt clippy

# Install development tools
cargo install cargo-deny cargo-nextest cargo-llvm-cov

# Verify setup
cargo build --workspace
cargo test --workspace
```

### Editor Setup

We provide an `.editorconfig` file for consistent formatting. Install the EditorConfig plugin for your editor.

Recommended VS Code extensions:
- rust-analyzer
- EditorConfig for VS Code
- crates

## Making Changes

### Branch Naming

Use descriptive branch names:

```
feature/add-lineage-traversal
fix/catalog-reader-timeout
docs/update-contributing-guide
refactor/simplify-tenant-validation
```

### Before You Start

1. Check existing issues and PRs for related work
2. For significant changes, open an issue first to discuss
3. Ensure you understand the crate boundaries (see [architecture docs](docs/))

## Commit Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvement
- `ci`: CI/CD changes

### Scopes

Use crate names: `core`, `catalog`, `flow`, `api`, `proto`, `compactor`

### Examples

```
feat(catalog): add lineage traversal API

Implements depth-limited lineage traversal for asset discovery.
Supports both upstream and downstream traversal with configurable
depth limits.

Closes #123
```

```
fix(core): handle invalid tenant IDs gracefully

Previously, invalid tenant IDs caused a panic. Now returns
a proper error with context.
```

### DCO Sign-Off

All commits must be signed off to certify you agree to the [Developer Certificate of Origin](https://developercertificate.org/):

```bash
git commit -s -m "feat(catalog): add new feature"
```

This adds a `Signed-off-by` line to your commit message.

### Commit Template

We provide a commit template. To use it:

```bash
git config commit.template .gitmessage
```

## Pull Request Process

### Before Submitting

1. **Rebase on main**: `git fetch upstream && git rebase upstream/main`
2. **Run all checks**:
   ```bash
   cargo fmt --check
   cargo clippy --workspace --all-features -- -D warnings
   cargo test --workspace
   cargo deny check
   ```
3. **Update documentation** if you changed public APIs
4. **Add tests** for new functionality

### PR Description

Include:
- **Summary**: What does this PR do?
- **Motivation**: Why is this change needed?
- **Testing**: How was this tested?
- **Breaking Changes**: Any breaking changes?

### Review Process

1. All PRs require at least one approval
2. CI must pass (lint, test, docs, audit)
3. API changes require two approvals
4. Security-sensitive changes require security reviewer

### After Merge

- Delete your feature branch
- Update related issues

## Code Style

### Rust Guidelines

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `#![deny(missing_docs)]` in published crates
- Prefer `thiserror` for library errors, `anyhow` for binaries
- No `.unwrap()` or `.expect()` in library code
- Add context to errors at boundary crossings

### Documentation

- All public items must have documentation
- Include examples in doc comments
- Complex modules need module-level documentation

### Formatting

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt --check
```

## Testing

### Test Pyramid

| Level | Purpose | Location |
|-------|---------|----------|
| Unit | Test individual functions | `src/` (inline `#[cfg(test)]`) |
| Integration | Test cross-crate behavior | `tests/integration/` |
| Property | Test invariants | Use `proptest` |

### Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p arco-core

# With coverage
cargo llvm-cov --workspace

# Integration tests
cargo test --workspace --test '*'
```

### Test Requirements

- New features need tests
- Bug fixes need regression tests
- Aim for 80%+ coverage on new code
- No flaky tests

## Documentation

### Types of Documentation

1. **API Docs**: Inline rustdoc comments
2. **User Guide**: `docs/guide/` (mdBook)
3. **Architecture**: `docs/adr/` (ADRs)

### Building Docs

```bash
# API documentation
cargo doc --workspace --no-deps --open

# User guide (requires mdBook)
cd docs/guide && mdbook build
```

### ADR Process

For architectural decisions:

1. Copy `docs/adr/template.md`
2. Number sequentially (e.g., `adr-002-*.md`)
3. Fill in context, decision, consequences
4. Submit for review

## Getting Help

- **Questions**: Open a [GitHub Discussion](https://github.com/daxis-io/arco/discussions)
- **Bugs**: Open a [GitHub Issue](https://github.com/daxis-io/arco/issues)
- **Security**: See [SECURITY.md](SECURITY.md)
- **Real-time**: Join our [Discord](https://discord.gg/arco)

## Recognition

Contributors are recognized in:
- Release notes
- `CONTRIBUTORS.md` file
- GitHub contributor graphs

Thank you for contributing to Arco!
