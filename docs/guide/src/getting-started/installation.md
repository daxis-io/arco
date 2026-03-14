# Installation

## Prerequisites

- Rust `1.85` or newer
- `protoc` (Protocol Buffers compiler)
- `mdbook` (for documentation builds)

## Clone and Build

```bash
git clone https://github.com/daxis-io/arco.git
cd arco
cargo check --workspace --all-features
```

## Optional Tooling

```bash
cargo install cargo-deny
cargo install cargo-nextest
cargo install cargo-llvm-cov
```

## Validate Environment

```bash
cargo xtask adr-check
cargo xtask parity-matrix-check
cd docs/guide && mdbook build
```

If these commands pass, your local environment is ready for development and documentation changes.
