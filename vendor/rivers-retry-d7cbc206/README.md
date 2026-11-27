# Pinned Rivers retry source

This private package carries unmodified `execution/retry.rs` and `execution/compute.rs` from Rivers revision `d7cbc2065554093b0ddddc40cd0fc3174d79324c`. `SOURCE.json` records paths and SHA-256 identities; `LICENSE` and `NOTICE` retain upstream attribution. `lib.rs` preserves the module path referenced by retry metadata.

Arco uses the original `should_retry` and `compute_delay` functions also used by Rivers' executors. The compatibility profile lives in Arco, not in these source files. There is no scheduler, storage backend or execution loop in this package.

The package is a temporary extraction until a supported upstream library surface exists. Updating the pin requires copying the exact source and notices, updating hashes and the accepted retry profile identity when semantics change, running upstream module tests and Arco recovery tests, and retaining a compatible reader for already accepted plans. Do not silently replace behavior behind an existing profile ID.

Rust 1.88 validation covers these modules only; the full Rivers workspace has its own toolchain requirement.
