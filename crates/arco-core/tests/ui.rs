//! Compile-negative tests for storage trait separation (Gate 5).
//!
//! These tests verify that the split storage traits enforce capability-based
//! access at compile time. Code that attempts to use operations not granted
//! by the available traits should fail to compile.

#[test]
fn trait_separation_compile_failures() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
