# Qualification-only HTTP redirect control

This is the exact object_store 0.11.2 crate, whose published archive SHA-256 is
`3cfccb68961a56facde1163f9319e0d15743352344e7808a11795fb99698dcaf`.
The upstream source change is confined to `src/client/mod.rs`: an opt-in
`ClientOptions::with_no_redirects()` sets reqwest's redirect policy to `none`.
Its default is false, preserving existing callers' behavior.

Gate 7 uses this option because reqwest can resend a conditional PUT after a
307/308 even when object_store retries are disabled. The TLS loopback qualification
probes require exactly one physical conditional PUT for either response. The
qualification client alone opts in. Upstream file hashes and the exact source patch
are retained in the external qualification evidence.
