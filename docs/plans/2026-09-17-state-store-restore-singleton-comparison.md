# Step 3 compact singleton comparison execution contract

Source: 444188e21e2afcc61d402ab986c69827bd2113d6. Rust 1.88,
locked offline dependencies, sequential owned target, incremental/debug disabled.
The authority-8 native advancement flag remains disabled; production format 7
and restore-plan 6 are unchanged.

Implement private comparison preparation through the existing immutable receipt,
progress, selector CAS and exact-candidate reconciliation path. Freeze the three
comparison edges None -> CompareSource -> CompareCurrent -> Emit. Every edge
holds the entire merge cursor fixed and writes no data output. Emit remains
unsupported until its separately verified output encoder is implemented.

Each invocation authenticates selected-plan roots and the next pending key,
reads at most one singleton payload via the existing physical admission, hashes
exact opaque value bytes through the admitted hash counter, and drops decoded
rows before returning a compact generation/tombstone/length/digest and immutable
physical descriptor/index/block witness. Empty present and deleted values remain
distinguishable. Reject a physical sequence newer than its pinned role.
Previously recorded observations must match reread observations; null witnesses
are only treated as absent after rooted exclusion. Comparison state records do
not independently establish historical prefix coverage or HEAD authority.

Reuse frozen one-payload limits (P <= 64 MiB, scratch <= 64 MiB + 24P), original
control budget, guarded allocation and stop-on-error/cancellation semantics.
Metadata traversal is charged. Only authenticated singleton boundaries may avoid
payload decode; multirow boundaries require the standard path or typed refusal.
No unbounded fallback, payload clone, new wire format, or dependency.

Compiled behavioral red precedes implementation. Verify two different 40 MiB
values across restarted invocations, exact persisted phase/cursor/counts and
compact retained ownership, opaque-byte hashing, tombstone/empty distinction,
root/sequence/tampered observation refusal and existing CAS/recovery behavior.
Retain failures, command exits and source/tool manifests. Full mixed-block
execution, singleton Emit/final verification, whole workflow ownership and HEAD
publication remain Step 3 obligations, not claims of this component.
