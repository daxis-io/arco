# Claude Code Guidelines for Arco

## Commit Rules

**CRITICAL: Never include Claude Code attribution in commits.**

When creating git commits:
- Do NOT add "Generated with Claude Code" footer
- Do NOT add "Co-Authored-By: Claude" lines
- Do NOT reference Claude, Anthropic, or AI assistance in commit messages
- Commit messages should be clean, professional, and attribution-free

Example of what NOT to do:
```
feat: add new feature

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

Example of correct commit:
```
feat: add new feature

Implements X functionality with Y approach.
```

## Project Context

Arco is a serverless lakehouse infrastructure project with:
- Rust 1.85+ (edition 2024)
- Protobuf contracts with Buf linting
- Multi-tenant storage isolation
- Two-tier consistency model (Tier 1: strong DDL, Tier 2: eventual events)

## Testing

- Run `cargo test --workspace` to verify all tests pass
- Run `cargo clippy --workspace` for linting
- Run `buf lint` for proto validation
