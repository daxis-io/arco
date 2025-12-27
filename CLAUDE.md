# Claude Code Guidelines for Arco

## Superpowered Workflow (OpenCode Skills & Agents)

This repo includes a multi-model, multi-agent engineering pipeline in `.opencode/`. Use these for professional-grade development.

### Quick Start

1. **For complex tasks**, load the superpowered workflow:
   ```
   /skill superpowered-workflow
   ```

2. **Or switch to the superpowered agent** for full orchestration:
   ```
   /agent superpowered
   ```

### Available Agents

| Agent | Model | Purpose |
|-------|-------|---------|
| `superpowered` | Claude Sonnet 4.5 | Full orchestrator: planning → implementation → review → fix → sign-off |
| `committee-claude` | Claude Sonnet 4.5 | Architecture/taste critic for planning discussions |
| `committee-chatgpt` | GPT-5.1 | Structured specs and acceptance criteria for planning |
| `claude-explorer` | Claude Sonnet 4.5 | Deep codebase analysis and architecture understanding |
| `chatgpt-explorer` | GPT-5.1 | Systematic codebase exploration with structured output |
| `claude-implementer` | Claude Sonnet 4.5 | First-pass implementation |
| `openai-reviewer` | GPT-5.1 | Read-only code review |
| `openai-fixer` | GPT-5.1 Codex | Convert review feedback into fix plans and apply patches |
| `verifier` | Claude Sonnet 4.5 | Run checks and summarize results |

### Available Skills

| Skill | Purpose |
|-------|---------|
| `superpowered-workflow` | End-to-end multi-agent pipeline |
| `professional-engineering-standards` | Definition-of-done baseline |
| `committee-planning` | Multi-model committee planning |
| `claude-first-pass` | Claude implementation pass |
| `openai-review-feedback` | OpenAI review process |
| `openai-feedback-execute` | Execute fixes from review |
| `tdd-red-green-refactor` | Test-driven development |
| `systematic-debugging` | Structured debugging approach |
| `rust-quality-gates` | Rust-specific quality checks |
| `fullstack-quality-gates` | Full-stack quality checks |
| `security-dependency-hygiene` | Security and dependency audits |
| `adr-decision-records` | Architecture decision records |
| `git-worktree-manager` | Git worktree management |
| `rollback-and-regression-control` | Safe rollback procedures |
| `brainstorm-requirements` | Requirements gathering |
| `committee-disagreement-resolution` | Resolve committee conflicts |
| `finalize-and-handoff` | Final handoff procedures |

### Workflow Complexity Classification

- **SIMPLE** (1-2 files, <50 lines): Direct implementation, skip committee
- **MEDIUM** (3-5 files, 50-200 lines): Abbreviated planning, single reviewer
- **COMPLEX** (5+ files, >200 lines): Full committee planning and multi-stage review

---

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
