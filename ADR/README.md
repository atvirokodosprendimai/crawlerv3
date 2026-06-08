# Architecture Decision Records

Log of load-bearing design choices for the crawler/registry. Read these when a choice surprises you — the "why" lives here, not in code comments.

Format: [Michael Nygard ADRs](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions). Each file: Context → Decision → Consequences. Status one of `proposed | accepted | superseded`.

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-ddd-ports-and-adapters.md) | DDD + ports & adapters layout | accepted |
| [0002](0002-cqrs-rwdb-single-writer.md) | CQRS via `rwdb` — single writer pool, many readers | accepted |
| [0003](0003-capability-strings-for-authz.md) | Capability strings for authorization + routing | accepted |
| [0004](0004-hmac-stateless-lease-tokens.md) | HMAC stateless lease tokens | accepted |
| [0005](0005-declarative-pipeline-triggers.md) | Declarative `pipeline_triggers` (replaces hardcoded MIME map) | accepted |
| [0006](0006-multi-dialect-goose-migrations.md) | Multi-dialect SQL via Goose migrations | accepted |
| [0007](0007-http-reserve-lease-result-protocol.md) | HTTP polling protocol: reserve → lease → result/fail | accepted |
| [0008](0008-internal-vs-external-processors.md) | Internal vs external processors split | accepted |
| [0009](0009-token-sized-chunks-per-collection-config.md) | Token-sized chunks + per-collection chunker config | accepted |
| [0010](0010-smoke-scripts-instead-of-unit-tests.md) | Smoke shell scripts as primary test layer | accepted |

## Adding an ADR

1. Copy the next number. Don't reuse.
2. File name: `NNNN-kebab-case-title.md`.
3. Status starts `proposed`. Flip to `accepted` once merged. Superseding ADR links the old one and flips old status to `superseded`.
4. Update this index.
5. Keep it short — one screen if possible. The decision matters; the prose doesn't.
