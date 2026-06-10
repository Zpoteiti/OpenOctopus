# Reference Material

Documents ported from the prior Plexus codebase for **feature-parity checking** during the M0→M3 rebuild.

These are NOT the spec for the rebuild. The spec is `../DECISIONS.md`. These files answer the question: *"did we cover the same ground?"*

## Contents

| File | What it is | Use during rebuild |
|---|---|---|
| [current-plexus-audit.md](./current-plexus-audit.md) | Comprehensive 17-section audit of the prior Plexus codebase — crates, modules, routes, DB, tools, MCP, channels, autonomous flows, frontend. | Highest-value reference. Use as a checklist when planning each milestone to ensure nothing important is missed. |
| [old-api.md](./old-api.md) | REST API reference for prior plexus-server: endpoints, request/response shapes, auth rules. | Cross-check during M2 (server REST) — any endpoint here that we genuinely need in the rebuild should show up in our new API design. |
| [old-schema.md](./old-schema.md) | Prior PostgreSQL schema with column-by-column rationale. | Cross-check during M2 (DB). Columns explicitly dropped in the rebuild (e.g. `users.soul`, `users.memory_text`, `users.ssrf_whitelist`, `cron_jobs.kind`, `users.last_dream_at`) are noted in ADR-060 and ADR-055. |
| [old-client-tools.md](./old-client-tools.md) | Prior plexus-client tool reference: `shell`, `read_file`, `write_file`, `edit_file`, `list_dir`, `glob`, `grep`. Parameters, behaviors, edge cases. | Cross-check during M1 (client tools) — tool arg shapes are mostly nanobot-aligned already but this captures observed behavior. |
| [old-client-sandbox.md](./old-client-sandbox.md) | Prior plexus-client sandbox design: bwrap wrapping, env isolation, FsPolicy modes. | Cross-check during M1 (sandbox). |
| [superpowers/](./superpowers/) | Archived Superpowers specs and implementation plans from the old Rust milestone flow. | Historical context only. They captured the journey toward decisions now in `DECISIONS.md`; do not treat them as forward-looking implementation instructions. |

## What is NOT here and why

Deliberately not ported:

- **Old `DECISIONS.md` (~38 historical ADRs)** — fully superseded by new `DECISIONS.md`. Reversals documented in its Appendix B.
- **`DEPLOYMENT.md` (all crates)** — assumes 4-crate architecture (with gateway); obsolete per ADR-001.
- **`PROTOCOL.md`** — was specifically about browser↔gateway and gateway↔server; both go away with ADR-001 and ADR-003. Device↔server protocol types live in `plexus-common/src/protocol.rs` in the rebuild.
- **`SECURITY.md` (server)** — decisions folded into ADRs (auth in ADR-004, unrestricted in ADR-051, SSRF in ADR-052). Prose was redundant.
- **`ISSUE.md` (all crates)** — old work-tracking, not design material.

## How to use during the rebuild

When writing a new spec or implementing a milestone:

1. Check `DECISIONS.md` first — that's the authority.
2. Skim the relevant reference doc here to see what shape the prior implementation landed on.
3. If the rebuild deviates from the reference, make sure an ADR in `DECISIONS.md` justifies why.
4. If the rebuild silently matches the reference, that's fine — many behaviors are nanobot-aligned and worth preserving.

**Do NOT copy code from the reference implementation** — the reference branch (`M3-gateway-frontend`) is available via `git checkout` if you need to inspect old code, but the rebuild uses different module boundaries and different types. Copy-pasting old code is the surest way to import the complexity the rebuild is meant to shed.
