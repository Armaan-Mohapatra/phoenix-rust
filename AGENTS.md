# Phoenix Rust agent instructions

## Authority and safety
- Follow `SECURITY.md`, `README.md`, relevant active-phase documents, and `docs/RELEASE_GATES.md` before material changes.
- No live exchange orders, credentials, real-money execution, or changes to active paper/collector services.
- Preserve historical data, data provenance, replay causality, settlement evidence, paper/live boundaries, and immutable records.
- Do not delete, compress, or rewrite real data or read active production SQLite/WAL files without explicit approval.
- Do not assume synthetic test success proves live readiness, real venue integrity, or profitability.
- No paid GitHub Actions, merges, deployments, or Windows task changes without separate permission.

## Token-efficient, bounded work
- Execute only the specifically authorized task/phase; stop at the acceptance checkpoint.
- No new Goals, subagents, parallel agent teams, autonomous continuations, or broad refactors by default.
- Use targeted symbol search and file ranges before opening full source trees or large logs.
- Read required documents, but do not repeatedly re-read unchanged content in one task.
- Run targeted tests first and the full required suite at release gates. Preserve full original failure logs and evidence.
- Report only changed files, full SHA, test evidence, unresolved blockers, and one next step. Never save tokens by weakening safety or validation.
- `docs/AI_HANDOFF.md` is a navigation template, not evidence. `docs/AI_TOOLING_SETUP.md` is optional guidance, not an installation.
