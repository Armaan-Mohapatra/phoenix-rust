# Optional local Codex and Claude Code token-efficiency setup

This file is instructional only. It makes **no changes** to your Windows computer or active Phoenix workers. Install only on the machine running the actual coding agent, after reviewing software and backing up existing settings.

## Measure local token records (Node.js required)

```powershell
node --version
npm --version
npx --yes ccusage@latest codex daily
npx --yes ccusage@latest claude daily
npx --yes ccusage@latest session
```

These estimates are not account-wide subscription quota figures.

## Optional RTK terminal-output compression

```powershell
winget install --id rtk-ai.rtk --exact
```

In a new terminal, **preview** user-level changes first:

```powershell
rtk --version
rtk init --global --dry-run
rtk init --global --codex --dry-run
```

Only after reviewing compatibility, any hook conflicts, and a rollback plan, enable:

```powershell
rtk init --global
rtk init --global --codex
rtk gain
```

Test on harmless commands outside production folders. Inspect uncompressed original logs for safety-critical failures. Do not silently overwrite hooks.

## Do not turn on by default
Caveman cache keepalive, cloud model routing, transparent third-party proxies, recurring AI background tasks, autonomous Goals, agent teams, paid CI, or simultaneous compression layers.

Benchmark complete, correctly verified tasks including retries and full required test evidence. Remove tools that reduce reliability.

Sources: https://github.com/rtk-ai/rtk and https://github.com/ccusage/ccusage .
