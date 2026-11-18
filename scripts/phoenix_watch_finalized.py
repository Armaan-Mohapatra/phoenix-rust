#!/usr/bin/env python3
"""Fail-closed, paper-only finalized-segment watcher for Phoenix Phase 20.

This is a READ-ONLY source consumer, not a network collector or a live paper-order
launcher. It creates only catalog/derived/registry output beneath PhoenixLab.
Never copies, renames, compresses or deletes original TradingNext segments.
Use --execute to run; without it, prints a plan and writes NOTHING.

Python 3.11+, stdlib. No credentials, exchange API or order routes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

SEGMENT_RE = re.compile(r"^segment-(\d+)\.ready$")
REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
GIB = 1024 ** 3
SCHEMA = "phoenix-finalized-watcher-v1"


def fail(message: str) -> None:
    raise RuntimeError(message)


def no_links(path: Path) -> None:
    """Refuse symlinks/junctions on all existing ancestors BEFORE resolving."""
    absolute = path.absolute()
    for p in (*reversed(absolute.parents), absolute):
        try:
            s = p.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(s.st_mode) or getattr(s, "st_file_attributes", 0) & REPARSE_POINT:
            fail(f"refusing symlink or junction: {p}")


def within(candidate: Path, parent: Path) -> bool:
    try:
        return os.path.commonpath((os.path.normcase(str(candidate)),
                                   os.path.normcase(str(parent)))) == os.path.normcase(str(parent))
    except ValueError:
        return False


def validate_paths(source: Path, lab: Path, source_allow: Path) -> tuple[Path, Path]:
    for p in (source, lab, source_allow):
        no_links(p)
    source = source.resolve(strict=True)
    source_allow = source_allow.resolve(strict=True)
    lab = lab.resolve()
    if not source.is_dir() or not within(source, source_allow):
        fail("source is not a directory inside the explicit permitted READ-ONLY source prefix")
    if within(lab, source_allow) or within(source_allow, lab):
        fail("research workspace must be separate from and must not contain the source tree")
    for word in ("tradingnextdata", "kalshidata", "paper15m_runtime", "production"):
        if word in (p.lower() for p in lab.parts):
            fail("refusing production-looking workspace")
    return source, lab


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True)
class Ready:
    id: int
    path: Path
    sha: str
    length: int
    events: int


def ready_segments(root: Path, min_age_s: int) -> list[Ready]:
    """One-level listing only, no open segments, no event-data reads."""
    now = time.time()
    result = []
    for child in root.iterdir():
        if not (m := SEGMENT_RE.fullmatch(child.name)):
            continue
        no_links(child)
        if not child.is_dir():
            continue
        manifest = child / "manifest.json"
        event_file = child / "events.bin"
        if not manifest.is_file() or not event_file.is_file():
            continue
        no_links(manifest)
        no_links(event_file)
        ms, es = manifest.stat(), event_file.stat()
        if now - max(ms.st_mtime, es.st_mtime) < min_age_s:
            continue
        if ms.st_size > 1024 * 1024:
            fail(f"oversized source manifest: {manifest}")
        raw = json.loads(manifest.read_text(encoding="utf-8"))
        i = int(m.group(1))
        if int(raw["segment_id"]) != i:
            fail(f"directory/manifest segment ID mismatch at {child}")
        length = int(raw["file_bytes"])
        count = int(raw["event_count"])
        sha = str(raw["sha256"])
        if length != es.st_size or count < 0 or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha.lower()):
            fail(f"invalid finalized manifest or size mismatch: {child}")
        result.append(Ready(i, child, sha.lower(), length, count))
    result.sort(key=lambda x: x.id)
    ids = [x.id for x in result]
    if len(ids) != len(set(ids)):
        fail("duplicate segment IDs")
    return result


def read_state(path: Path) -> dict:
    if not path.exists():
        return {"schema": SCHEMA, "last_id": None, "last_sha": None,
                "runs": [], "initial_history_excluded": False}
    no_links(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema") != SCHEMA or not isinstance(data.get("runs"), list):
        fail("checkpoint schema mismatch; refusing to reset history")
    return data


def save_json_atomic(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    no_links(path.parent)
    temp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with temp.open("x", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def select_batch(ready: list[Ready], state: dict, batch_max: int,
                 bootstrap_last: int, lookback: int) -> tuple[list[Ready], list[Ready]]:
    if not ready:
        return [], []
    last_id = state["last_id"]
    by_id = {s.id: s for s in ready}
    if last_id is not None:
        prior = by_id.get(last_id)
        if prior is None or prior.sha != state["last_sha"]:
            fail("last processed immutable segment is missing or its manifest changed")
        new = [s for s in ready if s.id > last_id]
    else:
        new = ready[-bootstrap_last:] if bootstrap_last else ready[:]
    chosen = new[:batch_max]
    if not chosen:
        return [], []
    if last_id is not None and chosen[0].id != last_id + 1:
        fail(f"source sequence discontinuity after {last_id}; no silent skip")
    for a, b in zip(chosen, chosen[1:]):
        if b.id != a.id + 1:
            fail(f"source gap between {a.id} and {b.id}; no silent skip")
    # Preserve warm-up context but never pretend this window proves a continuous feed.
    start = max(ready[0].id, chosen[0].id - lookback)
    context = [s for s in ready if start <= s.id <= chosen[-1].id]
    if len(context) != context[-1].id - context[0].id + 1:
        fail("missing finalized segment in selected lookback window")
    return chosen, context


def tree_bytes(root: Path) -> int:
    """Bounded inspection of one newly produced run directory only."""
    if not root.exists():
        return 0
    total = 0
    for parent, dirs, files in os.walk(root, followlinks=False):
        if len(Path(parent).relative_to(root).parts) > 3:
            fail("unexpected deep output directory")
        for sub in dirs:
            no_links(Path(parent) / sub)
        for name in files:
            p = Path(parent) / name
            no_links(p)
            total += p.stat().st_size
    return total


def run_guarded(command: list[str], output_dirs: list[Path], lab: Path,
                reserved_bytes: int, prior_bytes: int, output_budget: int,
                timeout_seconds: int) -> None:
    """Terminate child on timeout/disk pressure. No shell, and no source writes."""
    with subprocess.Popen(command, shell=False, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, bufsize=1) as child:
        start = time.monotonic()
        # Redirect stdout to an independent bounded pipe reader? stdout of these tools
        # is small, but a full pipe can block. Use DEVNULL; status is in exit code/output.
        # This loop is intentionally not used with chatty executables.
        while child.poll() is None:
            used = sum(tree_bytes(p) for p in output_dirs)
            free = shutil.disk_usage(lab).free
            if free < reserved_bytes or prior_bytes + used > output_budget:
                child.kill()
                child.wait()
                fail("child stopped for disk budget / minimum free space")
            if time.monotonic() - start > timeout_seconds:
                child.kill()
                child.wait()
                fail("child stopped for bounded runtime timeout")
            time.sleep(0.5)
        if child.returncode:
            excerpt = child.stdout.read(4096) if child.stdout else ""
            fail(f"offline research command returned {child.returncode}: {excerpt[:1000]}")


def execute_batch(args: argparse.Namespace, ready: list[Ready], state: dict,
                  selected: list[Ready], context: list[Ready]) -> dict:
    lab: Path = args.workspace
    binary: Path = args.tnevent
    if not binary.is_file():
        fail("compiled phoenix-tnevent binary missing; run local Rust validation/build first")
    if shutil.disk_usage(lab).free < args.min_free_bytes:
        fail("free disk space below configured reserve")
    used_so_far = int(state.get("managed_output_bytes", 0))
    if used_so_far >= args.max_managed_bytes:
        fail("managed output budget exhausted; no auto deletion")
    ticket = f"watch-{selected[0].id:08d}-{selected[-1].id:08d}-{uuid.uuid4().hex[:8]}"
    catalog = lab / "catalog" / ticket
    derived = lab / "derived" / ticket
    catalog.parent.mkdir(parents=True, exist_ok=True)
    if args.cases:
        derived.parent.mkdir(parents=True, exist_ok=True)
    active = [catalog, derived] if args.cases else [catalog]
    commands = [
        [str(binary), "manifest", "--root", str(args.source),
         "--out", str(catalog), "--from", str(context[0].id),
         "--to", str(context[-1].id)]
    ]
    if args.cases:
        commands.append(
            [str(binary), "cases", "--root", str(args.source),
             "--manifest", str(catalog / "markets.jsonl"), "--out", str(derived),
             "--from", str(context[0].id), "--to", str(context[-1].id)]
        )
    try:
        for command in commands:
            run_guarded(command, active, lab, args.min_free_bytes,
                        used_so_far, args.max_managed_bytes, args.job_timeout)
    except Exception as error:
        save_json_atomic(lab / "registry" / "watcher-failure.json",
                         {"schema": SCHEMA, "error": str(error),
                          "ticket": ticket, "checkpoint_advanced": False,
                          "partial_outputs_retained_for_review": True})
        raise
    summary = json.loads((catalog / "summary.json").read_text(encoding="utf-8"))
    if (int(summary.get("segments", -1)) != len(context)
        or int(summary.get("segments_sha_verified", -1)) != len(context)):
        fail("source manifest failed closed: expected every segment to be SHA-verified")
    if args.cases and not (derived / "cases_summary.json").is_file():
        fail("cases proof missing; checkpoint not advanced")
    produced = sum(tree_bytes(p) for p in active)
    if used_so_far + produced > args.max_managed_bytes:
        fail("post-command managed output budget exceeded")
    s = dict(state)
    s["last_id"], s["last_sha"] = selected[-1].id, selected[-1].sha
    s["initial_history_excluded"] = s.get("initial_history_excluded", False) or (
        state["last_id"] is None and selected[0].id > ready[0].id)
    s["managed_output_bytes"] = used_so_far + produced
    record = {"ticket": ticket, "first_new_id": selected[0].id,
              "last_new_id": selected[-1].id, "warmup_from_id": context[0].id,
              "source_segments_verified": len(context), "bytes_written": produced,
              "catalog": str(catalog), "derived": str(derived) if args.cases else None,
              "paper_orders": 0, "exchange_orders": 0,
              "historical_only": True, "source_venue_result_not_independently_verified": True}
    s["runs"] = (s["runs"] + [record])[-1000:]
    save_json_atomic(lab / "registry" / "finalized-watcher.json", s)
    save_json_atomic(lab / "registry" / "finalized-watcher-status.json",
                     {"schema": SCHEMA, "latest_run": record, "state": "offline_historical_only"})
    return record


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, default=Path(r"E:\TradingNextData\kalshi\raw"))
    p.add_argument("--source-allow", type=Path, default=Path(r"E:\TradingNextData"))
    p.add_argument("--workspace", type=Path, default=Path(r"E:\PhoenixLab"))
    p.add_argument("--tnevent", type=Path, default=Path(r"E:\PhoenixLab\cache\build\target\release\phoenix-tnevent.exe"))
    p.add_argument("--execute", action="store_true", help="Without this: read-only dry run, no outputs")
    p.add_argument("--watch", action="store_true", help="Poll repeatedly; use only after local certification")
    p.add_argument("--cases", action="store_true", help="Generate derived verified cases; more disk")
    p.add_argument("--bootstrap-last", type=int, default=8,
                   help="First run only: start with last N finalized segments; mark older history excluded")
    p.add_argument("--batch-max", type=int, default=8)
    p.add_argument("--lookback", type=int, default=6)
    p.add_argument("--min-age", type=int, default=30)
    p.add_argument("--poll-seconds", type=int, default=60)
    p.add_argument("--min-free-gib", type=float, default=20.0)
    p.add_argument("--max-managed-gib", type=float, default=2.0)
    p.add_argument("--job-timeout", type=int, default=900)
    return p


def main(argv: list[str] | None = None) -> int:
    p = parser()
    args = p.parse_args(argv)
    if (args.bootstrap_last < 0 or args.batch_max < 1 or args.batch_max > 32
        or args.lookback < 1 or args.lookback > 100
        or args.min_age < 0 or args.poll_seconds < 10 or args.job_timeout < 10
        or args.min_free_gib < 1 or args.max_managed_gib <= 0):
        p.error("invalid bounds")
    args.min_free_bytes = int(args.min_free_gib * GIB)
    args.max_managed_bytes = int(args.max_managed_gib * GIB)
    try:
        args.source, args.workspace = validate_paths(args.source, args.workspace, args.source_allow)
        if args.execute:
            args.workspace.mkdir(parents=True, exist_ok=True)
        while True:
            ready = ready_segments(args.source, args.min_age)
            state = read_state(args.workspace / "registry" / "finalized-watcher.json")
            selected, context = select_batch(
                ready, state, args.batch_max, args.bootstrap_last, args.lookback)
            if selected:
                proposal = {"new_ids": [s.id for s in selected],
                            "context_ids": [s.id for s in context],
                            "action": "offline historical manifest + optional case builder",
                            "cases": args.cases, "execute": args.execute,
                            "excluded_older_history": state["last_id"] is None and selected[0].id > ready[0].id,
                            "paper_orders": 0, "live_orders": 0}
                print(json.dumps(proposal), flush=True)
                if args.execute:
                    print(json.dumps(execute_batch(args, ready, state, selected, context)), flush=True)
            else:
                print(json.dumps({"state": "no new finalized segments"}), flush=True)
            if not args.watch:
                break
            time.sleep(args.poll_seconds)
    except Exception as error:
        print(f"PHOENIX WATCHER FAIL-CLOSED: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
