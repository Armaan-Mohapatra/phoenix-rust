#!/usr/bin/env python3
"""Phoenix storage tooling: bounded inventory, duplicate analysis, staged cleanup.

Stdlib only. Read-only except `cleanup --apply`, which only *moves* files that are
inside the PhoenixLab workspace into its reversible trash (never deletes, never
touches anything outside the workspace).

  python scripts/phoenix_storage.py inventory ROOT... --out DIR
  python scripts/phoenix_storage.py dupes --out DIR [--min-bytes N]
  python scripts/phoenix_storage.py cleanup --workspace E:\\PhoenixLab [--apply]
  python scripts/phoenix_storage.py selftest
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import stat
import sys
import time
from collections import defaultdict
from pathlib import Path

CLUSTER = 4096
REPARSE = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
COMPRESSED_OR_SPARSE = 0x800 | 0x200

EXT = {
    ".sqlite": "sqlite", ".sqlite3": "sqlite", ".db": "sqlite", ".duckdb": "sqlite",
    ".parquet": "parquet",
    ".json": "json", ".jsonl": "json", ".ndjson": "json",
    ".zip": "archive", ".7z": "archive", ".tar": "archive", ".tgz": "archive",
    ".gz": "archive", ".zst": "archive", ".xz": "archive", ".bz2": "archive",
    ".log": "log",
    ".csv": "csv",
    ".tmp": "temp", ".partial": "temp",
}
TEMP_DIRS = {"temp", "tmp", "scratch", "benchmark-tmp", ".tmp", "__pycache__"}


def category_of_file(name: str) -> str:
    low = name.lower()
    for suffix in ("-wal", "-shm", "-journal"):
        if low.endswith(suffix):
            return "sqlite"
    for compound in (".jsonl.gz", ".jsonl.zst", ".json.gz", ".ndjson.gz"):
        if low.endswith(compound):
            return "json"
    return EXT.get(os.path.splitext(low)[1], "other")


def context_of_dir(path: str, name: str) -> str | None:
    """A directory whose whole subtree belongs to one category."""
    low = name.lower()
    if low == "node_modules" or os.path.exists(os.path.join(path, "pyvenv.cfg")):
        return "env"
    if low == "target" and (os.path.exists(os.path.join(path, ".rustc_info.json"))
                            or os.path.exists(os.path.join(path, "CACHEDIR.TAG"))):
        return "rust_build"
    if low.endswith("cargo-cache") or low in (".cargo", ".rustup"):
        return "cargo_cache"
    if low in TEMP_DIRS or low.startswith(".phoenix_dataset_build_"):
        return "temp"
    if low == "logs":
        return "log"
    return None


def allocated(path: str, st: os.stat_result) -> int:
    attrs = getattr(st, "st_file_attributes", 0)
    if os.name == "nt" and attrs & COMPRESSED_OR_SPARSE:
        high = ctypes.c_ulong(0)
        low = ctypes.windll.kernel32.GetCompressedFileSizeW(ctypes.c_wchar_p(path), ctypes.byref(high))
        if low != 0xFFFFFFFF:
            return (high.value << 32) + low
    return -(-st.st_size // CLUSTER) * CLUSTER  # shortcut: ignores MFT-resident tiny files, ok at GB scale


def inventory(roots: list[str], out: Path, depth: int, min_file: int) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    tree: dict[str, dict] = {}
    totals: dict[str, dict] = defaultdict(lambda: {"bytes": 0, "alloc": 0, "files": 0})
    skipped: list[dict] = []
    big = open(out / "files.jsonl", "w", encoding="utf-8")
    started = time.time()

    def node(p: str) -> dict:
        n = tree.get(p)
        if n is None:
            n = tree[p] = {"bytes": 0, "alloc": 0, "files": 0, "newest": 0.0, "cats": defaultdict(int)}
        return n

    for root in roots:
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            skipped.append({"path": root, "reason": "missing"})
            continue
        stack = [(root, [root], None)]  # (dir, ancestors-within-depth, inherited context)
        while stack:
            d, anc, ctx = stack.pop()
            try:
                it = list(os.scandir(d))
            except OSError as e:
                skipped.append({"path": d, "reason": type(e).__name__})
                continue
            for e in it:
                try:
                    st = e.stat(follow_symlinks=False)
                except OSError as ex:
                    skipped.append({"path": e.path, "reason": type(ex).__name__})
                    continue
                if e.is_symlink() or getattr(st, "st_file_attributes", 0) & REPARSE:
                    skipped.append({"path": e.path, "reason": "reparse_point_not_followed"})
                    continue
                if stat.S_ISDIR(st.st_mode):
                    sub_anc = anc + [e.path] if len(anc) <= depth else anc
                    stack.append((e.path, sub_anc, ctx or context_of_dir(e.path, e.name)))
                    continue
                cat = ctx or category_of_file(e.name)
                al = allocated(e.path, st)
                for a in anc:
                    n = node(a)
                    n["bytes"] += st.st_size
                    n["alloc"] += al
                    n["files"] += 1
                    n["cats"][cat] += st.st_size
                    n["newest"] = max(n["newest"], st.st_mtime)
                t = totals[cat]
                t["bytes"] += st.st_size
                t["alloc"] += al
                t["files"] += 1
                if st.st_size >= min_file:
                    big.write(json.dumps({"path": e.path, "size": st.st_size, "mtime": st.st_mtime, "cat": cat}) + "\n")
    big.close()
    catalog = {
        "schema": 1,
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "roots": roots,
        "depth": depth,
        "min_file_bytes_listed": min_file,
        "elapsed_s": round(time.time() - started, 1),
        "notes": ["alloc = cluster-rounded size, or GetCompressedFileSizeW for NTFS-compressed/sparse files",
                  "reparse points (junctions/symlinks) are not followed; hardlinks are counted once per path"],
        "totals_by_category": dict(totals),
        "dirs": {p: {**v, "cats": dict(v["cats"]),
                     "newest": time.strftime("%Y-%m-%d", time.localtime(v["newest"])) if v["newest"] else None}
                 for p, v in sorted(tree.items())},
        "skipped": skipped[:500],
        "skipped_count": len(skipped),
    }
    (out / "catalog.json").write_text(json.dumps(catalog, indent=1), encoding="utf-8")
    return catalog


def sha256_of(path: str, limit: int | None = None) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        if limit is None:
            while chunk := f.read(1 << 22):
                h.update(chunk)
        else:  # head + tail fingerprint, prefilter only
            h.update(f.read(limit))
            size = os.fstat(f.fileno()).st_size
            if size > 2 * limit:
                f.seek(size - limit)
                h.update(f.read(limit))
    return h.hexdigest()


def dupes(out: Path, min_bytes: int) -> dict:
    by_size: dict[int, list[dict]] = defaultdict(list)
    with open(out / "files.jsonl", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r["size"] >= min_bytes:
                by_size[r["size"]].append(r)
    groups, hashed_bytes, unreadable = [], 0, []
    for size, rs in sorted(by_size.items(), reverse=True):
        if len(rs) < 2:
            continue
        pre: dict[str, list[dict]] = defaultdict(list)
        for r in rs:
            try:
                pre[sha256_of(r["path"], 1 << 16)].append(r)
            except OSError as e:
                unreadable.append({"path": r["path"], "reason": type(e).__name__})
        for cands in pre.values():
            if len(cands) < 2:
                continue
            full: dict[str, list[dict]] = defaultdict(list)
            for r in cands:
                try:
                    full[sha256_of(r["path"])].append(r)
                    hashed_bytes += size
                except OSError as e:
                    unreadable.append({"path": r["path"], "reason": type(e).__name__})
            for digest, same in full.items():
                if len(same) > 1:
                    groups.append({"sha256": digest, "size": size, "copies": len(same),
                                   "redundant_bytes": size * (len(same) - 1),
                                   "paths": sorted(r["path"] for r in same)})
    result = {"schema": 1, "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "min_bytes": min_bytes, "bytes_fully_hashed": hashed_bytes,
              "exact_duplicate_groups": len(groups),
              "exact_redundant_bytes": sum(g["redundant_bytes"] for g in groups),
              "unreadable": unreadable, "groups": groups}
    (out / "duplicates.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    return result


# ---- cleanup: only inside the workspace, only regenerable/temporary classes ----
# Retention per class lives in <workspace>/config/storage-policy.json.
DEFAULT_POLICY = {"temporary_max_age_days": 3, "regenerable_max_age_days": 30,
                  "managed": {"cache/temporary": "temporary", "cache/replay-cache": "regenerable",
                              "cache/feature-cache": "regenerable"}}


def cleanup(workspace: Path, apply: bool, now: float | None = None) -> dict:
    workspace = workspace.resolve()
    pol_path = workspace / "config" / "storage-policy.json"
    policy = json.loads(pol_path.read_text(encoding="utf-8")) if pol_path.exists() else DEFAULT_POLICY
    now = now or time.time()
    trash = workspace / "maintenance" / "trash" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now))
    actions = []
    for rel, cls in policy["managed"].items():
        base = (workspace / rel).resolve()
        if workspace not in base.parents:  # trust boundary: policy must stay inside the workspace
            raise SystemExit(f"refusing managed path outside workspace: {base}")
        if cls not in ("temporary", "regenerable"):
            raise SystemExit(f"refusing to manage class {cls!r}: only temporary/regenerable are cleanable")
        if not base.is_dir():
            continue
        max_age = policy[f"{cls}_max_age_days"] * 86400
        for p in sorted(base.rglob("*")):
            if not p.is_file() or p.is_symlink():
                continue
            if p.name.endswith((".lock", ".keep")) or (p.parent / "KEEP").exists():
                continue
            st = p.stat()
            if now - st.st_mtime < max_age:
                continue
            dest = trash / p.relative_to(workspace)
            actions.append({"path": str(p), "bytes": st.st_size, "class": cls,
                            "age_days": round((now - st.st_mtime) / 86400, 1), "staged_to": str(dest)})
            if apply:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(p), str(dest))
    log = {"schema": 1, "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
           "mode": "apply" if apply else "dry-run", "workspace": str(workspace),
           "files": len(actions), "bytes": sum(a["bytes"] for a in actions), "actions": actions}
    logdir = workspace / "maintenance" / "cleanup-plans"
    logdir.mkdir(parents=True, exist_ok=True)
    (logdir / f"cleanup-{log['mode']}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(now))}.json").write_text(
        json.dumps(log, indent=1), encoding="utf-8")
    return log


def _selftest() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as t:
        ws = Path(t)
        (ws / "cache" / "temporary").mkdir(parents=True)
        old, new = ws / "cache/temporary/old.bin", ws / "cache/temporary/new.bin"
        old.write_bytes(b"x" * 10)
        new.write_bytes(b"y" * 10)
        os.utime(old, (time.time() - 10 * 86400,) * 2)
        dry = cleanup(ws, apply=False)
        assert dry["files"] == 1 and old.exists(), dry
        done = cleanup(ws, apply=True)
        assert done["files"] == 1 and not old.exists() and new.exists()
        assert Path(done["actions"][0]["staged_to"]).exists()
        (ws / "a").mkdir()
        (ws / "a" / "x.bin").write_bytes(b"z" * 5000)
        (ws / "a" / "y.bin").write_bytes(b"z" * 5000)
        (ws / "a" / "w.bin").write_bytes(b"q" * 5000)
        cat = inventory([str(ws / "a")], ws / "out", depth=2, min_file=1)
        assert cat["dirs"][str(ws / "a")]["files"] == 3
        d = dupes(ws / "out", 1)
        assert d["exact_duplicate_groups"] == 1 and d["exact_redundant_bytes"] == 5000, d
        (ws / "config").mkdir()
        for managed in ({"../escape": "temporary"}, {"cache/temporary": "permanent"}):
            bad = {**DEFAULT_POLICY, "managed": managed}
            (ws / "config" / "storage-policy.json").write_text(json.dumps(bad))
            try:
                cleanup(ws, apply=True)
                raise AssertionError(f"not refused: {managed}")
            except SystemExit:
                pass
    print("selftest ok")


def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("inventory")
    a.add_argument("roots", nargs="+")
    a.add_argument("--out", required=True, type=Path)
    a.add_argument("--depth", type=int, default=3)
    a.add_argument("--min-file-bytes", type=int, default=1 << 20)
    b = sub.add_parser("dupes")
    b.add_argument("--out", required=True, type=Path)
    b.add_argument("--min-bytes", type=int, default=1 << 20)
    c = sub.add_parser("cleanup")
    c.add_argument("--workspace", required=True, type=Path)
    c.add_argument("--apply", action="store_true")
    sub.add_parser("selftest")
    args = ap.parse_args(argv)
    if args.cmd == "inventory":
        cat = inventory(args.roots, args.out, args.depth, args.min_file_bytes)
        print(json.dumps({k: cat[k] for k in ("elapsed_s", "totals_by_category", "skipped_count")}, indent=1))
    elif args.cmd == "dupes":
        d = dupes(args.out, args.min_bytes)
        print(json.dumps({k: d[k] for k in ("exact_duplicate_groups", "exact_redundant_bytes", "bytes_fully_hashed")}))
    elif args.cmd == "cleanup":
        log = cleanup(args.workspace, args.apply)
        print(json.dumps({k: log[k] for k in ("mode", "files", "bytes")}))
    else:
        _selftest()


if __name__ == "__main__":
    main(sys.argv[1:])
