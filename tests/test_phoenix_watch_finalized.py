"""Unit tests for bounded finalized watcher; never touch original production data."""
import importlib.util
import json
import os
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "phoenix_watch_finalized.py"
spec = importlib.util.spec_from_file_location("watch", SCRIPT)
watch = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = watch
spec.loader.exec_module(watch)


def segment(root, i, raw=b"test", sha="a" * 64):
    d = root / f"segment-{i:08d}.ready"
    d.mkdir()
    (d / "events.bin").write_bytes(raw)
    (d / "manifest.json").write_text(json.dumps({
        "segment_id": i, "event_count": 1,
        "file_bytes": len(raw), "sha256": sha,
    }))
    return d


class WatcherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.lab = self.base / "lab"
        self.lab.mkdir()
        for i in range(1, 13):
            segment(self.source, i)
        self.ready = watch.ready_segments(self.source, min_age_s=0)

    def state(self):
        return {"schema": watch.SCHEMA, "last_id": None, "last_sha": None,
                "runs": [], "initial_history_excluded": False}

    def test_dry_run_creates_nothing(self):
        lab = self.base / "nonexistent-lab"
        self.assertEqual(watch.main(["--source", str(self.source),
            "--source-allow", str(self.source),
            "--workspace", str(lab), "--bootstrap-last", "2",
            "--min-age", "0"]), 0)
        self.assertFalse(lab.exists())

    def test_incremental_bootstrap_and_context(self):
        selected, ctx = watch.select_batch(self.ready, self.state(), 8, 2, 3)
        self.assertEqual([s.id for s in selected], [11, 12])
        self.assertEqual([s.id for s in ctx], [8, 9, 10, 11, 12])

    def test_continuation_does_not_repeat_old_segments(self):
        state = self.state()
        state.update(last_id=10, last_sha="a" * 64)
        selected, ctx = watch.select_batch(self.ready, state, 8, 2, 4)
        self.assertEqual([s.id for s in selected], [11, 12])
        self.assertEqual(ctx[0].id, 7)

    def test_mutation_of_checkpointed_segment_fails(self):
        s = self.state()
        s.update(last_id=10, last_sha="b" * 64)
        with self.assertRaisesRegex(RuntimeError, "changed"):
            watch.select_batch(self.ready, s, 8, 2, 4)

    def test_gap_fails_closed(self):
        (self.source / "segment-00000011.ready").rename(self.source / "quarantined-11")
        ready = watch.ready_segments(self.source, 0)
        s = self.state()
        s.update(last_id=10, last_sha="a" * 64)
        with self.assertRaisesRegex(RuntimeError, "discontinuity"):
            watch.select_batch(ready, s, 8, 2, 4)

    def test_symlink_refused(self):
        link = self.base / "linked-source"
        try:
            link.symlink_to(self.source, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks not supported")
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            watch.no_links(link)

    def test_workspace_inside_source_refused(self):
        with self.assertRaisesRegex(RuntimeError, "separate"):
            watch.validate_paths(self.source, self.source / "inside", self.source)

    def test_source_outside_permitted_prefix_refused(self):
        other = self.base / "other"
        other.mkdir()
        with self.assertRaisesRegex(RuntimeError, "permitted"):
            watch.validate_paths(self.source, self.lab, other)

    def test_oversized_or_wrong_manifest_fails(self):
        p = self.source / "segment-00000001.ready" / "manifest.json"
        record = json.loads(p.read_text())
        record["segment_id"] = 999
        p.write_text(json.dumps(record))
        with self.assertRaisesRegex(RuntimeError, "mismatch"):
            watch.ready_segments(self.source, 0)

    def test_execute_checkpoints_only_after_verified_outputs(self):
        state = self.state()
        chosen, context = watch.select_batch(self.ready, state, 8, 2, 3)
        fakebin = self.base / "phoenix-tnevent"
        fakebin.touch()
        args = SimpleNamespace(workspace=self.lab, tnevent=fakebin,
            source=self.source, min_free_bytes=1, max_managed_bytes=1024 * 1024,
            cases=True, job_timeout=10)
        def simulated_completed(cmd, output_dirs, *unused):
            target = Path(cmd[cmd.index("--out") + 1])
            target.mkdir(parents=True)
            if cmd[1] == "manifest":
                (target / "summary.json").write_text(json.dumps({
                    "segments": len(context), "segments_sha_verified": len(context)}))
                (target / "markets.jsonl").write_text("")
            else:
                (target / "cases_summary.json").write_text('{"cases_written":0}')
        with patch.object(watch, "run_guarded", simulated_completed):
            report = watch.execute_batch(args, self.ready, state, chosen, context)
        self.assertEqual(report["paper_orders"], 0)
        checkpoint = watch.read_state(self.lab / "registry" / "finalized-watcher.json")
        self.assertEqual(checkpoint["last_id"], 12)
        self.assertTrue(checkpoint["initial_history_excluded"])

    def test_missing_proof_never_advances_checkpoint(self):
        state = self.state()
        chosen, context = watch.select_batch(self.ready, state, 8, 2, 3)
        fakebin = self.base / "phoenix-tnevent"
        fakebin.touch()
        args = SimpleNamespace(workspace=self.lab, tnevent=fakebin,
            source=self.source, min_free_bytes=1, max_managed_bytes=1024 * 1024,
            cases=False, job_timeout=10)
        def invalid_output(cmd, output_dirs, *unused):
            target = Path(cmd[cmd.index("--out") + 1])
            target.mkdir(parents=True)
            (target / "summary.json").write_text('{"segments":0,"segments_sha_verified":0}')
        with patch.object(watch, "run_guarded", invalid_output):
            with self.assertRaisesRegex(RuntimeError, "SHA-verified"):
                watch.execute_batch(args, self.ready, state, chosen, context)
        self.assertFalse((self.lab / "registry" / "finalized-watcher.json").exists())

    def test_disk_limit_prevents_execution(self):
        state = self.state()
        chosen, context = watch.select_batch(self.ready, state, 8, 2, 3)
        fakebin = self.base / "phoenix-tnevent"
        fakebin.touch()
        args = SimpleNamespace(workspace=self.lab, tnevent=fakebin,
            source=self.source, min_free_bytes=10 ** 18,
            max_managed_bytes=1000, cases=False, job_timeout=10)
        with self.assertRaisesRegex(RuntimeError, "free disk"):
            watch.execute_batch(args, self.ready, state, chosen, context)

    def test_second_instance_refused_and_stale_lock_recovered(self):
        lock = watch.acquire_lock(self.lab, stale_s=60)
        with self.assertRaisesRegex(RuntimeError, "another watcher"):
            watch.acquire_lock(self.lab, stale_s=60)
        old = time.time() - 3600
        os.utime(lock, (old, old))
        self.assertEqual(watch.acquire_lock(self.lab, stale_s=60), lock)

    def test_source_mutated_after_listing_detected(self):
        ev = self.source / "segment-00000012.ready" / "events.bin"
        ev.write_bytes(b"tamper!!")
        with self.assertRaisesRegex(RuntimeError, "changed during ingestion"):
            watch.assert_unchanged(self.ready)

    def test_abandoned_outputs_count_against_quota(self):
        junk = self.lab / "catalog" / "watch-00000001-00000002-dead"
        junk.mkdir(parents=True)
        (junk / "partial.bin").write_bytes(b"x" * 5000)
        self.assertEqual(watch.managed_bytes(self.lab), 5000)
        state = self.state()
        chosen, context = watch.select_batch(self.ready, state, 8, 2, 3)
        fakebin = self.base / "phoenix-tnevent"
        fakebin.touch()
        args = SimpleNamespace(workspace=self.lab, tnevent=fakebin, source=self.source,
            min_free_bytes=1, max_managed_bytes=4000, cases=False, job_timeout=10)
        with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
            watch.execute_batch(args, self.ready, state, chosen, context)

    def test_chain_detects_mutation_of_old_ingested_segment(self):
        s = self.state()
        s.update(last_id=10, last_sha="a" * 64, processed={"3": "c" * 64})
        with self.assertRaisesRegex(RuntimeError, "previously ingested segment 3"):
            watch.select_batch(self.ready, s, 8, 2, 4)

    def test_chain_and_processed_recorded(self):
        state = self.state()
        chosen, context = watch.select_batch(self.ready, state, 8, 2, 3)
        fakebin = self.base / "phoenix-tnevent"
        fakebin.touch()
        args = SimpleNamespace(workspace=self.lab, tnevent=fakebin, source=self.source,
            min_free_bytes=1, max_managed_bytes=1024 * 1024, cases=False, job_timeout=10)
        def ok(cmd, *unused):
            t = Path(cmd[cmd.index("--out") + 1]); t.mkdir(parents=True)
            (t / "summary.json").write_text(json.dumps({
                "segments": len(context), "segments_sha_verified": len(context)}))
        with patch.object(watch, "run_guarded", ok):
            rec = watch.execute_batch(args, self.ready, state, chosen, context)
        cp = watch.read_state(self.lab / "registry" / "finalized-watcher.json")
        self.assertEqual(sorted(cp["processed"]), ["11", "12"])
        self.assertEqual(rec["chain_head"], cp["chain"])
        self.assertEqual(rec["mode"], "HISTORICAL_ONLY")

    def test_corrupt_checkpoint_fails_closed(self):
        p = self.lab / "registry" / "finalized-watcher.json"
        p.parent.mkdir()
        p.write_text('{"schema": "phoenix-finalized-watcher-v1", "runs": [')  # half-written
        with self.assertRaises(ValueError):
            watch.read_state(p)

    @unittest.skipUnless(os.name == "nt", "NTFS junction test")
    def test_junction_refused(self):
        import subprocess
        link = self.base / "junction-source"
        r = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(self.source)],
                           capture_output=True)
        if r.returncode:
            self.skipTest("cannot create junction")
        with self.assertRaisesRegex(RuntimeError, "symlink or junction"):
            watch.no_links(link)

    def test_chatty_child_does_not_deadlock_and_failure_is_reported(self):
        script = self.base / "chatty.py"
        script.write_text("import sys\nsys.stdout.write('x' * 5_000_000)\nsys.exit(3)\n")
        with self.assertRaisesRegex(RuntimeError, "returned 3"):
            watch.run_guarded([sys.executable, str(script)], [], self.lab,
                              1, 0, 1 << 30, 30)

    def test_child_killed_on_timeout(self):
        script = self.base / "hang.py"
        script.write_text("import time\ntime.sleep(60)\n")
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            watch.run_guarded([sys.executable, str(script)], [], self.lab,
                              1, 0, 1 << 30, 1)


if __name__ == "__main__":
    unittest.main()
