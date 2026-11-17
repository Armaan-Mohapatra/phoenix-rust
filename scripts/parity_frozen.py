#!/usr/bin/env python3
"""Decision parity: frozen Python strategy functions vs the Rust replicas, on identical observations.

Runs the REAL functions from the frozen source (`luna_experimental_paper.py`, SHA-256 checked) on
order-book observations sampled from authentic campaign cases, runs `phoenix-tnevent parity-eval` on the
same observations, and compares recommendation, probability and edge. Decision-only parity: it says
nothing about execution, fills or profitability. See crates/tnevent/src/frozen.rs for what is replicated.

  python scripts/parity_frozen.py --frozen luna_experimental_paper.py --cases cases.jsonl \
      --exe phoenix-tnevent.exe [--stride 40] [--max-obs 20000]
"""
import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

FROZEN_SHA256 = "362038d643fbece296debc52a677c257894d7719517e04ac377d23dfc11e4d6c"


def load_frozen(path: str):
    sha = hashlib.sha256(open(path, "rb").read()).hexdigest()
    if sha != FROZEN_SHA256:
        raise SystemExit(f"frozen source SHA-256 mismatch: {sha}")
    # The module only builds a Path from this variable at import; point it at a scratch location so the
    # production experiment database can never be touched.
    os.environ["LUNA_EXPERIMENT_DB"] = os.path.join(tempfile.gettempdir(), "phoenix_parity_nonexistent.sqlite3")
    spec = importlib.util.spec_from_file_location("luna_experimental_paper", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, sha


def observations(cases_path: str, stride: int, max_obs: int):
    n = 0
    with open(cases_path, encoding="utf-8") as f:
        for line in f:
            case = json.loads(line)
            for i, s in enumerate(case["snapshots"]):
                if i % stride == 0:
                    yield {"yes_bids": s["yes_bids"], "no_bids": s["no_bids"]}
                    n += 1
                    if n >= max_obs:
                        return


def market_dict(o):
    """The market object the frozen functions read: bids best-first, asks = complement of the opposite bids."""
    f = lambda levels: [[p / 10000.0, float(q)] for p, q in levels]
    yes_bids, no_bids = f(o["yes_bids"]), f(o["no_bids"])
    return {"book_valid": True, "book": {
        "yes_bids": yes_bids, "no_bids": no_bids,
        "yes_asks": [[1.0 - p, q] for p, q in no_bids], "no_asks": [[1.0 - p, q] for p, q in yes_bids]}}


def frozen_decision(m, mkt, p):
    quotes = m._quotes(mkt)
    if p is None:
        return {"p_yes": None, "recommendation": "SKIP", "edge": None}
    sv = m._side_inputs(p, quotes, 1.0)
    rec, _ = m._recommend(sv)
    evs = [v["net_ev_per_contract"] for v in sv.values() if v["net_ev_per_contract"] is not None and v["executable_one_contract_ask"]]
    return {"p_yes": p, "recommendation": rec, "edge": max(evs) if evs else None}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frozen", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--exe", required=True)
    ap.add_argument("--stride", type=int, default=40)
    ap.add_argument("--max-obs", type=int, default=20000)
    ap.add_argument("--out", help="write the summary JSON here (must not exist)")
    a = ap.parse_args()
    m, sha = load_frozen(a.frozen)
    obs = list(observations(a.cases, a.stride, a.max_obs))
    py = []
    for o in obs:
        mkt = market_dict(o)
        mid, _src = m._market_yes_probability(mkt, m._quotes(mkt))
        flow_p, _ = m._flow_features(mkt)
        py.append({"mo": frozen_decision(m, mkt, mid), "bf": frozen_decision(m, mkt, flow_p if flow_p is not None else mid)})
    proc = subprocess.run([a.exe, "parity-eval"], input="\n".join(json.dumps(o) for o in obs) + "\n", capture_output=True, text=True, check=True)
    rs = [json.loads(l) for l in proc.stdout.splitlines()]
    assert len(rs) == len(py) == len(obs), (len(rs), len(py), len(obs))
    summary = {"frozen_sha256": sha, "observations": len(obs), "strategies": {}}
    bad_any = False
    for key, name in (("mo", "market_only_p0_v2"), ("bf", "book_flow_imbalance_p0_v3 (book-only subset)")):
        stats = {"rec_match": 0, "p_match": 0, "edge_match": 0, "trades_python": 0, "trades_rust": 0, "mismatches": []}
        for i, (p, r) in enumerate(zip(py, rs)):
            pp, rr = p[key], r[key]
            rec_ok = pp["recommendation"] == rr["recommendation"]
            p_ok = (pp["p_yes"] is None) == (rr["p_yes"] is None) and (pp["p_yes"] is None or abs(pp["p_yes"] - rr["p_yes"]) < 1e-9)
            e_ok = (pp["edge"] is None) == (rr["edge"] is None) and (pp["edge"] is None or abs(pp["edge"] - rr["edge"]) <= 1.5e-6)
            stats["rec_match"] += rec_ok
            stats["p_match"] += p_ok
            stats["edge_match"] += e_ok
            stats["trades_python"] += pp["recommendation"] != "SKIP"
            stats["trades_rust"] += rr["recommendation"] != "SKIP"
            if not (rec_ok and p_ok and e_ok) and len(stats["mismatches"]) < 5:
                stats["mismatches"].append({"i": i, "obs": obs[i], "python": pp, "rust": rr})
        stats["all_match"] = stats["rec_match"] == stats["p_match"] == stats["edge_match"] == len(obs)
        bad_any |= not stats["all_match"]
        summary["strategies"][name] = stats
    text = json.dumps(summary, indent=1)
    if a.out:
        with open(a.out, "x", encoding="utf-8") as f:
            f.write(text)
    print(text)
    sys.exit(1 if bad_any else 0)


if __name__ == "__main__":
    main()
