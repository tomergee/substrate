#!/usr/bin/env python3
"""Print P50 / P90 tables from results/*.jsonl (this harness) and, optionally, from the AgentENV
harness files (agentenv-on-gke/tests/concurrency-results/*.jsonl) so both read the same way.

  python3 summarize.py results/golden-one-1.jsonl results/golden-one-10.jsonl ...
"""
import json, sys

def load(path):
    lines = [json.loads(l) for l in open(path) if l.startswith("{")]
    summ = next((l for l in reversed(lines) if l.get("summary")), None)
    recs = [l for l in lines if not l.get("summary") and not l.get("setup")]
    return summ, recs

def fmt(s, k):
    v = s.get(k); return "-" if not v or v.get("p50") is None else f"{v['p50']} / {v['p90']}"

rows = []
for path in sys.argv[1:]:
    s, recs = load(path)
    if not s: print(f"{path}: no summary"); continue
    system = s.get("system", "agentenv"); mode = s.get("mode"); n = s.get("n")
    if system == "substrate":
        start = fmt(s, "t_running"); ready = fmt(s, "t_ready"); cmd = fmt(s, "t_first_cmd")
        park = fmt(s, "park_s"); res = fmt(s, "resume_running_s"); rcmd = fmt(s, "resume_first_cmd_s")
        nodes = len(s.get("per_node") or {}); extra = f"retries={s.get('retries_total')} goldens={((s.get('template_setup') or {}).get('goldens_ready'))}"
    else:
        start = fmt(s, "t_running"); ready = "-"; cmd = fmt(s, "t_first_cmd")
        park = fmt(s, "pause_s"); res = fmt(s, "resume_running_s"); rcmd = fmt(s, "resume_first_cmd_s")
        nodes = len(s.get("per_node_running_at_peak") or {}); extra = ""
    rows.append((system, mode, n, f"{s['pass']}/{n}", start, ready, cmd, park, res, rcmd, nodes, extra, s.get("kill_all_s")))
hdr = ("system", "mode", "N", "pass", "running p50/p90", "ready", "first cmd", "park", "resume running", "resume first cmd", "nodes", "notes", "kill s")
w = [max(len(str(r[i])) for r in rows + [hdr]) for i in range(len(hdr))]
for r in [hdr] + rows: print("  ".join(str(c).ljust(w[i]) for i, c in enumerate(r)))
