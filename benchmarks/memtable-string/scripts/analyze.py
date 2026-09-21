#!/usr/bin/env python3
"""Summarize measured amplification without mixing allocated and retained bytes."""
import csv
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]
MIB = 1024**2


def analyze():
    rows = []
    for path in sorted((ROOT / "runs").glob("*/summary.json")):
        s = json.loads(path.read_text())
        base = s["baseline"]
        for load in s["writes"]:
            payload = load["payload_bytes"]
            target = payload / MIB
            candidates = [c for c in s["checkpoints"] if c["label"].startswith("loaded_")]
            raw = min(candidates, key=lambda c: abs(float(c["label"].split("_")[1])-target))
            gc = next(c for c in s["checkpoints"] if c["label"] == raw["label"].replace("loaded_", "gc_"))
            cap = sum(sum(sh["Active"][k] for k in ["ValueCap", "OffsetCapBytes", "BitmapCap"]) for sh in gc["shards"])
            base_cap = sum(sum(sh["Active"][k] for k in ["ValueCap", "OffsetCapBytes", "BitmapCap"]) for sh in base["shards"])
            row = dict(run=path.parent.name, mib=round(target, 3),
                       series=sum(sh["Active"]["Series"] for sh in gc["shards"]),
                       unordered=sum(sh["Active"]["UnorderedRecords"] for sh in gc["shards"]),
                       shards=len(gc["shards"]),
                       column_cap_x=(cap-base_cap)/payload,
                       heap_gc_x=(gc["memstats"]["HeapAlloc"]-base["memstats"]["HeapAlloc"])/payload,
                       heap_loaded_x=(raw["memstats"]["HeapAlloc"]-base["memstats"]["HeapAlloc"])/payload,
                       peak_rss_x=(raw["proc"]["VmHWM"]-base["proc"]["VmRSS"])/payload,
                       rss_gc_x=(gc["proc"]["VmRSS"]-base["proc"]["VmRSS"])/payload,
                       alloc_x=(raw["memstats"]["TotalAlloc"]-base["memstats"]["TotalAlloc"])/payload,
                       gc_cycles=raw["memstats"]["NumGC"]-base["memstats"]["NumGC"],
                       gc_pause_ms=(raw["memstats"]["PauseTotalNs"]-base["memstats"]["PauseTotalNs"])/1e6,
                       throughput_mib_s=load["field_mib_s"], p99_ms=load["p99_s"]*1000,
                       flush_count=raw["flush_count"]-base["flush_count"])
            rows.append(row)
    if rows:
        with (ROOT / "results.csv").open("w") as f:
            w = csv.DictWriter(f, fieldnames=rows[0])
            w.writeheader()
            w.writerows(rows)
    print("run                            MiB   cap_x  gc_heap_x raw_heap_x peak_rss_x alloc_x MiB/s  unordered")
    for r in rows:
        print(f'{r["run"]:30} {r["mib"]:5.0f} {r["column_cap_x"]:6.2f} {r["heap_gc_x"]:9.2f} '
              f'{r["heap_loaded_x"]:10.2f} {r["peak_rss_x"]:10.2f} {r["alloc_x"]:7.2f} '
              f'{r["throughput_mib_s"]:5.0f} {r["unordered"]:9}')
    return rows


if __name__ == "__main__":
    analyze()
