#!/usr/bin/env python3
"""Persist regression-test, allocation benchmark and capacity-geometry evidence."""
import json
import argparse
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "validation"
OUT.mkdir(exist_ok=True)
common = ["go", "test", "-mod=readonly", "-gcflags=all=-l", "-tags", "memdiag",
          "-count=1", "-p", "2", "-timeout", "5m"]
checks = [
    ("packages", "", common + ["-short", "./lib/record", "./engine/mutable",
                               "./lib/util/lifted/vm/protoparser/influx", "./coordinator"]),
    ("doubling", "doubling", common + ["-short", "./lib/record", "./engine/mutable"]),
    ("pool64k", "pool64k", common + ["-short", "./lib/util/lifted/vm/protoparser/influx", "./coordinator"]),
    ("singlemarshal", "singlemarshal", common + ["-short", "./coordinator"]),
    ("wal_mutable", "doubling", common + ["-short", "./engine", "-run",
                  "^(TestWalReplayParallel|TestWalReplaySerial|TestQueryOnlyInMutableTable)$"]),
    ("normal_build", "", ["go", "test", "-mod=readonly", "-gcflags=all=-l", "-short",
                         "./lib/record", "./engine/mutable", "-count=1"]),
]
parser = argparse.ArgumentParser()
parser.add_argument("--bench-only", action="store_true")
args = parser.parse_args()
results = []
if args.bench_only:
    results = [r for r in json.loads((OUT / "results.json").read_text())
               if not r["name"].endswith(("_benchmark", "_geometry"))]
    checks = []
for name, variant, command in checks:
    env = os.environ.copy()
    env.pop("OG_MEMDIAG_ADDR", None)
    env["OG_MEMLAB_VARIANT"] = variant
    start = time.monotonic()
    completed = subprocess.run(command, cwd=ROOT / "source", env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    (OUT / (name + ".log")).write_text(completed.stdout)
    results.append(dict(name=name, variant=variant, command=command, returncode=completed.returncode,
                        seconds=time.monotonic()-start))
    print(json.dumps(results[-1]), flush=True)
    (OUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")

# Performance runs deliberately keep normal compiler inlining enabled.
for name, variant in [("baseline", ""), ("append", "append"), ("doubling", "doubling")]:
    env = os.environ.copy()
    env.pop("OG_MEMDIAG_ADDR", None)
    env["OG_MEMLAB_VARIANT"] = variant
    for kind, extra in [
        ("benchmark", ["-run", "^$", "-bench", "BenchmarkLargeStringLayout", "-benchtime=3x", "-count=3"]),
        ("geometry", ["-run", "^TestMemoryGrowthGeometry$", "-v", "-count=1"]),
    ]:
        command = ["go", "test", "-mod=readonly", "-tags", "memdiag", "./lib/record", *extra]
        completed = subprocess.run(command, cwd=ROOT / "source", env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True)
        (OUT / (name + "_" + kind + ".log")).write_text(completed.stdout)
        results.append(dict(name=name+"_"+kind, variant=variant, command=command, returncode=completed.returncode))
        print(json.dumps(results[-1]), flush=True)
        (OUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")

if any(r["returncode"] for r in results):
    raise SystemExit("some checks failed; see validation/results.json and corresponding logs")
