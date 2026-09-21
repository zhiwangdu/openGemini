#!/usr/bin/env python3
"""Interleaved A/B runs, followed by single-factor diagnostic interventions."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(name, variant="", flags=()):
    if (ROOT / "runs" / name / "summary.json").exists():
        print(f"already complete: {name}", flush=True)
        return
    subprocess.run([sys.executable, str(ROOT / "scripts/run.py"), "--name", name,
                    "--binary", str(ROOT / "bin/ts-server-experiments"),
                    "--variant", variant, *flags], check=True)


for repeat in range(1, 4):
    for name, variant in [("control", ""), ("append", "append"),
                          ("doubling", "doubling"), ("prealloc", "prealloc")]:
        run(f"{name}{repeat}", variant)
for name, variant, flags in [
    ("nopools", "nopools", []),
    ("pool64k", "pool64k", []),
    ("singlemarshal", "singlemarshal", []),
    ("trimsort", "trimsort", ["--sizes", "64", "--query-before-flush"]),
    ("doubling_growth", "doubling", ["--sizes", "64,100,128,200,256"]),
    ("scavenge256", "", ["--free-os-memory"]),
    ("gc50_2", "", ["--gogc", "50"]),
    ("gc50_3", "", ["--gogc", "50"]),
]:
    run(name, variant, flags)
