#!/usr/bin/env python3
"""Capacity boundaries, equal row counts, and crash/readback verification."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
cases = [
    ("baseline_boundaries", "", ["--sizes", "76,150,300"]),
    ("doubling_boundaries", "doubling", ["--sizes", "76,150,300"]),
    ("doubling_series128", "doubling", ["--series", "128"]),
    ("equalrows_1k", "", ["--field-size", "1024", "--sizes", "25.599609375"]),
    ("equalrows_32k", "", ["--field-size", "32768", "--sizes", "819.1875"]),
    ("wal_recovery_baseline", "", ["--sizes", "4", "--recovery", "--verify-full"]),
    ("wal_recovery_doubling", "doubling", ["--sizes", "8", "--recovery", "--verify-full", "--pattern", "escaped"]),
    ("wal_recovery_singlemarshal", "singlemarshal", ["--sizes", "4", "--recovery", "--verify-full"]),
    ("fullread_append", "append", ["--sizes", "4", "--verify-full", "--pattern", "escaped", "--reverse"]),
    ("fullread_pool64k", "pool64k", ["--sizes", "4", "--verify-full", "--pattern", "escaped"]),
]
for name, variant, flags in cases:
    if (ROOT / "runs" / name / "summary.json").exists():
        continue
    subprocess.run([sys.executable, str(ROOT / "scripts/run.py"), "--name", name,
                    "--binary", str(ROOT / "bin/ts-server-experiments"),
                    "--variant", variant, *flags], check=True)
