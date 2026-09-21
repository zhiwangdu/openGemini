#!/usr/bin/env python3
"""Bounded runtime/flush controls; flush cases are not retained-payload ratios."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
cases = [
    ("memlimit512_1", ["--gomemlimit", "512MiB"]),
    ("memlimit512_2", ["--gomemlimit", "512MiB"]),
    ("memlimit512_3", ["--gomemlimit", "512MiB"]),
    ("flush32m", ["--shard-limit", "32m", "--allow-flush"]),
    ("query_control", ["--sizes", "64", "--query-before-flush"]),
    ("fullread_concurrent8", ["--sizes", "8", "--concurrency", "8", "--verify-full"]),
]
for name, flags in cases:
    if (ROOT / "runs" / name / "summary.json").exists():
        continue
    subprocess.run([sys.executable, str(ROOT / "scripts/run.py"), "--name", name,
                    "--binary", str(ROOT / "bin/ts-server-validated"), *flags], check=True)
