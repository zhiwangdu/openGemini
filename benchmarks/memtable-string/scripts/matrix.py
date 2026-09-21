#!/usr/bin/env python3
"""Run one fresh server at a time; completed experiments are preserved."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("baseline1", []), ("baseline2", []), ("baseline3", []),
    ("growth", ["--sizes", "64,128,256"]),
    ("series128", ["--series", "128"]),
    ("series8192", ["--series", "8192"]),
    ("field1k", ["--field-size", "1024"]),
    ("field32k", ["--field-size", "32768"]),
    ("concurrent8", ["--concurrency", "8"]),
    ("batch1000", ["--batch", "1000"]),
    ("batch1", ["--batch", "1"]),
    ("cpu1", ["--cpus", "1"]),
    ("repeated", ["--pattern", "repeated"]),
    ("nowal", ["--no-wal"]),
    ("gc50", ["--gogc", "50"]),
    ("query_retry", ["--sizes", "64", "--query-before-flush", "--free-os-memory"]),
    ("reversed", ["--sizes", "64", "--reverse", "--query-before-flush"]),
    ("escaped", ["--sizes", "4", "--pattern", "escaped"]),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--cases", help="comma separated case names")
    args = p.parse_args()
    wanted = args.cases.split(",") if args.cases else [name for name, _ in CASES]
    for name, flags in CASES:
        if name not in wanted:
            continue
        run = ROOT / "runs" / name
        if (run / "summary.json").exists():
            print(f"already complete: {name}", flush=True)
            continue
        if run.exists():
            raise RuntimeError(f"incomplete run retained for inspection: {run}; choose a new name with run.py")
        subprocess.run([sys.executable, str(ROOT / "scripts" / "run.py"), "--name", name, *flags], check=True)


if __name__ == "__main__":
    main()
