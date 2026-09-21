#!/usr/bin/env python3
"""Export self-contained pprof reports, source patches and artifact identities."""
import csv
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "source"
OUT = ROOT / "evidence"
OUT.mkdir(exist_ok=True)


def capture(command, cwd=ROOT):
    p = subprocess.run(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode not in (0, 1):
        raise RuntimeError(p.stdout)
    return p.stdout


commands = []
for name in ("baseline1", "doubling1", "prealloc1", "nopools", "query_retry", "trimsort"):
    run = ROOT / "runs" / name
    summary = json.loads((run / "summary.json").read_text())
    gc = next(c["label"] for c in summary["checkpoints"] if c["label"].startswith("gc_"))
    for sample in ("inuse_space", "alloc_space"):
        command = ["go", "tool", "pprof", "-top", "-sample_index=" + sample, "-base",
                   str(run / "profiles/baseline.heap"), summary["parameters"]["binary"],
                   str(run / "profiles" / (gc + ".heap"))]
        (OUT / (name + "_" + sample + ".txt")).write_text(capture(command))
        commands.append(command)
(OUT / "pprof_commands.json").write_text(json.dumps(commands, indent=2) + "\n")

diagnostic_paths = ["engine/memory_diagnostic.go", "engine/mutable/memory_diagnostic.go"]
experiment_paths = ["lib/memexperiment/options.go", "lib/memexperiment/options_disabled.go",
                    "lib/record/memory_experiment_test.go"]
diagnostic = "".join(capture(["git", "diff", "--no-index", "--", "/dev/null", p], SRC)
                     for p in diagnostic_paths)
experiment = capture(["git", "diff", "HEAD"], SRC) + "".join(
    capture(["git", "diff", "--no-index", "--", "/dev/null", p], SRC) for p in experiment_paths)
(ROOT / "patches/instrumentation.patch").write_text(diagnostic)
(ROOT / "patches/experiments.patch").write_text(experiment)

manifest = {
    "source_commit": capture(["git", "rev-parse", "HEAD"], SRC).strip(),
    "go_version": capture(["go", "version"]).strip(),
    "go_root": capture(["go", "env", "GOROOT"]).strip(),
    "binaries": {},
    "completed_runs": [],
    "failed_runs_preserved": [],
}
environment_commands = {
    "kernel.txt": ["uname", "-a"],
    "cpu.txt": ["lscpu"],
    "memory.txt": ["free", "-b"],
    "python.txt": [sys.executable, "--version"],
    "plot_dependencies.txt": [str(ROOT / ".venv/bin/python"), "-m", "pip", "freeze"],
    "source_status.txt": ["git", "status", "--short"],
}
for filename, command in environment_commands.items():
    (OUT / filename).write_text(capture(command, SRC if filename == "source_status.txt" else ROOT))
manifest["environment_files"] = list(environment_commands)
for binary in sorted((ROOT / "bin").glob("ts-server*")):
    with binary.open("rb") as f:
        digest = hashlib.file_digest(f, "sha256").hexdigest()
    manifest["binaries"][binary.name] = {"sha256": digest, "size": binary.stat().st_size}
    (OUT / (binary.name + ".buildinfo")).write_text(capture(["go", "version", "-m", str(binary)]))
for run in sorted((ROOT / "runs").iterdir()):
    if (run / "summary.json").exists():
        manifest["completed_runs"].append(run.name)
    elif (run / "error.json").exists():
        manifest["failed_runs_preserved"].append(run.name)
manifest["artifact_sha256"] = {}
for path in sorted([*(ROOT / "scripts").glob("*.py"), *(ROOT / "patches").glob("*.patch"),
                    ROOT / "README.md", ROOT / "REPORT.md"]):
    with path.open("rb") as f:
        manifest["artifact_sha256"][str(path.relative_to(ROOT))] = hashlib.file_digest(f, "sha256").hexdigest()
(ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

rows = list(csv.DictReader((ROOT / "results.csv").open()))
groups = {"baseline": ["baseline1", "baseline2", "baseline3"],
          "control": ["control1", "control2", "control3"],
          "append": ["append1", "append2", "append3"],
          "doubling": ["doubling1", "doubling2", "doubling3"],
          "prealloc": ["prealloc1", "prealloc2", "prealloc3"],
          "gc50": ["gc50", "gc50_2", "gc50_3"],
          "memlimit512": ["memlimit512_1", "memlimit512_2", "memlimit512_3"]}
rollup = {}
for name, names in groups.items():
    group = [r for r in rows if r["run"] in names]
    rollup[name] = {k: {"median": statistics.median(float(r[k]) for r in group),
                        "min": min(float(r[k]) for r in group),
                        "max": max(float(r[k]) for r in group)}
                   for k in ["column_cap_x", "heap_gc_x", "peak_rss_x", "alloc_x", "throughput_mib_s", "p99_ms"]}
(OUT / "rollup.json").write_text(json.dumps(rollup, indent=2) + "\n")
print(json.dumps({"completed": len(manifest["completed_runs"]), "preserved_failed": manifest["failed_runs_preserved"],
                  "rollup": rollup}, indent=2))
