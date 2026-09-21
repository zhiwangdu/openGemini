#!/usr/bin/env python3
"""Check retention invariants independently of the load generator."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
checked = []
for path in sorted((ROOT / "runs").glob("*/summary.json")):
    summary = json.loads(path.read_text())
    if summary["parameters"].get("allow_flush"):
        continue
    base = summary["baseline"]
    base_rows = sum(sh["Active"]["Rows"] for sh in base["shards"])
    for load in summary["writes"]:
        label = min((c["label"] for c in summary["checkpoints"] if c["label"].startswith("loaded_")),
                    key=lambda s: abs(float(s.split("_")[1])-load["payload_bytes"]/1048576))
        for checkpoint in (label, label.replace("loaded_", "gc_")):
            c = next(c for c in summary["checkpoints"] if c["label"] == checkpoint)
            assert c["flush_count"] == base["flush_count"], (path, checkpoint, "flush count changed")
            assert not c["tssp_files"], (path, checkpoint, "data was flushed")
            assert len(c["shards"]) == 1, (path, checkpoint, "multiple shards")
            assert sum(sh["Active"]["Rows"] for sh in c["shards"]) == base_rows+load["cumulative_rows"], (path, checkpoint, "row mismatch")
            assert not sum(sh["Snapshot"]["Rows"] for sh in c["shards"]), (path, checkpoint, "snapshot in flight")
            checked.append(f"{path.parent.name}/{checkpoint}")
validation = json.loads((ROOT / "validation/results.json").read_text())
assert all(item["returncode"] == 0 for item in validation)
out = {"retained_samples_checked": len(checked), "checks_passed": len(validation),
       "samples": checked, "status": "passed"}
(ROOT / "validation/artifact_audit.json").write_text(json.dumps(out, indent=2)+"\n")
print(json.dumps({k: v for k, v in out.items() if k != "samples"}))
