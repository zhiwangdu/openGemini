#!/usr/bin/env python3
"""Publication-style static plots built from archived measurements."""
import csv
import json
from pathlib import Path
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "figures"
OUT.mkdir(exist_ok=True)
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 140, "savefig.bbox": "tight"})
colors = ["#4169a1", "#409b92", "#d68a39", "#735696"]
rollup = json.loads((ROOT / "evidence/rollup.json").read_text())
fig, ax = plt.subplots(figsize=(9, 4.5))
names = ["control", "append", "doubling", "prealloc"]
labels = ["Baseline", "Direct append", "2x growth PoC", "Exact reservation\n(oracle only)"]
for j, (metric, label, color) in enumerate([
        ("heap_gc_x", "Live heap after two GCs", colors[0]),
        ("peak_rss_x", "Peak RSS increase", colors[2]),
        ("alloc_x", "Cumulative allocation", colors[1])]):
    med = [rollup[n][metric]["median"] for n in names]
    low = [m-rollup[n][metric]["min"] for m, n in zip(med, names)]
    high = [rollup[n][metric]["max"]-m for m, n in zip(med, names)]
    pos = np.arange(len(names)) + (j-1)*.25
    bars = ax.bar(pos, med, .23, label=label, color=color, yerr=[low, high], capsize=3)
    for bar, v in zip(bars, med):
        ax.text(bar.get_x()+bar.get_width()/2, v+.10, f"{v:.2f}", ha="center", fontsize=8)
ax.set_xticks(np.arange(len(names)), labels)
ax.set_ylabel("Increase / 256 MiB field payload")
ax.set_ylim(0, 7.1)
ax.set_title("10 KiB string field, one series: memory layers and interventions")
ax.legend(loc="upper right", fontsize=8)
ax.grid(axis="y", alpha=.18)
ax.set_axisbelow(True)
fig.text(.13, -.025, "Median of three fresh processes; error bars show min/max. All pre-flush samples retain every row.", fontsize=8)
for ext in ["svg", "png"]:
    fig.savefig(OUT / f"memory_layers.{ext}")
plt.close(fig)

fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
for name, color in [("baseline", colors[0]), ("doubling", colors[2])]:
    text = (ROOT / f"validation/{name}_geometry.log").read_text()
    matches = [tuple(map(int, m)) for m in re.findall(
        r"row=(\d+) len=(\d+) old_cap=(\d+) new_cap=(\d+) cumulative_cap=(\d+)", text)]
    x = [m[0]*10240/1048576 for m in matches]
    y = [m[4]/1048576 for m in matches]
    final = re.search(r"FINAL payload=(\d+) len=\d+ cap=\d+ cumulative_cap=(\d+)", text)
    x.append(int(final.group(1))/1048576)
    y.append(int(final.group(2))/1048576)
    axes[0].step(x, y, where="post", label=name, color=color)
axes[0].set_xlabel("String payload (MiB)")
axes[0].set_ylabel("Sum of allocated value capacities (MiB)")
axes[0].set_title("Repeated growth copies the entire old column")
axes[0].legend()
axes[0].grid(alpha=.2)

rows = list(csv.DictReader((ROOT / "results.csv").open()))
for idx, (name, color) in enumerate([("baseline_boundaries", colors[0]), ("doubling_boundaries", colors[2])]):
    data = [r for r in rows if r["run"] == name]
    axes[1].bar(np.arange(3)+(idx-.5)*.35, [float(r["heap_gc_x"]) for r in data], .33,
                label=name.split("_")[0], color=color)
axes[1].set_xticks(np.arange(3), ["76", "150", "300"])
axes[1].set_xlabel("String payload (MiB)")
axes[1].set_ylabel("Live heap increase / field payload")
axes[1].set_ylim(0, 2.35)
axes[1].set_title("2x growth can nearly double the retained heap")
axes[1].legend()
axes[1].grid(axis="y", alpha=.2)
axes[1].set_axisbelow(True)
fig.tight_layout()
for ext in ["svg", "png"]:
    fig.savefig(OUT / f"growth_tradeoff.{ext}")
plt.close(fig)
print(OUT)
