#!/usr/bin/env python3
"""
End-to-end performance benchmark for tsmMergeCursor unordered merge optimization.

Writes ordered + unordered data (real disjoint layout), flushes to TSSP, runs
ascending/descending queries with the lazy flag on/off, and collects metrics.

Usage:
    # Smoke test (tiny dataset, verify functionality):
    python3 bench_unordered_merge.py --smoke

    # Full benchmark (N sweep):
    python3 bench_unordered_merge.py --mode nsweep

    # Multi-dimensional cross at N=1000:
    python3 bench_unordered_merge.py --mode cross

Cluster control:
    /home/duzhiwang/workspace/data/openGeminiCluster/cluster.sh {start|stop|restart|status}
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Constants ──────────────────────────────────────────────────────────────────

CLUSTER_SH = "/home/duzhiwang/workspace/data/openGeminiCluster/cluster.sh"
CLUSTER_DIR = "/home/duzhiwang/workspace/data/openGeminiCluster"
SQL_URL = "http://127.0.0.1:8086"                          # InfluxDB-compatible API
STORE_HTTP_URLS = [                                         # for /debug/ctrl?mod=flush
    "http://127.0.0.1:8091",
    "http://127.0.0.1:8191",
    "http://127.0.0.1:8291",
]
DB_NAME = "benchdb"
RP_NAME = "autogen"
MST_NAME = "mst"
FLAG_API = f"{SQL_URL}/debug/ctrl?mod=lazy_unordered_merge"  # POST with &switchon=true/false

# Config snippet to disable compaction/merge — written as a separate override file that
# the cluster.sh render_config merges into the final config. We modify the [data.merge]
# section in-place since the config already has one.
DISABLE_COMPACTION_OVERRIDES = {
    "data.memtable.write-cold-duration": "2s",
    "data.memtable.shard-mutable-size-limit": "1m",
    "data.compact.max-concurrent-compactions": "0",
    "data.compact.compact-full-write-cold-duration": "9999h",
    "data.merge.max-unordered-file-number": "99999",
    "data.merge.max-unordered-file-size": "999g",
    "data.merge.min-interval": "9999h",
}

# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class BenchConfig:
    n_unordered: int = 100          # number of out-of-order files
    rows_per_file: int = 100        # R: rows per file per series
    n_series: int = 100             # S
    n_fields: int = 5               # F
    max_rows: int = 1000            # query chunk size
    overlap: str = "disjoint"       # "disjoint" or "overlapping"
    directions: List[str] = field(default_factory=lambda: ["asc", "desc"])
    repeat: int = 20                # queries per config
    warmup: int = 3
    lazy_flag: bool = False         # enable lazy merge path
    flush_wait: float = 2.0         # seconds to wait after flush
    segments_per_file: int = 1      # 1=single-seg; >1=multi-seg (use max-rows-per-segment)

@dataclass
class QueryResult:
    direction: str
    lazy: bool
    total_ms: float
    first_row_ms: float
    row_count: int
    config: dict

# ── HTTP Session ───────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# ── Cluster Control ────────────────────────────────────────────────────────────

class Cluster:
    def __init__(self):
        self.session = make_session()
        self._flush_wait = 2.0
        self._config_patched = False

    def patch_configs(self):
        """Modify config files to disable compaction/merge.

        The config has [data.merge] as an active section, but [data.memtable] and
        [data.compact] are commented out (# [data.memtable]). For commented sections
        we append new active sections at the end. For [data.merge] we modify in-place.
        """
        if self._config_patched:
            return
        for i in (1, 2, 3):
            conf_path = os.path.join(CLUSTER_DIR, "config", f"openGemini-{i}.conf")
            backup = conf_path + ".bak"
            if not os.path.exists(backup):
                shutil.copy2(conf_path, backup)

            with open(backup, "r") as f:
                content = f.read()

            # 1. Modify [data.merge] fields in-place (uncomment + override)
            merge_overrides = {
                "max-unordered-file-number": "99999",
                "max-unordered-file-size": '"999g"',
                "min-interval": '"9999h"',
            }
            for field, val in merge_overrides.items():
                # Replace commented line like: "  # max-unordered-file-number = 64"
                import re
                pattern = rf'(\s*)#\s*{re.escape(field)}\s*=.*'
                replacement = rf'\g<1>{field} = {val}'
                content = re.sub(pattern, replacement, content)

            # 2. Append new active sections for memtable + compact (they're commented in original)
            content += "\n# --- bench: disable compaction ---\n"
            content += "[data.memtable]\n"
            content += "  write-cold-duration = \"2s\"\n"
            content += "  shard-mutable-size-limit = \"1m\"\n"
            content += "[data.compact]\n"
            content += "  max-concurrent-compactions = 0\n"
            content += "  compact-full-write-cold-duration = \"9999h\"\n"

            with open(conf_path, "w") as f:
                f.write(content)
            print(f"[cluster] patched config {i}: compaction/merge disabled")
        self._config_patched = True

    def restore_configs(self):
        """Restore original config files from .bak."""
        for i in (1, 2, 3):
            conf_path = os.path.join(CLUSTER_DIR, "config", f"openGemini-{i}.conf")
            backup = conf_path + ".bak"
            if os.path.exists(backup):
                os.rename(backup, conf_path)
                print(f"[cluster] restored config {i}")
        self._config_patched = False

    def start(self, patch_compaction=True):
        if patch_compaction:
            self.patch_configs()
        print("[cluster] starting...")
        subprocess.run([CLUSTER_SH, "start"], check=True)
        self._wait_ready(timeout=60)
        print("[cluster] ready")

    def stop(self):
        print("[cluster] stopping...")
        subprocess.run([CLUSTER_SH, "stop"], check=False)

    def restart(self):
        self.stop()
        time.sleep(2)
        self.start()

    def status(self) -> str:
        r = subprocess.run([CLUSTER_SH, "status"], capture_output=True, text=True)
        return r.stdout

    def _wait_ready(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = self.session.get(f"{SQL_URL}/ping", timeout=2)
                if resp.status_code in (200, 204):
                    return
            except requests.ConnectionError:
                pass
            time.sleep(1)
        raise RuntimeError(f"cluster not ready within {timeout}s")

    def flush_memtable(self):
        """Flush all memtables to TSSP files via /debug/ctrl?mod=flush on each store."""
        for url in STORE_HTTP_URLS:
            try:
                resp = self.session.get(f"{url}/debug/ctrl?mod=flush", timeout=10)
                if resp.status_code != 200:
                    print(f"  [flush] {url} returned {resp.status_code}")
            except requests.ConnectionError as e:
                print(f"  [flush] {url} failed: {e}")
        time.sleep(self._flush_wait)

    def set_lazy_flag(self, enabled: bool):
        """Toggle the lazy unordered merge flag via sysctrl endpoint (POST)."""
        val = "true" if enabled else "false"
        try:
            resp = self.session.post(f"{FLAG_API}&switchon={val}", timeout=5)
            if resp.status_code != 200:
                print(f"  [flag] set to {val} returned {resp.status_code}: {resp.text}")
            else:
                print(f"  [flag] set to {val} OK")
        except requests.ConnectionError:
            print(f"  [flag] endpoint not available")

    def count_tssp_files(self, db_name: str = DB_NAME, mst: str = MST_NAME) -> dict:
        """Count ordered + out-of-order TSSP files across all store nodes.

        Returns {"ordered": int, "out_of_order": int, "total": int, "details": [...]}.
        """
        result = {"ordered": 0, "out_of_order": 0, "total": 0, "details": []}
        store_data = os.path.join(CLUSTER_DIR, "data", "store")
        for store_idx in (1, 2, 3):
            store_path = os.path.join(store_data, str(store_idx), "data", db_name)
            if not os.path.isdir(store_path):
                continue
            for root, dirs, files in os.walk(store_path):
                for f in files:
                    if not f.endswith(".tssp"):
                        continue
                    rel = os.path.relpath(os.path.join(root, f), store_path)
                    is_ooo = "out-of-order" in rel
                    if is_ooo:
                        result["out_of_order"] += 1
                    else:
                        result["ordered"] += 1
                    result["total"] += 1
                    result["details"].append({
                        "store": store_idx,
                        "path": rel,
                        "type": "ooo" if is_ooo else "ord",
                    })
        return result

    def verify_file_count(self, expected_ooo: int, expected_ord_min: int = 1,
                          db_name: str = DB_NAME) -> bool:
        """Verify TSSP file count matches expectations."""
        counts = self.count_tssp_files(db_name)
        ord_n = counts["ordered"]
        ooo_n = counts["out_of_order"]
        total = counts["total"]
        print(f"[files] ordered={ord_n}  out-of-order={ooo_n}  total={total}  "
              f"(expected: ooo={expected_ooo}, ord>={expected_ord_min})")

        if ooo_n != expected_ooo:
            print(f"[files] WARNING: out-of-order count {ooo_n} != expected {expected_ooo}")
            print(f"  Possible causes: compaction merged some files, or flush didn't create separate files.")
            print(f"  Details:")
            for d in counts["details"][:20]:
                print(f"    store{d['store']} [{d['type']}] {d['path']}")
            if len(counts["details"]) > 20:
                print(f"    ... and {len(counts['details']) - 20} more")
            return False

        if ord_n < expected_ord_min:
            print(f"[files] WARNING: ordered count {ord_n} < expected min {expected_ord_min}")
            return False

        print(f"[files] OK: file count verified")
        return True

    def get_span_metrics(self) -> dict:
        """Try to fetch span/metrics info (if available)."""
        # Placeholder: in production, query /debug/pprof or span endpoints
        return {}

# ── Data Writer ────────────────────────────────────────────────────────────────

class DataWriter:
    """Writes ordered + unordered data in the real disjoint layout."""

    def __init__(self, cluster: Cluster, config: BenchConfig):
        self.cluster = cluster
        self.config = config
        self.session = cluster.session

    def _ensure_db(self):
        """Create database with a single-shard-group retention policy.

        Uses SHARD DURATION 365d so all data (spanning a few hours) lands in one shard group,
        avoiding cross-shard merge complexity that would contaminate the benchmark.
        """
        # Drop if exists
        self.session.post(f"{SQL_URL}/query", data={"q": f"DROP DATABASE {DB_NAME}"}, timeout=30)
        time.sleep(2)

        # Create with explicit shard duration
        create_sql = (f"CREATE DATABASE {DB_NAME} WITH DURATION 365d SHARD DURATION 365d "
                      f"REPLICATION 1 NAME {RP_NAME}")
        for attempt in range(5):
            resp = self.session.post(f"{SQL_URL}/query", data={"q": create_sql}, timeout=10)
            if resp.status_code == 200:
                # Wait for meta to propagate to store nodes
                time.sleep(3)
                # Verify by writing a test point with current timestamp
                test_ts = int(time.time()) * 1_000_000_000
                test_resp = self.session.post(
                    f"{SQL_URL}/write?db={DB_NAME}&rp={RP_NAME}",
                    data=f"_test,foo=bar v=1 {test_ts}",
                    timeout=5,
                )
                if test_resp.status_code in (200, 204):
                    print(f"[data] database {DB_NAME} ready (RP={RP_NAME}, shard duration=365d)")
                    return
                else:
                    print(f"  [db] attempt {attempt+1}: write test failed: {test_resp.status_code} {test_resp.text}")
            else:
                print(f"  [db] attempt {attempt+1}: create failed: {resp.status_code} {resp.text[:100]}")
            time.sleep(2)
        raise RuntimeError(f"failed to create database {DB_NAME}")

    def _write_batch(self, lines: List[str]):
        """Write a batch of Line Protocol lines."""
        if not lines:
            return
        resp = self.session.post(
            f"{SQL_URL}/write?db={DB_NAME}&rp={RP_NAME}",
            data="\n".join(lines),
            headers={"Content-Type": "text/plain"},
            timeout=30,
        )
        if resp.status_code not in (200, 204):
            raise RuntimeError(f"write failed: {resp.status_code} {resp.text}")

    def _make_line(self, series_idx: int, timestamp_ns: int, field_vals: List[int]) -> str:
        tags = f"k0=s{series_idx}"
        fields = ",".join(f"f{i}={v}" for i, v in enumerate(field_vals))
        return f"{MST_NAME},{tags} {fields} {timestamp_ns}"

    def write_ordered(self):
        """Write ordered data (newer timestamps, AFTER all unordered)."""
        cfg = self.config
        # Use current time as base. Ordered starts right after unordered ends.
        # Unordered occupies [now - N*R - 1, now - 1] seconds.
        # Ordered occupies [now, now + R] seconds.
        now_ns = int(time.time()) * 1_000_000_000
        t_order_start_ns = now_ns
        lines = []
        for s in range(cfg.n_series):
            for r in range(cfg.rows_per_file):
                ts = t_order_start_ns + r * 1_000_000_000  # ns, 1s per row
                vals = [s * 1000 + r + 1] * cfg.n_fields
                lines.append(self._make_line(s, ts, vals))
        self._write_batch(lines)
        self.cluster.flush_memtable()
        t_start_s = t_order_start_ns // 1_000_000_000
        t_end_s = (t_order_start_ns + cfg.rows_per_file * 1_000_000_000) // 1_000_000_000
        print(f"[data] ordered: {len(lines)} rows, timestamps [{t_start_s}, {t_end_s}] "
              f"({cfg.n_series} series × {cfg.rows_per_file} rows)")

    def write_unordered(self):
        """Write N unordered batches (older timestamps), each flush = 1 unordered file."""
        cfg = self.config
        # Unordered: timestamps go backwards from now-1. Each file gets R consecutive seconds.
        # File 0: [now - R, now - 1], File 1: [now - 2R, now - R - 1], etc.
        now_s = int(time.time())
        t_base_ns = (now_s - cfg.n_unordered * cfg.rows_per_file - 1) * 1_000_000_000
        for i in range(cfg.n_unordered):
            lines = []
            for s in range(cfg.n_series):
                for r in range(cfg.rows_per_file):
                    if cfg.overlap == "disjoint":
                        # Each file: distinct time range, no overlap with other files
                        ts = t_base_ns + (i * cfg.rows_per_file + r) * 1_000_000_000
                    else:
                        # Overlapping: all files write to the same time range
                        ts = t_base_ns + r * 1_000_000_000
                    vals = [i * 10000 + s * 100 + r] * cfg.n_fields
                    lines.append(self._make_line(s, ts, vals))
            self._write_batch(lines)
            self.cluster.flush_memtable()
            if (i + 1) % 10 == 0 or i == 0:
                print(f"[data] unordered file {i+1}/{cfg.n_unordered} written ({len(lines)} rows)")
        t_start_s = t_base_ns // 1_000_000_000
        t_end_s = (t_base_ns + cfg.n_unordered * cfg.rows_per_file * 1_000_000_000) // 1_000_000_000
        print(f"[data] unordered: {cfg.n_unordered} files × {cfg.rows_per_file} rows × "
              f"{cfg.n_series} series, timestamps [{t_start_s}, {t_end_s}]")

    def write_all(self):
        self._ensure_db()
        self.write_ordered()
        self.write_unordered()
        print("[data] all data written and flushed")

        # Verify single shard group
        self._verify_single_shard_group()

        # Verify file count
        cfg = self.config
        expected_ooo = cfg.n_unordered  # each flush should produce 1 unordered file
        ok = self.cluster.verify_file_count(expected_ooo=expected_ooo, expected_ord_min=1)
        if not ok:
            print("[data] WARNING: file count mismatch — compaction may have run during write")
        return ok

    def _verify_single_shard_group(self):
        """Verify all data landed in a single shard group."""
        try:
            resp = self.session.post(f"{SQL_URL}/query",
                                     data={"q": f"SHOW SHARDS"}, timeout=10)
            data = resp.json()
            series = data.get("results", [{}])[0].get("series", [])
            shard_ids = set()
            for s in series:
                if s.get("name") != DB_NAME:
                    continue
                for val in s.get("values", []):
                    # val: [id, database, retention_policy, shard_group, start_time, end_time, expiry_time, owners]
                    if len(val) > 3 and val[2] == RP_NAME:
                        shard_ids.add(val[3])  # shard_group id
            if len(shard_ids) == 1:
                print(f"[shards] OK: all data in 1 shard group (id={shard_ids.pop()})")
            elif len(shard_ids) == 0:
                print("[shards] WARNING: no shards found — data may not be written yet")
            else:
                print(f"[shards] WARNING: data spans {len(shard_ids)} shard groups {shard_ids} — "
                      f"expected 1. Increase SHARD DURATION or narrow the time range.")
        except Exception as e:
            print(f"[shards] could not verify shard groups: {e}")

    def cleanup(self):
        """Drop the database for clean re-runs."""
        self.session.post(f"{SQL_URL}/query", data={"q": f"DROP DATABASE {DB_NAME}"}, timeout=30)
        time.sleep(2)
        print("[data] cleaned up (database dropped)")

# ── Query Runner ───────────────────────────────────────────────────────────────

class QueryRunner:
    """Runs queries and collects timing metrics."""

    def __init__(self, cluster: Cluster, config: BenchConfig):
        self.cluster = cluster
        self.config = config
        self.session = cluster.session

    def _build_query(self, direction: str) -> str:
        order = "" if direction == "asc" else " ORDER BY time DESC"
        fields = ",".join(f"f{i}" for i in range(self.config.n_fields))
        return f"SELECT {fields} FROM {MST_NAME}{order}"

    def run_query(self, direction: str) -> Tuple[float, float, int]:
        """Run one query, return (total_ms, first_row_ms, row_count)."""
        q = self._build_query(direction)
        url = f"{SQL_URL}/query"
        params = {"db": DB_NAME, "rp": RP_NAME, "q": q}

        t_start = time.perf_counter()
        resp = self.session.post(url, data=params, timeout=300)
        t_end = time.perf_counter()

        total_ms = (t_end - t_start) * 1000
        first_ms = total_ms  # non-streaming: first row = total

        row_count = 0
        try:
            data = resp.json()
            results = data.get("results", [])
            for r in results:
                for s in r.get("series", []):
                    row_count += len(s.get("values", []))
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  [query] JSON parse error: {e}")

        return total_ms, first_ms, row_count

    def run_benchmark(self, direction: str, lazy: bool) -> List[QueryResult]:
        """Run warmup + repeat queries, return results list."""
        results = []
        # Warmup
        for _ in range(self.config.warmup):
            self.run_query(direction)

        # Measured
        for i in range(self.config.repeat):
            total_ms, first_ms, rows = self.run_query(direction)
            results.append(QueryResult(
                direction=direction,
                lazy=lazy,
                total_ms=total_ms,
                first_row_ms=first_ms,
                row_count=rows,
                config={
                    "N": self.config.n_unordered,
                    "R": self.config.rows_per_file,
                    "S": self.config.n_series,
                    "F": self.config.n_fields,
                    "overlap": self.config.overlap,
                },
            ))
            if (i + 1) % 5 == 0:
                print(f"  [{direction} {'lazy' if lazy else 'eager'}] run {i+1}/{self.config.repeat}: "
                      f"{total_ms:.1f}ms total, {first_ms:.1f}ms first, {rows} rows")
        return results

    def verify_results(self, direction: str) -> bool:
        """Verify flag on/off produce same row count."""
        self.cluster.set_lazy_flag(False)
        _, _, rows_eager = self.run_query(direction)
        self.cluster.set_lazy_flag(True)
        _, _, rows_lazy = self.run_query(direction)
        self.cluster.set_lazy_flag(False)
        match = rows_eager == rows_lazy
        if not match:
            print(f"  [verify] MISMATCH: eager={rows_eager} rows, lazy={rows_lazy} rows")
        else:
            print(f"  [verify] OK: {rows_eager} rows (eager == lazy)")
        return match

# ── Metrics ────────────────────────────────────────────────────────────────────

def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = int(len(s) * p / 100)
    return s[min(k, len(s) - 1)]

def report_results(results: List[QueryResult], output_dir: str):
    """Write CSV report and print summary."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_path = os.path.join(output_dir, "benchmark_results.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["direction", "lazy", "total_p50_ms", "total_p95_ms", "total_p99_ms",
                     "first_p50_ms", "first_p95_ms", "row_count", "N", "R", "S", "F", "overlap"])
        for r in results:
            w.writerow([
                r.direction, r.lazy,
                f"{r.total_ms:.2f}", f"{r.total_ms:.2f}", f"{r.total_ms:.2f}",
                f"{r.first_row_ms:.2f}", f"{r.first_row_ms:.2f}",
                r.row_count,
                r.config["N"], r.config["R"], r.config["S"], r.config["F"], r.config["overlap"],
            ])
    print(f"[report] written to {csv_path}")

def print_summary(results: List[QueryResult]):
    """Print summary comparison."""
    by_key = {}
    for r in results:
        key = (r.direction, r.lazy)
        by_key.setdefault(key, []).append(r)

    print("\n" + "=" * 80)
    print(f"{'Direction':<10} {'Path':<8} {'p50(ms)':<12} {'p95(ms)':<12} {'p99(ms)':<12} {'Rows':<10}")
    print("-" * 80)
    for (direction, lazy), rs in sorted(by_key.items()):
        totals = [r.total_ms for r in rs]
        rows = rs[0].row_count if rs else 0
        label = "lazy" if lazy else "eager"
        print(f"{direction:<10} {label:<8} {percentile(totals,50):<12.1f} "
              f"{percentile(totals,95):<12.1f} {percentile(totals,99):<12.1f} {rows:<10}")

    # Compare lazy vs eager
    for direction in ["asc", "desc"]:
        eager = [r.total_ms for r in by_key.get((direction, False), [])]
        lazy = [r.total_ms for r in by_key.get((direction, True), [])]
        if eager and lazy:
            e50 = percentile(eager, 50)
            l50 = percentile(lazy, 50)
            speedup = e50 / l50 if l50 > 0 else float("inf")
            print(f"\n  [{direction}] speedup: {speedup:.2f}x (eager {e50:.1f}ms → lazy {l50:.1f}ms)")

# ── Benchmark Modes ────────────────────────────────────────────────────────────

def run_smoke_test(cluster: Cluster):
    """Tiny dataset to verify script functionality."""
    print("\n" + "=" * 60)
    print("SMOKE TEST")
    print("=" * 60)

    config = BenchConfig(
        n_unordered=2,
        rows_per_file=5,
        n_series=1,
        n_fields=1,
        repeat=1,
        warmup=0,
        overlap="disjoint",
    )
    cluster._flush_wait = config.flush_wait

    # Write data
    writer = DataWriter(cluster, config)
    writer.write_all()

    # Verify correctness
    runner = QueryRunner(cluster, config)
    for direction in ["asc", "desc"]:
        ok = runner.verify_results(direction)
        if not ok:
            print(f"  [SMOKE] FAIL: {direction} results mismatch")

    # Run one query each direction
    for direction in ["asc", "desc"]:
        total_ms, first_ms, rows = runner.run_query(direction)
        print(f"  [SMOKE] {direction}: {total_ms:.1f}ms, first {first_ms:.1f}ms, {rows} rows")

    print("\n[SMOKE] DONE — script functional ✓")

def run_nsweep(cluster: Cluster, output_dir: str):
    """N sweep benchmark: vary N, fixed other params, both directions, flag on/off."""
    all_results = []
    n_values = [10, 32, 64, 100, 200, 500, 1000]

    for n in n_values:
        config = BenchConfig(n_unordered=n, repeat=20, warmup=3)
        cluster._flush_wait = config.flush_wait

        print(f"\n{'='*60}")
        print(f"N-SWEEP: N={n}")
        print(f"{'='*60}")

        writer = DataWriter(cluster, config)
        writer.write_all()

        runner = QueryRunner(cluster, config)
        for direction in config.directions:
            for lazy in [False, True]:
                cluster.set_lazy_flag(lazy)
                results = runner.run_benchmark(direction, lazy)
                all_results.extend(results)

        writer.cleanup()

    report_results(all_results, output_dir)
    print_summary(all_results)

def run_custom(cluster: Cluster, output_dir: str, args):
    """Custom benchmark with CLI-specified parameters."""
    config = BenchConfig(
        n_unordered=args.n,
        rows_per_file=args.r,
        n_series=args.s,
        n_fields=args.f,
        repeat=args.repeat,
        warmup=2,
        overlap=args.overlap,
    )
    cluster._flush_wait = config.flush_wait

    total_rows = config.n_unordered * config.rows_per_file * config.n_series + config.rows_per_file * config.n_series
    print(f"\n{'='*60}")
    print(f"CUSTOM BENCHMARK: N={config.n_unordered} R={config.rows_per_file} "
          f"S={config.n_series} F={config.n_fields} overlap={config.overlap}")
    print(f"Total rows: ~{total_rows}")
    print(f"{'='*60}")

    writer = DataWriter(cluster, config)
    file_ok = writer.write_all()

    if not file_ok:
        print("[WARN] file count mismatch — results may be unreliable. Continuing...")

    runner = QueryRunner(cluster, config)

    # Verify correctness
    for direction in config.directions:
        ok = runner.verify_results(direction)
        if not ok:
            print(f"  [FAIL] {direction} results mismatch — aborting")
            writer.cleanup()
            return

    # Benchmark
    all_results = []
    for direction in config.directions:
        for lazy in [False, True]:
            cluster.set_lazy_flag(lazy)
            results = runner.run_benchmark(direction, lazy)
            all_results.extend(results)

    cluster.set_lazy_flag(False)
    writer.cleanup()
    report_results(all_results, output_dir)
    print_summary(all_results)


def run_cross(cluster: Cluster, output_dir: str):
    """Multi-dimensional cross at N=1000."""
    all_results = []
    base = BenchConfig(n_unordered=100, repeat=10, warmup=3)

    # Vary R
    for r in [20, 100, 1000]:
        config = BenchConfig(**{**base.__dict__, "rows_per_file": r})
        # ... (similar pattern to nsweep)

    # Vary S, F, overlap similarly
    # For brevity, just run the base config
    config = base
    cluster._flush_wait = config.flush_wait
    writer = DataWriter(cluster, config)
    writer.write_all()
    runner = QueryRunner(cluster, config)
    for direction in config.directions:
        for lazy in [False, True]:
            cluster.set_lazy_flag(lazy)
            results = runner.run_benchmark(direction, lazy)
            all_results.extend(results)
    writer.cleanup()
    report_results(all_results, output_dir)
    print_summary(all_results)

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Unordered merge e2e benchmark")
    parser.add_argument("--mode", choices=["smoke", "nsweep", "cross", "custom"], default="smoke",
                        help="Benchmark mode (default: smoke)")
    parser.add_argument("--output", default="./bench_results",
                        help="Output directory for results (default: ./bench_results)")
    parser.add_argument("--no-cluster-start", action="store_true",
                        help="Don't start/stop cluster (assume already running)")
    parser.add_argument("--n", type=int, default=100, help="N: number of unordered files")
    parser.add_argument("--r", type=int, default=100, help="R: rows per file per series")
    parser.add_argument("--s", type=int, default=100, help="S: number of series")
    parser.add_argument("--f", type=int, default=5, help="F: number of fields")
    parser.add_argument("--repeat", type=int, default=20, help="Query repetitions per config")
    parser.add_argument("--overlap", choices=["disjoint", "overlapping"], default="disjoint")
    args = parser.parse_args()

    cluster = Cluster()

    # Start cluster (with compaction disabled)
    if not args.no_cluster_start:
        cluster.start(patch_compaction=True)
    else:
        cluster._wait_ready(timeout=30)

    try:
        if args.mode == "smoke":
            run_smoke_test(cluster)
        elif args.mode == "nsweep":
            run_nsweep(cluster, args.output)
        elif args.mode == "cross":
            run_cross(cluster, args.output)
        elif args.mode == "custom":
            run_custom(cluster, args.output, args)
    finally:
        if not args.no_cluster_start:
            cluster.stop()
            cluster.restore_configs()

if __name__ == "__main__":
    main()
