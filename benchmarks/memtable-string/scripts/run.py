#!/usr/bin/env python3
"""Isolated, bounded TSSTORE string-memory experiment; standard library only."""
import argparse
import base64
import concurrent.futures
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
HTTP = "http://127.0.0.1:18086"
DIAG = "http://127.0.0.1:18090"
MIB = 1024 * 1024


def request(url, data=None, timeout=120):
    with urllib.request.urlopen(url, data=data, timeout=timeout) as response:
        return response.read()


def query(q):
    result = json.loads(request(HTTP + "/query", urllib.parse.urlencode({"db": "memlab", "q": q, "epoch": "ns"}).encode()))
    for item in result.get("results", []):
        if "error" in item:
            raise RuntimeError(item["error"])
    return result


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def rss(pid):
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
        return {k: int(v.strip().split()[0]) * 1024 for line in lines if ":" in line
                for k, v in [line.split(":", 1)] if k in ("VmRSS", "VmHWM", "RssAnon", "RssFile")}
    except FileNotFoundError:
        return {}


def config_text(run, args):
    return f'''[common]
meta-join = ["127.0.0.1:18092"]
ha-policy = "write-available-first"
ignore-empty-tag = true
cpu-num = {args.cpus}
[meta]
bind-address = "127.0.0.1:18088"
http-bind-address = "127.0.0.1:18091"
rpc-bind-address = "127.0.0.1:18092"
dir = "{run}/meta"
ptnum-pernode = 1
[http]
bind-address = "127.0.0.1:18086"
flight-enabled = false
pprof-enabled = true
read-block-size = "{args.read_block}k"
max-body-size = 25000000
[data]
store-ingest-addr = "127.0.0.1:18400"
store-select-addr = "127.0.0.1:18401"
store-data-dir = "{run}/data"
store-wal-dir = "{run}/wal"
store-meta-dir = "{run}/meta"
enable-mmap-read = false
[data.memtable]
shard-mutable-size-limit = "{args.shard_limit}"
node-mutable-size-limit = "2g"
write-cold-duration = "1h"
force-snapShot-duration = "1h"
max-write-hang-time = "15s"
mem-data-read-enabled = true
[data.compact]
compact-full-write-cold-duration = "24h"
[data.wal]
wal-enabled = {str(not args.no_wal).lower()}
wal-sync-interval = "100ms"
wal-replay-async = false
[data.shelf-mode]
enabled = false
[coordinator]
query-timeout = "0s"
[index]
cache-compress-enable = false
[logging]
path = "{run}/logs"
[gossip]
enabled = false
[monitor]
pushers = "file"
store-enabled = true
store-interval = "1s"
store-path = "{run}/metrics/{{{{id}}}}/metric.data"
compress = false
[runtime-config]
enabled = false
[record-write]
enabled = false
[hierarchical_storage]
enabled = false
index-enabled = false
'''


class Experiment:
    def __init__(self, args):
        self.args = args
        self.run = ROOT / "runs" / args.name
        self.run.mkdir(parents=True, exist_ok=False)
        self.profiles = self.run / "profiles"
        self.profiles.mkdir()
        self.process = None
        self.stop = threading.Event()
        self.phase = "startup"
        self.samples = []
        self.checkpoints = []
        self.results = []
        self.base_time = int(time.time() // 3600) * 3600 * 10**9
        rng = random.Random(20260921)
        self.templates = [base64.b64encode(rng.randbytes(args.field_size))[:args.field_size].decode()
                          for _ in range(128)]
        (self.run / "server.conf").write_text(config_text(self.run, args))
        write_json(self.run / "parameters.json", vars(args) | {"timestamp_base": self.base_time,
                   "source_commit": "7fded2ceaf17f70a0cfbd0f8753837c26b5858c5", "go": "go1.26.4"})

    def start(self):
        for port in [18086, 18088, 18090, 18091, 18092, 18400, 18401]:
            with socket.socket() as probe:
                probe.settimeout(.2)
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError(f"experiment port {port} is already in use")
        env = os.environ.copy()
        env.update(OG_MEMDIAG_ADDR="127.0.0.1:18090", GOGC=str(self.args.gogc))
        env["OG_MEMLAB_VARIANT"] = self.args.variant
        env["OG_MEMLAB_PREALLOC"] = str(int(max(float(s) for s in self.args.sizes.split(","))*MIB) + 1024)
        env.pop("GOMEMLIMIT", None)
        if self.args.gomemlimit:
            env["GOMEMLIMIT"] = self.args.gomemlimit
        self.log = (self.run / "console.log").open("ab")
        self.process = subprocess.Popen([self.args.binary, "-config", str(self.run / "server.conf")],
                                        stdout=self.log, stderr=subprocess.STDOUT, env=env)
        (self.run / "pid").write_text(str(self.process.pid) + "\n")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server exited {self.process.returncode}; see {self.run}/console.log")
            try:
                request(HTTP + "/ping", timeout=1)
                self.metrics()
                return
            except Exception:
                time.sleep(.2)
        raise RuntimeError("server startup timeout")

    def metrics(self, detail=False):
        out = json.loads(request(DIAG + "/metrics" + ("?detail=1" if detail else ""), timeout=10))
        out["proc"] = rss(self.process.pid)
        return out

    def monitor(self):
        with (self.run / "timeline.csv").open("w") as f:
            cols = ["time", "phase", "rss", "hwm", "heap", "inuse", "idle", "released", "total_alloc", "gc", "flush"]
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            while not self.stop.is_set():
                try:
                    m = self.metrics()
                    ms = m["memstats"]
                    point = dict(time=time.time(), phase=self.phase, rss=m["proc"].get("VmRSS", 0),
                                 hwm=m["proc"].get("VmHWM", 0), heap=ms["HeapAlloc"], inuse=ms["HeapInuse"],
                                 idle=ms["HeapIdle"], released=ms["HeapReleased"],
                                 total_alloc=ms["TotalAlloc"], gc=ms["NumGC"], flush=m["flush_count"])
                    self.samples.append(point)
                    writer.writerow(point)
                    f.flush()
                    if point["rss"] > 8 * 1024**3:
                        self.process.terminate()
                        raise RuntimeError("experiment exceeded 8 GiB RSS limit")
                except Exception as e:
                    if self.process.poll() is not None:
                        return
                self.stop.wait(.1)

    def checkpoint(self, label, profiles=True):
        self.phase = label
        m = self.metrics(True)
        m["label"] = label
        m["tssp_files"] = [str(p.relative_to(self.run)) for p in (self.run / "data").rglob("*.tssp")]
        write_json(self.run / f"{label}.json", m)
        self.checkpoints.append(m)
        if profiles:
            (self.profiles / f"{label}.heap").write_bytes(request(HTTP + "/debug/pprof/heap"))
        return m

    def gc(self):
        request(DIAG + "/gc", b"")
        request(DIAG + "/gc", b"")

    def payload(self, index):
        if self.args.pattern == "repeated":
            return "x" * self.args.field_size
        value = f"{index:016x}" + self.templates[index % len(self.templates)][16:]
        if self.args.pattern == "escaped":
            value = ('quote"slash\\' + value)[0:self.args.field_size]
        return value

    def batch(self, start, end):
        lines = []
        for i in range(start, end):
            value = self.payload(i).replace("\\", "\\\\").replace('"', '\\"')
            ts_index = i if not self.args.reverse else self.total_rows - i
            lines.append(f'large_field,sid={i % self.args.series} payload="{value}" {self.base_time + ts_index}\n')
        body = "".join(lines).encode()
        begin = time.monotonic()
        request(HTTP + "/write?db=memlab&precision=ns", body)
        return {"rows": end-start, "http_bytes": len(body), "latency_s": time.monotonic()-begin}

    def load(self, start, end):
        begin = time.monotonic()
        result = []
        with concurrent.futures.ThreadPoolExecutor(self.args.concurrency) as executor:
            pending = set()
            for left in range(start, end, self.args.batch):
                pending.add(executor.submit(self.batch, left, min(end, left + self.args.batch)))
                if len(pending) >= 2 * self.args.concurrency:
                    done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                    result.extend(f.result() for f in done)
            result.extend(f.result() for f in concurrent.futures.as_completed(pending))
        duration = time.monotonic() - begin
        result.sort(key=lambda v: v["latency_s"])
        return dict(rows=sum(r["rows"] for r in result), http_bytes=sum(r["http_bytes"] for r in result),
                    duration_s=duration, field_mib_s=(end-start)*self.args.field_size/MIB/duration,
                    p50_s=result[len(result)//2]["latency_s"],
                    p99_s=result[min(len(result)-1, math.ceil(len(result)*.99)-1)]["latency_s"])

    def verify(self, rows):
        count = query("SELECT count(payload) FROM large_field")
        write_json(self.run / f"verification_count_{self.phase}.json", count)
        if not count["results"][0].get("series"):
            raise AssertionError(f"query has no series: {count!r}")
        value = count["results"][0]["series"][0]["values"][0][1]
        if value != rows + 1:
            raise AssertionError(f"row count {value}, expected {rows+1}")
        checked = []
        for index in sorted({0, rows//2, rows-1}):
            ts_index = index if not self.args.reverse else self.total_rows-index
            result = query(f"SELECT payload FROM large_field WHERE time = {self.base_time+ts_index}")
            actual = result["results"][0]["series"][0]["values"][0][1]
            if actual != self.payload(index):
                raise AssertionError(f"payload mismatch at row {index}")
            checked.append(hashlib.sha256(actual.encode()).hexdigest())
        verification = {"count": value, "sample_sha256": checked}
        if self.args.verify_full:
            data = urllib.parse.urlencode({"db": "memlab", "q": "SELECT payload FROM large_field ORDER BY time ASC",
                                           "epoch": "ns", "chunked": "true", "chunk_size": "128"}).encode()
            digests = {}
            with urllib.request.urlopen(HTTP + "/query", data=data, timeout=120) as response:
                for line in response:
                    if not line.strip():
                        continue
                    for result in json.loads(line).get("results", []):
                        if "error" in result:
                            raise AssertionError(result["error"])
                        for series in result.get("series", []):
                            for ts, actual in series["values"]:
                                if ts == self.base_time-1:
                                    if actual != "warmup":
                                        raise AssertionError("warmup changed")
                                    continue
                                index = self.total_rows-(ts-self.base_time) if self.args.reverse else ts-self.base_time
                                if index in digests or not 0 <= index < rows or actual != self.payload(index):
                                    raise AssertionError(f"full verification mismatch at timestamp {ts}")
                                digests[index] = hashlib.sha256(actual.encode()).digest()
            if len(digests) != rows:
                raise AssertionError(f"full verification read {len(digests)} rows, expected {rows}")
            verification["full_rows"] = rows
            verification["full_sha256"] = hashlib.sha256(b"".join(digests[i] for i in range(rows))).hexdigest()
        return verification

    def execute(self):
        self.start()
        sampler = threading.Thread(target=self.monitor, daemon=True)
        sampler.start()
        query("CREATE DATABASE memlab")
        request(HTTP + "/write?db=memlab&precision=ns",
                f'large_field,sid=0 payload="warmup" {self.base_time-1}\n'.encode())
        self.gc()
        base = self.checkpoint("baseline")
        sizes = [float(s) for s in self.args.sizes.split(",")]
        self.total_rows = int(max(sizes)*MIB)//self.args.field_size
        previous = 0
        for size in sizes:
            target = int(size*MIB)//self.args.field_size
            self.phase = f"write_{size:g}"
            result = self.load(previous, target)
            result.update(cumulative_rows=target, payload_bytes=target*self.args.field_size)
            self.results.append(result)
            print(json.dumps({"run": self.args.name, **result}), flush=True)
            raw = self.checkpoint(f"loaded_{size:g}")
            if not self.args.allow_flush and (raw["flush_count"] != base["flush_count"] or raw["tssp_files"]):
                raise AssertionError("unexpected snapshot/flush in retained-memtable experiment")
            self.gc()
            self.checkpoint(f"gc_{size:g}")
            previous = target
        if self.args.free_os_memory:
            request(DIAG + "/free-os-memory", b"")
            self.checkpoint("scavenged")
        if self.args.query_before_flush:
            self.phase = "query_memory"
            # MergeSet makes newly created series visible asynchronously. Wait
            # for its background flush without flushing the memtable itself.
            time.sleep(2)
            self.verify(previous)
            self.checkpoint("query_before_gc")
            self.gc()
            self.checkpoint("query_after_gc")
        if self.args.recovery:
            # Only the isolated child process is killed, after the WAL sync
            # interval. This preserves an unflushed WAL for crash-recovery tests.
            self.phase = "before_crash"
            time.sleep(.3)
            self.stop.set()
            sampler.join(15)
            self.process.kill()
            self.process.wait()
            self.log.close()
            self.start()
            time.sleep(2)
            write_json(self.run / "recovery_verification.json", self.verify(previous))
            self.gc()
            self.checkpoint("recovered")
            self.stop.clear()
            sampler = threading.Thread(target=self.monitor, daemon=True)
            # Keep the write timeline intact; recovery isn't used for peak ratios.
        if not self.args.no_flush:
            self.phase = "flush"
            request(HTTP + "/debug/ctrl?mod=flush", b"")
            self.checkpoint("flushed")
            self.gc()
            self.checkpoint("flushed_gc")
        self.phase = "verify"
        verify = self.verify(previous)
        summary = dict(parameters=vars(self.args), writes=self.results, verify=verify,
                       baseline=base, checkpoints=self.checkpoints,
                       peak_write_rss=max((s["rss"] for s in self.samples if s["phase"].startswith("write_")), default=0),
                       peak_write_heap=max((s["heap"] for s in self.samples if s["phase"].startswith("write_")), default=0),
                       peak_all_rss=max((s["rss"] for s in self.samples), default=0))
        write_json(self.run / "summary.json", summary)
        print(json.dumps({"run": self.args.name, "status": "passed", "directory": str(self.run)}), flush=True)
        self.stop.set()
        if sampler.ident is not None:
            sampler.join(15)

    def close(self):
        self.stop.set()
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if hasattr(self, "log"):
            self.log.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True)
    p.add_argument("--binary", default=str(ROOT / "bin" / "ts-server-baseline"))
    p.add_argument("--variant", default="")
    p.add_argument("--sizes", default="256")
    p.add_argument("--field-size", type=int, default=10240)
    p.add_argument("--series", type=int, default=1)
    p.add_argument("--batch", type=int, default=100)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--pattern", choices=["random", "repeated", "escaped"], default="random")
    p.add_argument("--cpus", type=int, default=0)
    p.add_argument("--gogc", type=int, default=100)
    p.add_argument("--gomemlimit", default="")
    p.add_argument("--shard-limit", default="1g")
    p.add_argument("--allow-flush", action="store_true")
    p.add_argument("--read-block", type=int, default=64)
    p.add_argument("--no-wal", action="store_true")
    p.add_argument("--no-flush", action="store_true")
    p.add_argument("--reverse", action="store_true")
    p.add_argument("--query-before-flush", action="store_true")
    p.add_argument("--free-os-memory", action="store_true")
    p.add_argument("--verify-full", action="store_true")
    p.add_argument("--recovery", action="store_true")
    args = p.parse_args()
    if not args.name.replace("-", "").replace("_", "").isalnum():
        p.error("name must consist of letters, digits, hyphens or underscores")
    if min(args.field_size, args.series, args.batch, args.concurrency) <= 0 or args.field_size < 16:
        p.error("positive sizes required; field size must be at least 16")
    if args.field_size*args.batch*2 > 25000000:
        p.error("batch could exceed HTTP body limit")
    if args.no_wal and args.recovery:
        p.error("WAL recovery requires WAL to be enabled")
    if "prealloc" in args.variant and args.series != 1:
        p.error("oracle preallocation is only bounded for the single-series experiment")
    exp = Experiment(args)
    try:
        exp.execute()
    except BaseException as e:
        write_json(exp.run / "error.json", {"error": repr(e)})
        raise
    finally:
        exp.close()


if __name__ == "__main__":
    main()
