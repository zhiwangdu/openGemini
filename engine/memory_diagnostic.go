//go:build memdiag

package engine

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

// This private listener is compiled only in experimental binaries. It is never
// enabled without an explicit loopback address supplied by the experiment runner.
func init() {
	addr := os.Getenv("OG_MEMDIAG_ADDR")
	if addr == "" {
		return
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		panic("OG_MEMDIAG_ADDR must be a loopback IP address")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		out := map[string]any{
			"time_ns": time.Now().UnixNano(), "memstats": ms,
			"flush_count":     atomic.LoadInt64(&statistics.PerfStat.FlushSnapshotCount),
			"write_rows":      atomic.LoadInt64(&statistics.PerfStat.WriteRowsCount),
			"gomaxprocs":      runtime.GOMAXPROCS(0),
			"memtable_config": config.GetMemTableConfig(),
		}
		if r.URL.Query().Get("detail") == "1" {
			type shardMemory struct {
				ID       uint64
				Active   mutable.DiagnosticMemory
				Snapshot mutable.DiagnosticMemory
			}
			var shards []shardMemory
			compWorker.mu.RLock()
			for id, sh := range compWorker.sources {
				sh.snapshotLock.RLock()
				shards = append(shards, shardMemory{id, sh.activeTbl.DiagnosticMemory(), sh.snapshotTbl.DiagnosticMemory()})
				sh.snapshotLock.RUnlock()
			}
			compWorker.mu.RUnlock()
			out["shards"] = shards
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/gc", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		runtime.GC()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/free-os-memory", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		debug.FreeOSMemory()
		w.WriteHeader(http.StatusNoContent)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	go func() {
		_ = (&http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}).Serve(ln)
	}()
}
