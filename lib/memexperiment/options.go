//go:build memdiag

// Package memexperiment controls isolated investigation builds, not production.
package memexperiment

import (
	"os"
	"strconv"
	"strings"
)

var variants = "," + os.Getenv("OG_MEMLAB_VARIANT") + ","

func enabled(name string) bool { return strings.Contains(variants, ","+name+",") }

var (
	DoubleGrowth  = enabled("doubling")
	DirectAppend  = enabled("append") || DoubleGrowth || enabled("prealloc")
	SingleMarshal = enabled("singlemarshal")
	ReleaseSort   = enabled("trimsort")
	Preallocate   = func() int {
		if !enabled("prealloc") {
			return 0
		}
		n, _ := strconv.Atoi(os.Getenv("OG_MEMLAB_PREALLOC"))
		return n
	}()
	PoolLimit = func() int {
		if enabled("nopools") {
			return 0
		}
		if enabled("pool64k") {
			return 64 * 1024
		}
		return -1
	}()
)
