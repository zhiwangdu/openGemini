//go:build !memdiag

package memexperiment

const (
	DoubleGrowth  = false
	DirectAppend  = false
	SingleMarshal = false
	ReleaseSort   = false
	Preallocate   = 0
	PoolLimit     = -1
)
