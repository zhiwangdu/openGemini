package influx

import "github.com/openGemini/openGemini/lib/memexperiment"

var unmarshalWorkPool chan *unmarshalWork

func GetUnmarshalWork() *unmarshalWork {
	select {
	case uw := <-unmarshalWorkPool:
		return uw
	default:
		return &unmarshalWork{}
	}
}

func putUnmarshalWork(uw *unmarshalWork) {
	uw.reset()
	if memexperiment.PoolLimit >= 0 && cap(uw.ReqBuf) > memexperiment.PoolLimit {
		uw.ReqBuf = nil
	}
	select {
	case unmarshalWorkPool <- uw:
	default:
	}
}
