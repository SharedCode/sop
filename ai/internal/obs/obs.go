package obs

import "github.com/sharedcode/sop/ai/internal/port"

var (
	logImpl   port.Logger
	meterImpl port.Meter
	traceImpl port.Tracer
)

func Init(l port.Logger, m port.Meter, t port.Tracer) { logImpl, meterImpl, traceImpl = l, m, t }

func Log() port.Logger   { return logImpl }
func Meter() port.Meter  { return meterImpl }
func Trace() port.Tracer { return traceImpl }
