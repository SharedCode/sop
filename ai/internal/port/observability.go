package port

type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Meter interface {
	Inc(counter string, delta float64, labels map[string]string)
	Obs(hist string, value float64, labels map[string]string)
	Gauge(name string, value float64, labels map[string]string)
}

type Tracer interface {
	StartSpan(name string, kv map[string]any) Span
}

type Span interface {
	AddEvent(name string, kv map[string]any)
	End(err error)
}
