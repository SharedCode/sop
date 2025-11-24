package obs

import "github.com/sharedcode/sop/ai/internal/port"

type NoopMeter struct{}

func (n NoopMeter) Inc(string, float64, map[string]string)   {}
func (n NoopMeter) Obs(string, float64, map[string]string)   {}
func (n NoopMeter) Gauge(string, float64, map[string]string) {}

type NoopTracer struct{}

func (n NoopTracer) StartSpan(string, map[string]any) port.Span { return NoopSpan{} }

type NoopSpan struct{}

func (n NoopSpan) AddEvent(string, map[string]any) {}
func (n NoopSpan) End(error)                       {}
