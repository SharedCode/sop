package functions

import "github.com/sharedcode/sop/ai/internal/port"

type FnFactory func(cfg map[string]any) (port.DomainFunction, error)

var fns = map[string]FnFactory{}

func Register(id string, f FnFactory) { fns[id] = f }

func Get(id string) port.DomainFunction {
	if ff, ok := fns[id]; ok {
		fn, _ := ff(nil)
		return fn
	}
	return nil
}

func List() []string {
	out := make([]string, 0, len(fns))
	for k := range fns {
		out = append(out, k)
	}
	return out
}
