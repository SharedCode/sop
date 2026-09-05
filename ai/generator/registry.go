package generator

import (
	"fmt"

	"github.com/sharedcode/joltrin/ai"
)

type Factory func(cfg map[string]any) (ai.Generator, error)

var registry = map[string]Factory{}

func Register(name string, f Factory) {
	registry[name] = f
}

func New(name string, cfg map[string]any) (ai.Generator, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("generator %q not found", name)
	}
	return f(cfg)
}
