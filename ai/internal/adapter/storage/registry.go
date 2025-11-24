package storage

import (
	"fmt"

	"github.com/sharedcode/sop/ai/internal/port"
)

type Factory func(cfg map[string]any) (port.KVStore, error)

var stores = map[string]Factory{}

func Register(name string, f Factory) { stores[name] = f }
func Open(name string, cfg map[string]any) (port.KVStore, error) {
	f, ok := stores[name]
	if !ok {
		return nil, fmt.Errorf("unknown store driver: %s", name)
	}
	return f(cfg)
}
