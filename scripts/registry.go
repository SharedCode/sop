package scripts

import (
	"context"
	"sync"
)

// ScriptFunc is the signature for callable Go functions from the script engine.
type ScriptFunc func(context.Context, map[string]any) (any, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]ScriptFunc)
)

// Register registers a new native script function.
func Register(name string, fn ScriptFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = fn
}

// Get retrieves a registered native script function.
func Get(name string) (ScriptFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	fn, ok := registry[name]
	return fn, ok
}

// List returns all registered function names.
func List() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for k := range registry {
		names = append(names, k)
	}
	return names
}
