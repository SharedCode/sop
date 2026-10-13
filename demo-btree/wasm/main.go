//go:build js && wasm

// Command wasm is the browser entry point for the Joltrin Arena B-tree challenge. It exposes one
// function, joltrinBtreeArena(scenario, n), that returns a JSON string.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/sharedcode/joltrin/v5/demo-btree/arena"
)

func run(this js.Value, args []js.Value) any {
	fail := func(msg string) any {
		b, _ := json.Marshal(map[string]string{"error": msg})
		return string(b)
	}
	if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeNumber {
		return fail("expected (scenario string, n number)")
	}
	res, err := arena.Run(args[0].String(), args[1].Int())
	if err != nil {
		return fail(err.Error())
	}
	b, _ := json.Marshal(res)
	return string(b)
}

func main() {
	js.Global().Set("joltrinBtreeArena", js.FuncOf(run))
	js.Global().Set("__BTREE_ARENA_READY__", js.ValueOf(true))
	select {}
}
