// Package arena runs small, repeatable workloads against Joltrin's
// in-memory B-tree and reports what it measured. It has no syscall/js
// dependency so it can be tested natively; demo/main.go exposes it to the
// browser build.
package arena

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/sharedcode/joltrin/v5/inmemory"
)

const (
	MinN = 100
	MaxN = 100000

	// ItemsPerNode is fixed by inmemory.NewBtree (its itemsPerNode constant).
	ItemsPerNode = 8

	trials      = 3
	minDuration = 30 * time.Millisecond
	maxTrialOps = 2_000_000
	seed        = 42
)

const (
	ScenarioBasic = "basic"
	ScenarioOrder = "order"
	ScenarioRange = "range"
)

// Measure is one timed workload inside a scenario.
type Measure struct {
	Label       string  `json:"label"`
	Ops         int     `json:"ops"`
	Millis      float64 `json:"millis"`
	OpsPerSec   float64 `json:"opsPerSec"`
	MicrosPerOp float64 `json:"microsPerOp"`
}

// Result is the full answer for one scenario run.
type Result struct {
	Scenario     string    `json:"scenario"`
	N            int       `json:"n"`
	Measures     []Measure `json:"measures"`
	Verified     bool      `json:"verified"`
	Checks       []string  `json:"checks"`
	Trials       int       `json:"trials"`
	KeyType      string    `json:"keyType"`
	ValueType    string    `json:"valueType"`
	ItemsPerNode int       `json:"itemsPerNode"`
	Seed         int64     `json:"seed"`
}

// Run executes one scenario with n keys. n is validated here so no caller can
// ask for more work than the browser can safely do.
func Run(scenario string, n int) (Result, error) {
	if n < MinN || n > MaxN {
		return Result{}, fmt.Errorf("n must be between %d and %d, got %d", MinN, MaxN, n)
	}
	res := Result{
		Scenario: scenario, N: n, Trials: trials, Verified: true,
		KeyType: "int", ValueType: "int", ItemsPerNode: ItemsPerNode, Seed: seed,
	}
	var err error
	switch scenario {
	case ScenarioBasic:
		err = runBasic(&res)
	case ScenarioOrder:
		err = runOrder(&res)
	case ScenarioRange:
		err = runRange(&res)
	default:
		return Result{}, errors.New("unknown scenario")
	}
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func check(res *Result, ok bool, pass, fail string) {
	if ok {
		res.Checks = append(res.Checks, pass)
		return
	}
	res.Verified = false
	res.Checks = append(res.Checks, fail)
}

func sequential(n int) *inmemory.BtreeInterface[int, int] {
	t := inmemory.NewBtree[int, int](true)
	for i := 0; i < n; i++ {
		t.Add(i, i)
	}
	return &t
}

// timeLoop repeats fn until at least minDuration has passed (or the op budget
// is spent) so coarse browser clocks still give a stable reading.
func timeLoop(fn func() int) (ops int, d time.Duration) {
	start := time.Now()
	for {
		ops += fn()
		d = time.Since(start)
		if d >= minDuration || ops >= maxTrialOps {
			return ops, d
		}
	}
}

// measure runs the workload `trials` times and keeps the median trial by
// throughput, which is steadier than the mean when the garbage collector fires.
func measure(label string, fn func() int) Measure {
	type t struct {
		ops int
		d   time.Duration
	}
	all := make([]t, 0, trials)
	for i := 0; i < trials; i++ {
		ops, d := timeLoop(fn)
		all = append(all, t{ops, d})
	}
	rate := func(x t) float64 { return float64(x.ops) / x.d.Seconds() }
	sort.Slice(all, func(i, j int) bool { return rate(all[i]) < rate(all[j]) })
	m := all[len(all)/2]
	ms := float64(m.d.Nanoseconds()) / 1e6
	return Measure{
		Label:       label,
		Ops:         m.ops,
		Millis:      round2(ms),
		OpsPerSec:   math.Round(rate(m)),
		MicrosPerOp: round2(float64(m.d.Nanoseconds()) / 1e3 / float64(m.ops)),
	}
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func runBasic(res *Result) error {
	n := res.N
	res.Measures = append(res.Measures, measure("Insert, keys in order", func() int {
		sequential(n)
		return n
	}))

	t := sequential(n)
	rng := rand.New(rand.NewSource(seed))
	lookups := n
	if lookups > 100000 {
		lookups = 100000
	}
	keys := make([]int, lookups)
	for i := range keys {
		keys[i] = rng.Intn(n)
	}
	allFound := true
	res.Measures = append(res.Measures, measure("Look up random keys", func() int {
		for _, k := range keys {
			if !t.Find(k, true) {
				allFound = false
			}
		}
		return len(keys)
	}))
	check(res, t.Count() == n, fmt.Sprintf("Tree holds %d items", n), "Item count did not match")
	check(res, allFound, "Every lookup found its key", "A lookup missed a key that was inserted")
	return nil
}

func inAscendingOrder(t *inmemory.BtreeInterface[int, int], n int) bool {
	prev, count := -1, 0
	for k := range t.All() {
		if k <= prev {
			return false
		}
		prev = k
		count++
	}
	return count == n
}

func runOrder(res *Result) error {
	n := res.N
	perm := rand.New(rand.NewSource(seed)).Perm(n)
	res.Measures = append(res.Measures,
		measure("Insert, keys in order", func() int {
			sequential(n)
			return n
		}),
		measure("Insert, keys shuffled", func() int {
			t := inmemory.NewBtree[int, int](true)
			for _, k := range perm {
				t.Add(k, k)
			}
			return n
		}))
	shuffled := inmemory.NewBtree[int, int](true)
	for _, k := range perm {
		shuffled.Add(k, k)
	}
	check(res, inAscendingOrder(sequential(n), n), "In-order tree reads back sorted", "In-order tree was not sorted")
	check(res, inAscendingOrder(&shuffled, n), "Shuffled tree also reads back sorted", "Shuffled tree was not sorted")
	return nil
}

func runRange(res *Result) error {
	n := res.N
	t := sequential(n)
	window := n / 100
	if window < 1 {
		window = 1
	}
	from := n / 2
	to := from + window - 1
	var rangeCount, fullCount int
	res.Measures = append(res.Measures,
		measure(fmt.Sprintf("Range scan, %d keys (1%%)", window), func() int {
			rangeCount = 0
			for range t.Range(from, to) {
				rangeCount++
			}
			return rangeCount
		}),
		measure(fmt.Sprintf("Full scan, %d keys", n), func() int {
			fullCount = 0
			for range t.All() {
				fullCount++
			}
			return fullCount
		}))
	check(res, rangeCount == window, fmt.Sprintf("Range returned %d keys", window), fmt.Sprintf("Range returned %d keys, expected %d", rangeCount, window))
	check(res, fullCount == n, fmt.Sprintf("Full scan returned %d keys", n), fmt.Sprintf("Full scan returned %d keys, expected %d", fullCount, n))
	return nil
}
