package sop

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type CacheRestartHelper struct {
	cache Cache
	// Atomic restart detection state (lock-free snapshot + guard)
	restartState    atomic.Value // stores restartState
	restartChecking atomic.Int32 // 0 or 1 when a goroutine is performing a check
}

// restartState holds the snapshot for restart detection.
type restartState struct {
	lastCheck int64  // unix nano timestamp of last completed check
	runID     string // last observed run_id
	cycle     uint64 // number of completed cycles
}

const(
	UnifiedCoordinatorLockName = "Prbs"
	restartSentinelKey   = "sentl"
)

// Restart detection configuration.
var (
	restartCheckInterval = 2 * time.Second
	restartInfoEveryN    = 2 // every 2nd interval -> INFO
)

func NewCacheRestartHelper(cache Cache) *CacheRestartHelper {
	return &CacheRestartHelper{
		cache: cache,
	}
}

// IsRestarted returns true if the Redis server run_id has changed since the previous call.
// It now alternates between lightweight sentinel key checks and INFO calls.
// Sentinel key holds the last observed run_id to make cheap cycles O(1 GET) and
// expensive verification cycles every N intervals. Missing sentinel forces an
// INFO confirmation (to avoid false positives on eviction).
func (c *CacheRestartHelper) IsRestarted(ctx context.Context) (bool, error) {
	// If a cluster-wide restart window is active, always report restarted so higher layers can freeze
	if active, err := c.cache.IsLockedByOthers(ctx, []string{c.cache.FormatLockKey(UnifiedCoordinatorLockName)}); err == nil && active {
		return true, nil
	}
	// Initialize state lazily.
	if c.restartState.Load() == nil {
		c.restartState.Store(restartState{})
	}

	nowNano := time.Now().UnixNano()
	st := c.restartState.Load().(restartState)
	if st.lastCheck != 0 && time.Duration(nowNano-st.lastCheck) < restartCheckInterval {
		return false, nil
	}
	// Attempt to become the checker.
	if !c.restartChecking.CompareAndSwap(0, 1) {
		return false, nil
	}
	// Ensure flag cleared.
	defer c.restartChecking.Store(0)

	// Re-load in case another thread updated before we acquired flag (unlikely but safe)
	st = c.restartState.Load().(restartState)
	if st.lastCheck != 0 && time.Duration(nowNano-st.lastCheck) < restartCheckInterval {
		return false, nil
	}

	cycle := st.cycle + 1
	prevRunID := st.runID
	needInfo := restartInfoEveryN > 0 && (cycle%uint64(restartInfoEveryN) == 0)

	sentinelVal := ""
	sentinelExists := false
	if r, v, err := c.cache.Get(ctx, restartSentinelKey); err == nil && r {
		sentinelExists = true
		sentinelVal = v
	}
	if !sentinelExists {
		needInfo = true // confirm via INFO when missing
	}

	var newRunID = prevRunID
	restarted := false

	if needInfo {
		runID, err := c.fetchRunID(ctx)
		if err != nil {
			// Do not advance lastCheck so another caller can retry sooner.
			return false, err
		}
		if prevRunID != "" && runID != prevRunID {
			restarted = true
		}
		newRunID = runID
		_ = c.cache.Set(ctx, restartSentinelKey, runID, 0)
	} else {
		// Cheap cycle: detect if another process updated sentinel (new runID there)
		if sentinelExists && prevRunID != "" && sentinelVal != "" && sentinelVal != prevRunID {
			newRunID = sentinelVal
			restarted = true
		}
	}

	// Publish new snapshot.
	c.restartState.Store(restartState{lastCheck: nowNano, runID: newRunID, cycle: cycle})
	return restarted, nil
}

// fetchRunID performs an INFO server call and extracts run_id.
func (c *CacheRestartHelper) fetchRunID(ctx context.Context) (string, error) {
	info, err := c.cache.Info(ctx, "server")
	if err != nil {
		return "", err
	}
	runID := ""
	for _, line := range splitLines(info) {
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if len(line) > 7 && line[:7] == "run_id:" {
			runID = line[7:]
			break
		}
	}
	if runID == "" {
		return "", fmt.Errorf("unable to read run_id from INFO server response")
	}
	return runID, nil
}

// SetRestartCheckInterval sets minimum delay between restart detection cycles.
func SetRestartCheckInterval(d time.Duration) {
	if d > 0 {
		restartCheckInterval = d
	}
}

// SetRestartInfoEveryN sets how many cycles between INFO validations (>=1).
func SetRestartInfoEveryN(n int) {
	if n >= 1 {
		restartInfoEveryN = n
	}
}

// splitLines is a tiny helper to avoid strings import bloat here.
func splitLines(s string) []string {
	// INFO uses \r\n line endings typically; support both.
	lines := make([]string, 0, 32)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			// trim optional carriage return
			end := i
			if end > start && s[end-1] == '\r' {
				end--
			}
			lines = append(lines, s[start:end])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
