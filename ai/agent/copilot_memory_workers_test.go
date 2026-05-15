package agent

import (
	"testing"
	"time"
)

func TestShouldTriggerQuickNap(t *testing.T) {
	now := time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)

	// Test 1: No last log, shouldn't trigger
	if shouldTriggerQuickNap(0, 5, now) {
		t.Errorf("Should not trigger when lastLog is 0")
	}

	// Test 2: Timeout is 0
	if shouldTriggerQuickNap(now.UnixMilli(), 0, now) {
		t.Errorf("Should not trigger when timeout is 0")
	}

	// Test 3: Idle time exactly matching timeout
	lastLog := now.Add(-5 * time.Minute).UnixMilli()
	if !shouldTriggerQuickNap(lastLog, 5, now) {
		t.Errorf("Should trigger when idle time equals timeout")
	}

	// Test 4: Idle time significantly exceeding timeout
	lastLog = now.Add(-10 * time.Minute).UnixMilli()
	if !shouldTriggerQuickNap(lastLog, 5, now) {
		t.Errorf("Should trigger when idle time exceeds timeout")
	}

	// Test 5: Idle time not quite reaching the timeout
	lastLog = now.Add(-4 * time.Minute).UnixMilli()
	if shouldTriggerQuickNap(lastLog, 5, now) {
		t.Errorf("Should not trigger if idle time is less than timeout")
	}
}

func TestShouldTriggerDeepSleep(t *testing.T) {
	// Midnight check
	if !shouldTriggerDeepSleep(0, 0) {
		t.Errorf("Should always trigger deep sleep at midnight (0)")
	}

	// 1 Hour interval check
	if !shouldTriggerDeepSleep(3, 1) {
		t.Errorf("Should trigger deep sleep every hour (interval: 1)")
	}

	// 6 Hour interval check
	if !shouldTriggerDeepSleep(12, 6) {
		t.Errorf("Should trigger at hour 12 for interval 6")
	}
	if shouldTriggerDeepSleep(15, 6) {
		t.Errorf("Should not trigger at hour 15 for interval 6")
	}

	// No interval provided
	if shouldTriggerDeepSleep(15, 0) {
		t.Errorf("Should not trigger at hour 15 when interval is 0")
	}
}
