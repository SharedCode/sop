package common

// ResetOnIdleTimers resets the last run times for onIdle tasks.
// This is intended for testing purposes only.
func ResetOnIdleTimers() {
	locker.Lock()
	lastOnIdleRunTime = 0
	locker.Unlock()

	priorityLocker.Lock()
	lastPriorityOnIdleTime = 0
	priorityLocker.Unlock()
}
