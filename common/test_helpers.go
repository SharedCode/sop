package common

// ResetOnIdleTimers resets the last run times for onIdle tasks.
// This is intended for testing purposes only.
func ResetOnIdleTimers() {
	func() {
		locker.Lock()
		defer locker.Unlock()
		lastOnIdleRunTime = 0
	}()

	func() {
		priorityLocker.Lock()
		defer priorityLocker.Unlock()
		lastPriorityOnIdleTime = 0
	}()
}
