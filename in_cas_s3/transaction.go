package in_cas_s3

import (
	"fmt"
	"time"

	"github.com/SharedCode/sop/btree"
)

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	HasBegun() bool
}

type transaction struct {
	// stores(or its items) accessed/managed within the transaction session.
	stores     []StoreInterface[interface{}, interface{}]
	forWriting bool
	hasBegun   bool
	done       bool
	maxTime    time.Duration
}

// Use lambda for time.Now so automated test can replace with replayable time if needed.
var getCurrentTime = time.Now

// NewTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Defaults to 15 minutes session duration.
func NewTransaction(forWriting bool) Transaction {
	return NewTransactionWithMaxSessionTime(forWriting, -1)
}
func NewTransactionWithMaxSessionTime(forWriting bool, maxTime time.Duration) Transaction {
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		forWriting: forWriting,
		maxTime:    maxTime,
	}
}

func (t *transaction) Begin() error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if t.HasBegun() {
		return fmt.Errorf("Transaction is ongoing, 'can't begin again.")
	}
	t.hasBegun = true
	return nil
}

func (t *transaction) Commit() error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction.")
	}
	t.hasBegun = false
	t.done = true
	return t.commit()
}

func (t *transaction) Rollback() error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to rollback, call Begin to start a transaction.")
	}
	t.hasBegun = false
	t.done = true
	return t.rollback()
}

func (t *transaction) HasBegun() bool {
	return t.hasBegun
}

func (t *transaction) commit() error {
	var updatedNodes, removedNodes, addedNodes []nodeEntry
	startTime := getCurrentTime()

	// For writer transaction. Save the managed Node(s) as inactive:
	// NOTE: a transaction Commit can timeout and thus, rollback if it exceeds the maximum time(defaults to 30 mins).
	// Return error to trigger rollback for any operation that fails.
	//
	// - Create a lookup table of added/updated/removed items together with their Nodes
	//   Specify whether Node is updated, added or removed
	// * Repeat until timeout, for updated Nodes:
	// - Upsert each Node from the lookup to blobStore(Add only if blobStore is S3)
	// - Log UUID in transaction rollback log categorized as updated Node
	// - Compare each updated Node to Redis copy if identical(active UUID is same)
	//   NOTE: added Node(s) don't need this logic.
	//   For identical Node(s), update the "inactive UUID" with the Node's UUID(in redis).
	//   Collect each Node that are different in Redis(as updated by other transaction(s))
	//   Gather all the items of these Nodes(using the lookup table)
	//   Break if there are no more items different.
	// - Re-fetch the Nodes of these items, re-create the lookup table consisting only of these items & their re-fetched Nodes
	// - Loop end.
	// - Return error if loop timed out to trigger rollback.
	for {
		if getCurrentTime().Sub(startTime).Minutes() > float64(t.maxTime) {
			return fmt.Errorf("Transaction timed out(maxTime=%v).", t.maxTime)
		}
		if t.trackedItemsHasConflict() {
			if rerr := t.rollback(); rerr != nil {
				return fmt.Errorf("Item(s) that were modified as well by another transaction was detected, and rollback failed as well, details: %v.", rerr)
			}
			return fmt.Errorf("Item(s) that were modified as well by another transaction was detected, 'failing commit as violating Isolation & affecting Consistency.")
		}

		if !t.forWriting {
			// Reader transaction only checks tracked items consistency, return success at this point.
			return nil
		}

		updatedNodes, removedNodes, addedNodes = t.classifyModifiedNodes()
		t.saveUpdatedNodes(updatedNodes)

		if t.countDiffsWithRedisNodes(updatedNodes) == 0 {
			break
		}
	}

	t.saveRemovedNodes(removedNodes)
	t.saveAddedNodes(addedNodes)
	stores := t.getModifiedStores()
	t.saveStores(stores)

	// Mark these items as locked in Redis.
	// Return error to rollback if any failed to lock as alredy locked by another transaction. Or if Redis fetch failed(error).
	if !t.manageTrackedItemsLocking(true) {
		return fmt.Errorf("Tracked items within the transaction failed to lock, thus, conflict with other transaction changes is presumed.")
	}

	t.setActiveModifiedInactiveNodes(updatedNodes)
	t.cleanup()
	return nil
}

type nodeEntry struct {
	nodeId btree.UUID
	node   interface{}
}

func (t *transaction) cleanup() {
	t.deleteTransactionLogs()
	t.manageTrackedItemsLocking(false)
	// TODO: delete cached data sets on this transaction.
}

func (t *transaction) deleteTransactionLogs() {

}

func (t *transaction) setActiveModifiedInactiveNodes([]nodeEntry) {
}

func (t *transaction) saveStores([]btree.StoreInfo) {
}
func (t *transaction) saveUpdatedNodes([]nodeEntry) {
}
func (t *transaction) countDiffsWithRedisNodes([]nodeEntry) int {
	return 0
}
func (t *transaction) saveRemovedNodes([]nodeEntry) {
}
func (t *transaction) saveAddedNodes([]nodeEntry) {
}

func (t *transaction) getModifiedStores() []btree.StoreInfo {
	return nil
}

// classifyModifiedNodes will classify modified Nodes into 3 tables & return them:
// a. updated Nodes, b. removed Nodes, c. added Nodes.
func (t *transaction) classifyModifiedNodes() ([]nodeEntry, []nodeEntry, []nodeEntry) {
	// for  t.stores
	return nil, nil, nil
}

func (t *transaction) rollback() error {
	// TODO
	t.cleanup()
	return nil
}

func (t *transaction) manageTrackedItemsLocking(lockOrUnlock bool) bool {
	// TODO: Lock items tracked.

	// If action is to lock, re-check (again) after locking if tracked items has conflict.
	if lockOrUnlock && t.trackedItemsHasConflict() {
		// TODO: unlock before returning failure.
		return false
	}
	return true
}

// Check all explicitly fetched(i.e. - GetCurrentKey/GetCurrentValue invoked) & managed(add/update/remove) items
// if they have the expected version number. If different, rollback.
// Compare local vs redis/blobStore copy and see if different. Read from blobStore if not found in redis.
// Commit to return error if there is at least an item with different version no. as compared to
// local cache's copy.
func (t *transaction) trackedItemsHasConflict() bool {
	// TODO
	return false
}
