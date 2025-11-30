package main

import (
	"sync"

	"github.com/sharedcode/sop"
)

// Registry is a thread-safe generic registry for storing objects by UUID.
type Registry[T any] struct {
	lookup map[sop.UUID]T
	mu     sync.Mutex
}

// NewRegistry creates a new instance of Registry.
func NewRegistry[T any]() *Registry[T] {
	return &Registry[T]{
		lookup: make(map[sop.UUID]T),
	}
}

// Add registers an item and returns its new UUID.
func (r *Registry[T]) Add(item T) sop.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := sop.NewUUID()
	r.lookup[id] = item
	return id
}

// Set registers an item with a specific UUID.
func (r *Registry[T]) Set(id sop.UUID, item T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lookup[id] = item
}

// Get retrieves an item by UUID.
func (r *Registry[T]) Get(id sop.UUID) (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.lookup[id]
	return item, ok
}

// Remove removes an item by UUID.
func (r *Registry[T]) Remove(id sop.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lookup, id)
}

// TransactionItem holds a transaction and its associated B-trees.
type TransactionItem struct {
	Transaction sop.Transaction
	Btrees      map[sop.UUID]any
}

// TransactionRegistry manages transactions and their associated B-trees.
type TransactionRegistry struct {
	lookup map[sop.UUID]*TransactionItem
	mu     sync.Mutex
}

// NewTransactionRegistry creates a new TransactionRegistry.
func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		lookup: make(map[sop.UUID]*TransactionItem),
	}
}

// Add registers a transaction and returns its UUID.
func (tr *TransactionRegistry) Add(t sop.Transaction) sop.UUID {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	id := sop.NewUUID()
	tr.lookup[id] = &TransactionItem{
		Transaction: t,
		Btrees:      make(map[sop.UUID]any),
	}
	return id
}

// Get retrieves a transaction by UUID.
func (tr *TransactionRegistry) Get(id sop.UUID) (sop.Transaction, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	item, ok := tr.lookup[id]
	if !ok {
		return nil, false
	}
	return item.Transaction, true
}

// GetItem retrieves the transaction item (Transaction + Btree Map).
func (tr *TransactionRegistry) GetItem(id sop.UUID) (*TransactionItem, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	item, ok := tr.lookup[id]
	return item, ok
}

// Remove removes a transaction and all its associated B-trees.
func (tr *TransactionRegistry) Remove(id sop.UUID) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	delete(tr.lookup, id)
}

// AddBtree adds a B-tree (or similar object) to a transaction.
func (tr *TransactionRegistry) AddBtree(transID sop.UUID, btree any) (sop.UUID, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	item, ok := tr.lookup[transID]
	if !ok {
		return sop.NilUUID, nil
	}
	btreeID := sop.NewUUID()
	item.Btrees[btreeID] = btree
	return btreeID, nil
}

// GetBtree retrieves a B-tree from a transaction.
func (tr *TransactionRegistry) GetBtree(transID, btreeID sop.UUID) (any, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	item, ok := tr.lookup[transID]
	if !ok {
		return nil, false
	}
	btree, ok := item.Btrees[btreeID]
	return btree, ok
}

// RemoveBtree removes a B-tree from a transaction.
func (tr *TransactionRegistry) RemoveBtree(transID, btreeID sop.UUID) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	item, ok := tr.lookup[transID]
	if !ok {
		return
	}
	delete(item.Btrees, btreeID)
}
