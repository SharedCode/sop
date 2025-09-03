package sop

import (
	"context"
	"io"
	"time"
)

// RegistryPayload represents a request/response payload to manage or fetch Handles/UUIDs in a registry table.
// T can be either Handle (for writes) or UUID (for reads/deletes).
type RegistryPayload[T Handle | UUID] struct {
	// RegistryTable is the table (or namespace) where the virtual IDs are stored or fetched.
	RegistryTable string

	// BlobTable is the paired blob table (or base filesystem path) used during Rollback and Commit.
	BlobTable string
	// CacheDuration specifies Redis cache duration.
	CacheDuration time.Duration
	// IsCacheTTL enables Redis TTL (sliding expiration) semantics when true.
	IsCacheTTL bool

	// IDs contains the virtual IDs (or Handles) to manage.
	IDs []T
}

// Registry provides CRUD and replication operations for virtual ID management that back SOP's ACID workflow.
// All methods accept and/or return batches.
type Registry interface {
	// Get fetches Handles (given logical IDs) from registry table(s).
	Get(context.Context, []RegistryPayload[UUID]) ([]RegistryPayload[Handle], error)
	// Add inserts Handles into registry table(s).
	Add(context.Context, []RegistryPayload[Handle]) error
	// Update modifies Handles across registry table(s) and acquires cache locks for each Handle.
	Update(ctx context.Context, handles []RegistryPayload[Handle]) error
	// UpdateNoLocks updates Handles in an active transaction where locks were pre-acquired by the transaction manager.
	UpdateNoLocks(ctx context.Context, allOrNothing bool, storesHandles []RegistryPayload[Handle]) error
	// Remove deletes Handles (given logical IDs) from registry table(s).
	Remove(context.Context, []RegistryPayload[UUID]) error

	// Replicate performs post-commit replication of blobs/data to passive targets.
	Replicate(ctx context.Context, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []RegistryPayload[Handle]) error
}

// ManageStore declares lifecycle operations for creating and removing store containers (e.g., folders).
type ManageStore interface {
	// CreateStore creates the store(s) container (e.g., a filesystem folder).
	CreateStore(context.Context, string) error
	// RemoveStore removes the store(s) container (e.g., a filesystem folder).
	RemoveStore(context.Context, string) error
}

// BlobStore defines CRUD operations for binary blobs that are too large for typical databases
// and are stored in external systems (e.g., S3, filesystem, Cassandra partitions).
type BlobStore interface {
	// GetOne fetches a blob by ID from a blob table.
	GetOne(ctx context.Context, blobTable string, blobID UUID) ([]byte, error)
	// Add inserts blobs.
	Add(ctx context.Context, blobs []BlobsPayload[KeyValuePair[UUID, []byte]]) error
	// Update modifies existing blobs.
	Update(ctx context.Context, blobs []BlobsPayload[KeyValuePair[UUID, []byte]]) error
	// Remove deletes blobs by ID.
	Remove(ctx context.Context, blobsIDs []BlobsPayload[UUID]) error
}

// BlobsPayload is a request/response envelope for blob operations.
type BlobsPayload[T UUID | KeyValuePair[UUID, []byte]] struct {
	// BlobTable is the blob store table name (or base filesystem path).
	BlobTable string
	// Blobs holds either IDs (for deletes) or ID+data pairs (for upserts).
	Blobs []T
}

// TransactionPriorityLog records prioritised transaction logs used for recovery and replication workflows.
type TransactionPriorityLog interface {
	// IsEnabled reports whether priority logging is enabled.
	IsEnabled() bool
	// Add appends a priority log for a transaction.
	Add(ctx context.Context, tid UUID, payload []byte) error
	// Remove deletes priority log file of a transaction.
	Remove(ctx context.Context, tid UUID) error
	// Get retrieves priority log details for a transaction.
	Get(ctx context.Context, tid UUID) ([]RegistryPayload[Handle], error)

	// GetBatch fetches up to batchSize of the oldest (older than 2 minutes) priority logs for processing.
	GetBatch(ctx context.Context, batchSize int) ([]KeyValuePair[UUID, []RegistryPayload[Handle]], error)

	// LogCommitChanges writes a special commit-change log used during drive reinstate for replication.
	LogCommitChanges(ctx context.Context, stores []StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []RegistryPayload[Handle]) error

	// ClearRegistrySectorClaims clears all per-sector claim markers used to coordinate exclusive
	// access to registry file sector CUD operations. Implementations should make best-effort and
	// return nil if there is nothing to clear.
	ClearRegistrySectorClaims(ctx context.Context) error
}

// TransactionLog persists transaction steps and provides job-distribution accessors for cleanup tasks.
type TransactionLog interface {
	// PriorityLog returns the priority logger implementation.
	PriorityLog() TransactionPriorityLog
	// Add appends a transaction log entry.
	Add(ctx context.Context, tid UUID, commitFunction int, payload []byte) error
	// Remove deletes all logs for a transaction.
	Remove(ctx context.Context, tid UUID) error

	// GetOne returns the oldest hour bucket (older than 1 hour) and its logs for cleanup distribution.
	GetOne(ctx context.Context) (UUID, string, []KeyValuePair[int, []byte], error)

	// GetOneOfHour returns the available cleanup logs for a specific hour bucket.
	GetOneOfHour(ctx context.Context, hour string) (UUID, []KeyValuePair[int, []byte], error)

	// NewUUID generates a UUID suitable for the logging backend (e.g., time-based in Cassandra).
	NewUUID() UUID
}

// StoreRepository specifies CRUD and replication methods for StoreInfo records.
type StoreRepository interface {
	// Get retrieves store info by name(s).
	Get(context.Context, ...string) ([]StoreInfo, error)
	// GetWithTTL retrieves store info using TTL/sliding cache semantics.
	GetWithTTL(context.Context, bool, time.Duration, ...string) ([]StoreInfo, error)
	// GetAll lists all store names available in the backend.
	GetAll(context.Context) ([]string, error)
	// Add creates new store info entries and related tables (registry/blob).
	Add(context.Context, ...StoreInfo) error
	// Remove deletes store info by name and drops related tables.
	Remove(context.Context, ...string) error

	// Update modifies store info and reconciles Count using CountDelta.
	Update(context.Context, []StoreInfo) ([]StoreInfo, error)
	// Replicate performs post-commit replication of updated data managed by the repository.
	Replicate(context.Context, []StoreInfo) error
}

// KeyValueStoreItemActionResponse is the per-item response including payload and error for a CRUD action.
type KeyValueStoreItemActionResponse[T any] struct {
	Payload T
	Error   error
}

// KeyValueStoreResponse aggregates per-item results and an optional summary error.
type KeyValueStoreResponse[T any] struct {
	// Details contains per-item action results.
	Details []KeyValueStoreItemActionResponse[T]
	// Error is a summary error if at least one action failed.
	Error error
}

// KeyValueStore defines CRUD operations for a generic key-value backend with optional partial success semantics.
type KeyValueStore[TK any, TV any] interface {
	// Fetch retrieves entries by keys from the remote storage subsystem.
	Fetch(context.Context, string, []TK) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// FetchLargeObject retrieves a single large entry by key.
	FetchLargeObject(context.Context, string, TK) (TV, error)
	// Add inserts entries.
	Add(context.Context, string, []KeyValuePair[TK, TV]) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// Update modifies existing entries.
	Update(context.Context, string, []KeyValuePair[TK, TV]) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// Remove deletes entries by keys.
	Remove(context.Context, string, []TK) KeyValueStoreResponse[TK]
}

// Cache abstracts an out-of-process cache (e.g., Redis) and its locking facilities.
type Cache interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	// Get returns: found(bool), value(string), err(error from backend).
	Get(ctx context.Context, key string) (bool, string, error)
	// GetEx returns found(bool), value(string), err using TTL/sliding expiration semantics.
	GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error)

	// IsRestarted reports whether the cache backend (e.g., Redis) has restarted since the last check.
	// Implementations should return true once per backend restart event per-process and false otherwise.
	IsRestarted(ctx context.Context) (bool, error)

	// SetStruct upserts a struct value under a key.
	SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	// GetStruct fetches a struct value; first return indicates success (false for not found or error).
	GetStruct(ctx context.Context, key string, target interface{}) (bool, error)
	// GetStructEx fetches a struct value with TTL/sliding expiration semantics.
	GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error)
	// Delete removes objects by keys; returns whether all keys were deleted.
	Delete(ctx context.Context, keys []string) (bool, error)
	// Ping checks connectivity to the cache backend.
	Ping(ctx context.Context) error

	// FormatLockKey creates a lock key name from an arbitrary string.
	FormatLockKey(k string) string
	// CreateLockKeys builds LockKey objects from a set of key names.
	CreateLockKeys(keys []string) []*LockKey
	// CreateLockKeysForIDs builds LockKey objects for ID tuples (e.g., Transaction ID scoped locks).
	CreateLockKeysForIDs(keys []Tuple[string, UUID]) []*LockKey

	// IsLockedTTL reports whether all keys are locked and refreshes TTL with the provided duration.
	IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, error)

	// Lock attempts to lock all keys; returns success, lock owner UUID, and any error encountered.
	Lock(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, UUID, error)
	// IsLocked reports whether all keys are currently locked.
	IsLocked(ctx context.Context, lockKeys []*LockKey) (bool, error)
	// IsLockedByOthers reports whether the keys are locked by other processes.
	IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error)
	// Unlock releases a set of keys.
	Unlock(ctx context.Context, lockKeys []*LockKey) error

	// Clear purges the entire cache database.
	Clear(ctx context.Context) error
}

// CtxPriorityLogIgnoreAge is a context key used by priority log implementations to
// opt into processing all .plg files regardless of age (used for restart-triggered sweeps).
// When set to true in context, GetBatch should ignore the age filter.
type contextKey string

// ContextPriorityLogIgnoreAge signals priority log GetBatch to ignore age filter when true.
const ContextPriorityLogIgnoreAge contextKey = "plg_ignore_age"

// CloseableCache is a Cache that also implements io.Closer for explicit lifecycle control.
type CloseableCache interface {
	Cache
	io.Closer
}

// LockKey represents a lockable cache key along with ownership metadata.
type LockKey struct {
	Key         string
	LockID      UUID
	IsLockOwner bool
}
