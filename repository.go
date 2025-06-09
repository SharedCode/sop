package sop

import (
	"context"
	"io"
	"time"
)

// Manage or fetch Virtual ID request/response payload.
type RegistryPayload[T Handle | UUID] struct {
	// Registry table (name) where the Virtual IDs will be stored or fetched from.
	RegistryTable string

	// During Rollback and Commit, we need to get hold of the paired BlobTable(or blob base folder path if in FS).
	BlobTable string
	// CacheDuration to be used for Redis caching.
	CacheDuration time.Duration
	// true will use Redis' sliding time, a.k.a. TTL support.
	IsCacheTTL bool

	// IDs is an array containing the Virtual IDs details to be managed (CRUD).
	IDs []T
}

// Virtual ID registry is essential in our support for all or nothing (sub)feature,
// which is essential for fault tolerance.
//
// All methods are taking in a set of items.
type Registry interface {
	// Get will fetch handles(given their IDs) from registry table(s).
	Get(context.Context, []RegistryPayload[UUID]) ([]RegistryPayload[Handle], error)
	// Add will insert handles to registry table(s).
	Add(context.Context, []RegistryPayload[Handle]) error
	// Update will update handles potentially spanning across registry table(s). Will issue a cache lock call
	// for each handle to be updated.
	Update(ctx context.Context, handles []RegistryPayload[Handle]) error
	// Update for use in an active transaction where the registry handles for update were
	// all pre-locked (& post call unlocked) by the transaction manager.
	UpdateNoLocks(ctx context.Context, storesHandles []RegistryPayload[Handle]) error
	// Remove will delete handles(given their IDs) from registry table(s).
	Remove(context.Context, []RegistryPayload[UUID]) error

	// Implement to write to do the replication of data to passive target paths.
	// This will be invoked after the transaction got committed to allow the registry to
	// copy the files or portion of the files that were updated during the transaction.
	Replicate(ctx context.Context, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []RegistryPayload[Handle])
}

// ManageStore specifies the methods used to manage the Store(s) container.
type ManageStore interface {
	// Create the store(s) container, e.g. - folder if in file system.
	CreateStore(context.Context, string) error
	// Remove the store(s) container, e.g. - folder if in file system.
	RemoveStore(context.Context, string) error
}

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, a Cassandra partition, etc...
type BlobStore interface {
	// Get or fetch a blob given an ID.
	GetOne(ctx context.Context, blobTable string, blobID UUID) ([]byte, error)
	// Add blobs to store.
	Add(ctx context.Context, blobs []BlobsPayload[KeyValuePair[UUID, []byte]]) error
	// Update blobs in store.
	Update(ctx context.Context, blobs []BlobsPayload[KeyValuePair[UUID, []byte]]) error
	// Remove blobs in store with given IDs.
	Remove(ctx context.Context, blobsIDs []BlobsPayload[UUID]) error
}

// Manage or fetch node blobs request/response payload.
type BlobsPayload[T UUID | KeyValuePair[UUID, []byte]] struct {
	// Blob store table name.
	BlobTable string
	// Blobs contains the blobs IDs and blobs data for upsert to the store or the blobs IDs to be removed.
	Blobs []T
}

// Transaction Log specifies the API(methods) needed to implement logging for the transaction.
type TransactionLog interface {
	// Add a transaction log.
	Add(ctx context.Context, tid UUID, commitFunction int, payload []byte) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid UUID) error

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago, mark it so succeeding call
	// will return the next hour and so on, until no more, upon reaching the current hour.
	//
	// GetOne behaves like a job distributor by the hour. SOP uses it to sprinkle/distribute task to cleanup
	// left over resources by unfinished transactions in time. Be it due to crash or host reboot, any transaction
	// temp resource will then age and reach expiration limit, then get cleaned up. This method is used to do distribution.
	//
	// It is capped to an hour ago older because anything newer may still be an in-flight or ongoing transaction.
	GetOne(ctx context.Context) (UUID, string, []KeyValuePair[int, []byte], error)

	// Given a date hour, returns an available for cleanup set of transaction logs with their Transaction ID.
	// Or nils if there is no more needing cleanup for this date hour.
	GetLogsDetails(ctx context.Context, hour string) (UUID, []KeyValuePair[int, []byte], error)

	// Implement to generate a new UUID. Cassandra transaction logging uses gocql.UUIDFromTime, SOP in file system
	// should just use the general sop.NewUUID function which currently uses google's uuid package.
	NewUUID() UUID

	// Log commit changes to its own log file separate than the rest of transaction logs.
	// This is a special log file only used during "reinstate" of drives back for replication.
	LogCommitChanges(ctx context.Context, stores []StoreInfo, newRootNodesHandles, addedNodesHandles,
		updatedNodesHandles, removedNodesHandles []RegistryPayload[Handle])
}

// StoreRepository specifies CRUD methods for StoreInfo (storage &) management.
type StoreRepository interface {
	// Fetch store info with name(s).
	Get(context.Context, ...string) ([]StoreInfo, error)
	// Fetch store info with name(s) & option to specify (caching) sliding time(TTL) duration.
	GetWithTTL(context.Context, bool, time.Duration, ...string) ([]StoreInfo, error)
	// GetAll returns list of store names available in the backend.
	GetAll(context.Context) ([]string, error)
	// Add store info & create related tables like for registry & for node blob.
	Add(context.Context, ...StoreInfo) error
	// Remove store info with name & drop related tables like for registry & for node blob.
	Remove(context.Context, ...string) error

	// Update store info. Update should also merge the Count of items between the incoming store info
	// and the target store info on the backend, as they may differ. It should use StoreInfo.CountDelta to reconcile the two.
	Update(context.Context, []StoreInfo) ([]StoreInfo, error)
	// Implement to write to do the replication of data to passive target paths.
	// This will be invoked after the transaction got committed to allow the StoreRepository to
	// copy the files or portion of the files that were updated during the transaction.
	Replicate(context.Context, []StoreInfo)
}

// KeyValue Store Item Action Response has the payload and the error, if in case an error occurred while doing CRUD operation.
type KeyValueStoreItemActionResponse[T any] struct {
	Payload T
	Error   error
}

// KeyValue Store Overall Response has a summary error(if there is) and the details about each item action failure if there is.
type KeyValueStoreResponse[T any] struct {
	// Each Item action(or operation) result.
	Details []KeyValueStoreItemActionResponse[T]
	// Overall error if at least one item action (or operation) failed.
	Error error
}

// KeyValueStore is a general purpose Store interface specifying methods or CRUD operations on Key & Value pair.
// Implementations don't need to be too fancy, it can be as simple as supporting partial success.
type KeyValueStore[TK any, TV any] interface {
	// Fetch entry(ies) with given key(s).
	// Fetch term is used here because this CRUD interface is NOT part of the B-Tree system, thus, the context is
	// to "fetch" from the remote data storage sub-system like AWS S3.
	Fetch(context.Context, string, []TK) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// Fetch a large entry with the given key.
	FetchLargeObject(context.Context, string, TK) (TV, error)
	// Add entry(ies) to the store.
	Add(context.Context, string, []KeyValuePair[TK, TV]) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// Update entry(ies) of the store.
	Update(context.Context, string, []KeyValuePair[TK, TV]) KeyValueStoreResponse[KeyValuePair[TK, TV]]
	// Remove entry(ies) from the store given their names.
	Remove(context.Context, string, []TK) KeyValueStoreResponse[TK]
}

// Cache interface specifies the methods implemented for out of memory caching, e.g. - Redis based.
// String key and interface{} value are the supported types. Also specifies methods useful for locking.
type Cache interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	// First return bool var signifies success or false if either item was not found or an error occurred during Get.
	Get(ctx context.Context, key string) (bool, string, error)
	// First return bool var signifies success or false if either item was not found or an error occurred during Get.
	GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error)

	// SetStruct upserts a given object with a key to it.
	SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	// GetStruct fetches a given object given a key. First return bool var signifies success or false if
	// either item was not found or an error occurred during Get.
	GetStruct(ctx context.Context, key string, target interface{}) (bool, error)
	// GetStruct fetches a given object given a key in a TTL manner, that is, sliding time.
	// First return bool var signifies success or false if either item was not found or an error occurred during Get.
	GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error)
	// Delete removes the object given a key.
	Delete(ctx context.Context, keys []string) (bool, error)
	// Ping is a utility function to check if connection is good.
	Ping(ctx context.Context) error

	// Formats a given string as a lock key.
	FormatLockKey(k string) string
	// Create lock keys.
	CreateLockKeys(keys []string) []*LockKey

	// Returns whether a set of keys are all locked & with TTL.
	IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, error)

	// Lock a set of keys.
	Lock(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, error)
	// Returns whether a set of keys are all locked.
	IsLocked(ctx context.Context, lockKeys []*LockKey) (bool, error)
	// Returns true if a set of keys are all locked, most likely by other processes (or threads).
	// Use-case is for checking if a certain set of keys are locked by other processes.
	IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error)
	// Unlock a given set of keys.
	Unlock(ctx context.Context, lockKeys []*LockKey) error

	// Clear out the backend Cache database of all items.
	Clear(ctx context.Context) error
}

// Closeable Cache is a cache that which, you can explicitly call its "Close" method
// after you are done with it.
type CloseableCache interface {
	Cache
	io.Closer
}

// LockKey contain fields to allow locking and unlocking of a set of cache keys.
type LockKey struct {
	Key         string
	LockID      UUID
	IsLockOwner bool
}
