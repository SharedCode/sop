package fs

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/SharedCode/sop"
)

type registryMap struct {
	hashmap *hashmap
}

func newRegistryMap(readWrite bool, hashModValue int, replicationTracker *replicationTracker, cache sop.Cache) *registryMap {
	return &registryMap{
		hashmap: newHashmap(readWrite, hashModValue, replicationTracker, cache),
	}
}

// Add a given set of Handle(s) record(s) on file(s) where they are supposed to get stored in.
func (rm registryMap) add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	// Individually write to the file area occupied by the handle so we don't create "lock pressure".
	for _, item := range storesHandles {
		for _, h := range item.IDs {
			frd, err := rm.hashmap.findFileRegion(ctx, item.RegistryTable, []sop.UUID{h.LogicalID})
			if err != nil {
				return err
			}

			// Fail if item exists in target.
			if !frd[0].handle.IsEmpty() {
				return fmt.Errorf("registryMap.add failed, can't overwrite an item at offset=%v, item details: %v", frd[0].getOffset(), frd[0].handle)
			}

			frd[0].handle = h

			log.Debug(fmt.Sprintf("adding to file %s, sector offset %v, offset in block %v", frd[0].dio.filename, frd[0].blockOffset, frd[0].handleInBlockOffset))

			if err := rm.hashmap.updateFileRegion(ctx, frd); err != nil {
				return err
			}
		}
	}
	return nil
}

// Update a given set of Handle(s) record(s) on file(s) where they are stored in.
func (rm registryMap) set(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	for _, item := range storesHandles {
		frds, err := rm.hashmap.findFileRegion(ctx, item.RegistryTable, getIDs(item.IDs))
		if err != nil {
			return err
		}
		// Update the Handles read w/ the items' values.
		for i := range frds {
			// Check if the record in the target file region is different.
			if !frds[i].handle.IsEmpty() && frds[i].handle.LogicalID != item.IDs[i].LogicalID {
				// Fail if the record on target is different.
				return fmt.Errorf("registryMap.set failed, an item(target lid=%v) at offset=%v is different (source lid=%v)",
					frds[i].handle.LogicalID, frds[i].getOffset(), item.IDs[i].LogicalID)
			}
			// Update the handle with incoming.
			frds[i].handle = item.IDs[i]
		}

		// Do actual file region update.
		log.Debug(fmt.Sprintf("updating file %s, sector offset %v, offset in block %v", frds[0].dio.filename, frds[0].blockOffset, frds[0].handleInBlockOffset))

		if err := rm.hashmap.updateFileRegion(ctx, frds); err != nil {
			return err
		}
	}
	return nil
}

// Fetch the Handle record(s) from a given set of file(s) & their UUID(s).
func (rm registryMap) fetch(ctx context.Context, keys []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	result := make([]sop.RegistryPayload[sop.Handle], 0, len(keys))
	for _, k := range keys {
		handles, err := rm.hashmap.fetch(ctx, k.RegistryTable, k.IDs)
		if err != nil {
			return nil, fmt.Errorf("registryMap.fetch failed, details: %w", err)
		}
		result = append(result, sop.RegistryPayload[sop.Handle]{
			RegistryTable: k.RegistryTable,
			IDs:           handles,
		})
	}
	return result, nil
}

// Mark the Handle record(s) on file to be deleted & reuse ready.
func (rm registryMap) remove(ctx context.Context, keys []sop.RegistryPayload[sop.UUID]) error {
	// Individually delete the file area occupied by the handle so we don't create "lock pressure".
	for _, key := range keys {

		frds, err := rm.hashmap.findFileRegion(ctx, key.RegistryTable, key.IDs)
		if err != nil {
			return err
		}
		for i := range frds {
			// If read handle is empty, it means the item is already marked deleted in disk.
			if frds[i].handle.IsEmpty() {
				// Fail if there is no record on target, can't delete a missing item.
				return fmt.Errorf("registryMap.remove failed, an item at offset=%v was not found, can't delete a missing item", frds[i].getOffset())
			}
			// Check if the record in the target file region is different.
			if frds[i].handle.LogicalID != key.IDs[i] {
				// Fail if the found record on target is different.
				return fmt.Errorf("registryMap.remove failed, an item(target lid=%v) at offset=%v is different (source lid=%v)",
					frds[i].handle.LogicalID, frds[i].getOffset(), key.IDs[i])
			}
		}

		if err := rm.hashmap.markDeleteFileRegion(ctx, frds); err != nil {
			return err
		}
	}
	return nil
}

// Close all files opened by this hashmap on disk.
func (rm registryMap) close() error {
	return rm.hashmap.close()
}

func getIDs(items []sop.Handle) []sop.UUID {
	IDs := make([]sop.UUID, len(items))
	for i := range items {
		IDs[i] = items[i].LogicalID
	}
	return IDs
}
