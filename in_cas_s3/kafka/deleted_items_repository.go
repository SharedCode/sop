package kafka

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

// TODO: finalize the Deleted Items queue interface.

type DeletedItemsQueue interface {
	Dequeue(ctx context.Context) (DeletedItem, error)
	Enqueue(ctx context.Context, delItem DeletedItem) error
}

type ItemType int
const(
	Unknown = iota
	BtreeNode
	ItemValue
)

type DeletedItem struct {
	ItemType ItemType
	ItemId btree.UUID
}

type deletedItemsRepository struct{
	lookup map[btree.UUID]DeletedItem
}

// TODO: NewVirtualIdRegistry manages the Handle in Cassandra table.
func NewDeletedItemsRepository() DeletedItemsRepository {
	return &deletedItemsRepository{
		lookup: make(map[btree.UUID]DeletedItem),
	}
}

func (d *deletedItemsRepository) Add(ctx context.Context, delItem DeletedItem) error {
	d.lookup[delItem.ItemId] = delItem
	return nil
}

func (d *deletedItemsRepository) Update(ctx context.Context, delItem DeletedItem) error {
	d.lookup[delItem.ItemId] = delItem
	return nil
}
func (d *deletedItemsRepository) Get(ctx context.Context, itemId btree.UUID) (DeletedItem, error) {
	di,_ := d.lookup[itemId]
	return di, nil
}
func (d *deletedItemsRepository) Remove(ctx context.Context, itemId btree.UUID) error {
	delete(d.lookup, itemId)
	return nil
}
