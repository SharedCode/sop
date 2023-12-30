package kafka

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

type ItemType int

const (
	Unknown = iota
	BtreeNode
	ItemValue
)

type DeletedItem struct {
	ItemType ItemType
	ItemId   btree.UUID
}

type deletedItemsQueue struct {
	deletedItems []DeletedItem
}

// TODO: NewDeletedItemsQueue manages the deleted Items in Kafka or something similar/for simple enqueue/dequeue.
// Below is just a mock so we can move forward prototyping the system. Finalize the API as well, e.g. perhaps
// we can use one (generics) Queue implemented to talk to Kafka and can take in any struct type.
func NewDeletedItemsQueue() Queue[DeletedItem] {
	return &deletedItemsQueue{
		deletedItems: make([]DeletedItem, 0, 25),
	}
}

func (d *deletedItemsQueue) Enqueue(ctx context.Context, delItem ...DeletedItem) error {
	d.deletedItems = append(d.deletedItems, delItem...)
	return nil
}

func (d *deletedItemsQueue) Peek(ctx context.Context, count int) ([]DeletedItem, error) {
	batch := make([]DeletedItem, count)
	copy(batch, d.deletedItems)
	return batch, nil
}

func (d *deletedItemsQueue) Dequeue(ctx context.Context, count int) ([]DeletedItem, error) {
	batch := make([]DeletedItem, count)
	copy(batch, d.deletedItems)
	if count >= len(d.deletedItems) {
		d.deletedItems = make([]DeletedItem, 0, 20)
		return batch, nil
	}
	newSlice := make([]DeletedItem, len(d.deletedItems)-count)
	copy(newSlice, d.deletedItems[count:])
	d.deletedItems = newSlice
	return batch, nil
}
