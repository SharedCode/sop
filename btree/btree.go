package btree

import (
	"fmt"
	log "log/slog"
)

// Btree manages items using B-tree data structure and algorithm.
type Btree[TKey Comparable, TValue any] struct {
	Store          Store
	StoreInterface *StoreInterface[TKey, TValue] `json:"-"`
	TempSlots      []*Item[TKey, TValue]         `json:"-"`
	TempChildren   []UUID                        `json:"-"`
	CurrentItemRef    CurrentItemRef             `json:"-"`
	currentItem       *Item[TKey, TValue]
	DistributeAction DistributeAction[TKey, TValue]
}

type CurrentItemRef struct {
	NodeId        UUID
	NodeItemIndex int
}

type DistributeAction[TKey Comparable, TValue any] struct {
	Source *Node[TKey, TValue]
	Item *Item[TKey, TValue]
	// DistributeToLeft is true if item needs to be distributed to the left side,
	// otherwise to the right side.
	DistributeToLeft bool
}

func NewBtree[TKey Comparable, TValue any](store Store, si *StoreInterface[TKey, TValue]) *Btree[TKey, TValue] {
	var b3 = Btree[TKey, TValue]{
		Store:          store,
		StoreInterface: si,
		TempSlots:      make([]*Item[TKey, TValue], store.NodeSlotCount+1),
		TempChildren:   make([]UUID, store.NodeSlotCount+2),
	}
	return &b3
}

// done
func (btree *Btree[TKey, TValue]) Add(key TKey, value TValue) (bool, error) {
	var itm = Item[TKey, TValue]{
		Key:   key,
		Value: &value,
	}
	localTrans := false
	if !btree.StoreInterface.Transaction.HasBegun() {
		err := btree.StoreInterface.Transaction.Begin()
		if err != nil {
			return false, err
		}
		localTrans = true
	}
	node,err := btree.rootNode()
	if err != nil {
		return false, err
	}
	result, err := node.add(btree, &itm)
	if err != nil {
		if localTrans {
			// Rollback should rarely fail, but if it does, return it.
			err2 := btree.StoreInterface.Transaction.Rollback()
			if err2 != nil {
				return false, fmt.Errorf("Transaction rollback failed, error: %v, original error: %v", err2, err)
			}
		}
		return false, err
	}
	distribute()
	// Increment store's item count.
	btree.Store.Count++
	if localTrans {
		err = btree.StoreInterface.Transaction.Commit()
		if err != nil {
			return false, err
		}
	}
	return result, nil
}

// done
func (btree *Btree[TKey, TValue]) FindOne(key TKey, firstItemWithKey bool) (bool, error) {
	// return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 {
		return false, nil
	}
	// Return current Value if key is same as current Key.
	ci := btree.GetCurrentItem()
	if !firstItemWithKey && compare[TKey](ci.Key, key) == 0 {
		return true, nil
	}
	node, err := btree.rootNode()
	if err != nil {
		return false, err
	}
	return node.find(btree, key, firstItemWithKey)
}

func (btree *Btree[TKey, TValue]) GetCurrentKey() TKey {
	return btree.GetCurrentItem().Key
}
func (btree *Btree[TKey, TValue]) GetCurrentValue() TValue {
	return *btree.GetCurrentItem().Value
}

func (btree *Btree[TKey, TValue]) GetCurrentItem() Item[TKey, TValue] {
	var zero Item[TKey, TValue]
	if btree.CurrentItemRef.NodeId.IsNil() {
		btree.currentItem = nil
		return zero
	}
	if btree.currentItem != nil {
		return *btree.currentItem
	}
	n, err := btree.StoreInterface.NodeRepository.Get(btree.CurrentItemRef.NodeId)
	if err != nil {
		// TODO: Very rarely to happen, & we need to log err when logging is in.
		return zero
	}
	btree.currentItem = n.Slots[btree.CurrentItemRef.NodeItemIndex]
	return *btree.currentItem
}

// TODO
func (btree *Btree[TKey, TValue]) AddIfNotExist(key TKey, value TValue) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) Update(key TKey, value TValue) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) UpdateCurrentItem(newValue TValue) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) Remove(key TKey) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) RemoveCurrentItem() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) MoveToFirst() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) MoveToLast() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) MoveToNext() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) MoveToPrevious() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TKey, TValue]) IsValueDataInNodeSegment() bool {
	return btree.Store.IsValueDataInNodeSegment
}

// TODO
// SaveNode will prepare & persist (if needed) the Node to the backend
// via NodeRepository call.
func (btree *Btree[TKey, TValue]) SaveNode(node *Node[TKey, TValue]) error {
	return nil
}


func (btree *Btree[TKey, TValue]) rootNode() (*Node[TKey, TValue], error) {
	// TODO: register root node to nodeRepository or the Transaction.
	if btree.Store.RootNodeLogicalId.IsNil() {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeLogicalId = NewUUID()
		var root = NewNode[TKey, TValue](btree.Store.NodeSlotCount)
		// Set both logical Id & physical Id to the same UUID to begin with.
		// Transaction commit should handle resolving them.
		root.logicalId = btree.Store.RootNodeLogicalId
		root.Id = btree.Store.RootNodeLogicalId
		return root, nil
	}
	h, err := btree.StoreInterface.VirtualIdRepository.Get(btree.Store.RootNodeLogicalId)
	if err != nil {
		return nil, err
	}
	root, err := btree.getNode(h.GetActiveId())
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("Can't retrieve Root Node w/ logical Id '%s'", btree.Store.RootNodeLogicalId.ToString())
	}
	return root, nil
}

func (btree *Btree[TKey, TValue]) getNode(id UUID) (*Node[TKey, TValue], error) {
	n, e := btree.StoreInterface.NodeRepository.Get(id)
	if e != nil {
		return nil, e
	}
	return n, nil
}

func (btree *Btree[TKey, TValue]) setCurrentItemId(nodeId UUID, itemIndex int) {
	if btree.CurrentItemRef.NodeId == nodeId && btree.CurrentItemRef.NodeItemIndex == itemIndex {
		return
	}
	btree.currentItem = nil
	btree.CurrentItemRef.NodeId = nodeId
	btree.CurrentItemRef.NodeItemIndex = itemIndex
}

func (btree *Btree[TKey, TValue]) isUnique() bool {
	return btree.Store.IsUnique
}

func (btree *Btree[TKey, TValue])distribute() {
	if btree.DistributeAction.Source != nil {
		log.Debug("Distribute item with key(%v) of node Id(%v) to left(%v).",
			btree.DistributeAction.Item.Key, btree.DistributeAction.Source.Id, btree.DistributeAction.DistributeToLeft)
	}
	for btree.DistributeAction.Source != nil {
		n := btree.DistributeAction.Source
		btree.DistributeAction.Source = nil
		item := btree.DistributeAction.Item
		btree.DistributeAction.Item = nil

		if btree.DistributeAction.DistributeToLeft {
			n.DistributeToLeft(btree, item)
		} else {
			n.DistributeToRight(btree, item)
		}
	}
}
