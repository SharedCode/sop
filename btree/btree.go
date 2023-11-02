package btree

import (
	"fmt"
	log "log/slog"
)

// Btree manages items using B-tree data structure and algorithm.
type Btree[TK Comparable, TV any] struct {
	Store            Store
	StoreInterface   *StoreInterface[TK, TV] `json:"-"`
	tempSlots        []*Item[TK, TV]
	tempChildren     []UUID
	currentItemRef   currentItemRef
	currentItem      *Item[TK, TV]
	distributeAction distributeAction[TK, TV]
}

type currentItemRef struct {
	NodeId        UUID
	NodeItemIndex int
}

// distributeAction contains details to allow B-Tree to balance item load across nodes.
// "distribute" function will use these details in order to distribute an item of a node
// to either the left side or right side nodes of the branch(relative to the Source)
// that is known to have a vacant slot.
type distributeAction[TK Comparable, TV any] struct {
	Source *Node[TK, TV]
	Item   *Item[TK, TV]
	// DistributeToLeft is true if item needs to be distributed to the left side,
	// otherwise to the right side.
	DistributeToLeft bool
}

func NewBtree[TK Comparable, TV any](store Store, si *StoreInterface[TK, TV]) *Btree[TK, TV] {
	var b3 = Btree[TK, TV]{
		Store:          store,
		StoreInterface: si,
		tempSlots:      make([]*Item[TK, TV], store.NodeSlotCount+1),
		tempChildren:   make([]UUID, store.NodeSlotCount+2),
	}
	return &b3
}

// done
func (btree *Btree[TK, TV]) Add(key TK, value TV) (bool, error) {
	var itm = Item[TK, TV]{
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
	node, err := btree.rootNode()
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
	// Registers the root node to the transaction manager so it can get saved if needed.
	btree.distribute()
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
func (btree *Btree[TK, TV]) FindOne(key TK, firstItemWithKey bool) (bool, error) {
	// return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 {
		return false, nil
	}
	// Return current Value if key is same as current Key.
	ci := btree.GetCurrentItem()
	if !firstItemWithKey && compare[TK](ci.Key, key) == 0 {
		return true, nil
	}
	node, err := btree.rootNode()
	if err != nil {
		return false, err
	}
	return node.find(btree, key, firstItemWithKey)
}

// GetCurrentKey returns the current item's key part.
func (btree *Btree[TK, TV]) GetCurrentKey() TK {
	return btree.GetCurrentItem().Key
}

// GetCurrentValue returns the current item's value part.
func (btree *Btree[TK, TV]) GetCurrentValue() TV {
	return *btree.GetCurrentItem().Value
}

// GetCurrentItem returns the current item containing key/value pair.
func (btree *Btree[TK, TV]) GetCurrentItem() Item[TK, TV] {
	var zero Item[TK, TV]
	if btree.currentItemRef.NodeId.IsNil() {
		btree.currentItem = nil
		return zero
	}
	if btree.currentItem != nil {
		return *btree.currentItem
	}
	n, err := btree.StoreInterface.NodeRepository.Get(btree.currentItemRef.NodeId)
	if err != nil {
		// TODO: Very rarely to happen, & we need to log err when logging is in.
		return zero
	}
	btree.currentItem = n.Slots[btree.currentItemRef.NodeItemIndex]
	return *btree.currentItem
}

// TODO
func (btree *Btree[TK, TV]) AddIfNotExist(key TK, value TV) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) Update(key TK, value TV) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) UpdateCurrentItem(newValue TV) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) Remove(key TK) (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) RemoveCurrentItem() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) MoveToFirst() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) MoveToLast() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) MoveToNext() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) MoveToPrevious() (bool, error) {
	return false, nil
}

// TODO
func (btree *Btree[TK, TV]) IsValueDataInNodeSegment() bool {
	return btree.Store.IsValueDataInNodeSegment
}

// TODO
// SaveNode will prepare & persist (if needed) the Node to the backend
// via NodeRepository call.
func (btree *Btree[TK, TV]) saveNode(node *Node[TK, TV]) error {
	return nil
}

func (btree *Btree[TK, TV]) rootNode() (*Node[TK, TV], error) {
	// TODO: register root node to nodeRepository or the Transaction.
	if btree.Store.RootNodeLogicalId.IsNil() {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeLogicalId = NewUUID()
		var root = newNode[TK, TV](btree.Store.NodeSlotCount)
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

func (btree *Btree[TK, TV]) getNode(id UUID) (*Node[TK, TV], error) {
	n, e := btree.StoreInterface.NodeRepository.Get(id)
	if e != nil {
		return nil, e
	}
	return n, nil
}

func (btree *Btree[TK, TV]) setCurrentItemId(nodeId UUID, itemIndex int) {
	if btree.currentItemRef.NodeId == nodeId && btree.currentItemRef.NodeItemIndex == itemIndex {
		return
	}
	btree.currentItem = nil
	btree.currentItemRef.NodeId = nodeId
	btree.currentItemRef.NodeItemIndex = itemIndex
}

func (btree *Btree[TK, TV]) isUnique() bool {
	return btree.Store.IsUnique
}

// distribute function allows B-Tree to avoid using recursion. I.e. - instead of the node calling
// a recursive function that distributes or moves an item from a source node to a vacant slot somewhere
// in the sibling nodes, distribute allows a controller(distribute)-pawn(node.DistributeLeft or XxRight)
// pattern and avoids recursion.
func (btree *Btree[TK, TV]) distribute() {
	if btree.distributeAction.Source != nil {
		log.Debug("Distribute item with key(%v) of node Id(%v) to left(%v).",
			btree.distributeAction.Item.Key, btree.distributeAction.Source.Id, btree.distributeAction.DistributeToLeft)
	}
	for btree.distributeAction.Source != nil {
		n := btree.distributeAction.Source
		btree.distributeAction.Source = nil
		item := btree.distributeAction.Item
		btree.distributeAction.Item = nil

		// Call the node DistributeLeft or XxRight to do the 2nd part of the "item distribution" logic.
		if btree.distributeAction.DistributeToLeft {
			n.distributeToLeft(btree, item)
		} else {
			n.distributeToRight(btree, item)
		}
	}
}
