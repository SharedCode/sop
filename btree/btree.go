package btree

import (
	"fmt"
	log "log/slog"
)

// Btree manages items using B-tree data structure and algorithm.
type Btree[TK Comparable, TV any] struct {
	Store              Store
	storeInterface     *StoreInterface[TK, TV]
	tempSlots          []*Item[TK, TV]
	tempParent         *Item[TK, TV]
	tempChildren       []UUID
	tempParentChildren []UUID
	currentItemRef     currentItemRef
	currentItem        *Item[TK, TV]
	distributeAction   distributeAction[TK, TV]
	promoteAction      promoteAction[TK, TV]
}

// currentItemRef contains node Id & item slot index position in the node.
// SOP B-Tree has a "cursor" like feature to allow navigation & fetch of the items
// for most complicated querying scenario possible, or as needed by the business.
type currentItemRef struct {
	nodeId        UUID
	nodeItemIndex int
}

func (c currentItemRef) getNodeItemIndex() int {
	return c.nodeItemIndex
}
func (c currentItemRef) getNodeId() UUID {
	return c.nodeId
}

// distributeAction contains details to allow B-Tree to balance item load across nodes.
// "distribute" function will use these details in order to distribute an item of a node
// to either the left side or right side nodes of the branch(relative to the sourceNode)
// that is known to have a vacant slot.
type distributeAction[TK Comparable, TV any] struct {
	sourceNode *Node[TK, TV]
	item       *Item[TK, TV]
	// distributeToLeft is true if item needs to be distributed to the left side,
	// otherwise to the right side.
	distributeToLeft bool
}

// promoteAction similar to distributeAction, contains details to allow controller in B-Tree
// to drive calls for Node promotion to a higher level branch without using recursion.
// Recursion can be more "taxing"(on edge case) as it accumulates items pushed to the stack.
type promoteAction[TK Comparable, TV any] struct {
	targetNode *Node[TK, TV]
	slotIndex  int
}

// NewBtree creates a new B-Tree instance.
func NewBtree[TK Comparable, TV any](store Store, si *StoreInterface[TK, TV]) *Btree[TK, TV] {
	var b3 = Btree[TK, TV]{
		Store:              store,
		storeInterface:     si,
		tempSlots:          make([]*Item[TK, TV], store.SlotLength+1),
		tempChildren:       make([]UUID, store.SlotLength+2),
		tempParentChildren: make([]UUID, 2),
	}
	return &b3
}

// Add a key/value pair item to the tree.
func (btree *Btree[TK, TV]) Add(key TK, value TV) (bool, error) {
	var itm = Item[TK, TV]{
		Key:   key,
		Value: &value,
	}
	node, err := btree.getRootNode()
	if err != nil {
		return false, err
	}
	result, err := node.add(btree, &itm)
	if err != nil {
		return false, err
	}
	// Add failed with no reason, 'just return false.
	if !result {
		return false, nil
	}

	// Service the node's requested action(s).
	btree.distribute()
	btree.promote()

	// Increment store's item count.
	// TODO: Register Store change to transaction manager (on V2) so it can get persisted.
	btree.Store.Count++

	// Registers the root node to the transaction manager so it can get saved if needed.
	if err = btree.saveNode(node); err != nil {
		return false, err
	}
	return true, nil
}

// FindOne will traverse the tree to find an item with such key.
func (btree *Btree[TK, TV]) FindOne(key TK, firstItemWithKey bool) (bool, error) {
	// return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 {
		return false, nil
	}
	// Return current Value if key is same as current Key.
	if btree.isCurrentItemSelected() {
		ci := btree.GetCurrentItem()
		if !firstItemWithKey && compare[TK](ci.Key, key) == 0 {
			return true, nil
		}
	}
	node, err := btree.getRootNode()
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
func (btree *Btree[TK, TV]) GetCurrentValue() (TV, error) {
	// TODO: in V2, we need to fetch Value if btree is set to save Value in another "data segment"
	// and it is not yet fetched. That fetch action can error thus, need to be able to return an error.
	return *btree.GetCurrentItem().Value, nil
}

// GetCurrentItem returns the current item containing key/value pair.
func (btree *Btree[TK, TV]) GetCurrentItem() Item[TK, TV] {
	var zero Item[TK, TV]
	if btree.currentItemRef.nodeId.IsNil() {
		btree.currentItem = nil
		return zero
	}
	if btree.currentItem != nil {
		return *btree.currentItem
	}
	n, err := btree.storeInterface.NodeRepository.Get(btree.currentItemRef.getNodeId())
	if err != nil {
		// TODO: Very rarely to happen, & we need to log err when logging is in.
		return zero
	}
	btree.currentItem = n.Slots[btree.currentItemRef.getNodeItemIndex()]
	return *btree.currentItem
}

// AddIfNotExist will add an item if its key is not yet in the B-Tree.
func (btree *Btree[TK, TV]) AddIfNotExist(key TK, value TV) (bool, error) {
	u := btree.Store.IsUnique
	btree.Store.IsUnique = true
	ok, err := btree.Add(key, value)
	btree.Store.IsUnique = u
	return ok, err
}

// MoveToFirst will traverse the tree and find the first item, first according to
// the key ordering sequence.
func (btree *Btree[TK, TV]) MoveToFirst() (bool, error) {
	// Return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 {
		return false, nil
	}
	node, err := btree.getRootNode()
	if err != nil {
		return false, err
	}
	return node.moveToFirst(btree)
}

func (btree *Btree[TK, TV]) MoveToLast() (bool, error) {
	// Return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 {
		return false, nil
	}
	node, err := btree.getRootNode()
	if err != nil {
		return false, err
	}
	return node.moveToLast(btree)
}

func (btree *Btree[TK, TV]) MoveToNext() (bool, error) {
	// Return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 || !btree.isCurrentItemSelected() {
		return false, nil
	}
	node, err := btree.getNode(btree.currentItemRef.getNodeId())
	if err != nil {
		return false, err
	}
	if node == nil || node.Slots[btree.currentItemRef.getNodeItemIndex()] == nil {
		return false, nil
	}
	return node.moveToNext(btree)
}

func (btree *Btree[TK, TV]) MoveToPrevious() (bool, error) {
	// Return default value & no error if B-Tree is empty.
	if btree.Store.Count == 0 || !btree.isCurrentItemSelected() {
		return false, nil
	}
	node, err := btree.getNode(btree.currentItemRef.getNodeId())
	if err != nil {
		return false, err
	}
	if node == nil || node.Slots[btree.currentItemRef.getNodeItemIndex()] == nil {
		return false, nil
	}
	return node.moveToPrevious(btree)
}

// Update will find the item with matching key as the key parameter & update its value
// with the provided value parameter.
func (btree *Btree[TK, TV]) Update(key TK, value TV) (bool, error) {
	ok, err := btree.FindOne(key, false)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return btree.UpdateCurrentItem(value)
}

func (btree *Btree[TK, TV]) UpdateCurrentItem(newValue TV) (bool, error) {
	if btree.currentItemRef.getNodeId() == NilUUID {
		return false, nil
	}
	node, err := btree.getNode(btree.currentItemRef.getNodeId())
	if err != nil {
		return false, err
	}
	if node == nil || node.Slots[btree.currentItemRef.getNodeItemIndex()] == nil {
		return false, nil
	}
	node.Slots[btree.currentItemRef.getNodeItemIndex()].Value = &newValue
	// Let the NodeRepository (& TransactionManager take care of backend storage upsert, etc...)
	if err = btree.saveNode(node); err != nil {
		return false, err
	}
	return true, nil
}

// Remove will find the item with given key and delete it.
func (btree *Btree[TK, TV]) Remove(key TK) (bool, error) {
	ok, err := btree.FindOne(key, false)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return btree.RemoveCurrentItem()
}

// RemoveCurrentItem will remove the current item, i.e. - referenced by CurrentItemRef.
func (btree *Btree[TK, TV]) RemoveCurrentItem() (bool, error) {
	if btree.currentItemRef.getNodeId() == NilUUID {
		return false, nil
	}
	node, err := btree.getNode(btree.currentItemRef.getNodeId())
	if err != nil {
		return false, err
	}
	if node == nil || node.Slots[btree.currentItemRef.getNodeItemIndex()] == nil {
		return false, nil
	}
	// Check if there are children nodes.
	if node.hasChildren() {
		index := btree.currentItemRef.getNodeItemIndex()
		if ok, err := node.removeItemOnNodeWithNilChild(btree, index); ok || err != nil {
			if ok {
				// Make the current item pointer point to null since we just deleted the current item.
				btree.setCurrentItemId(NilUUID, 0)
				btree.Store.Count--
			}
			return ok, err
		}
		// Below code allows for deletion to happen in the leaf(a.k.a. outermost) node's slots.
		// MoveNext method will position the Current Item ref to point to a leaf node.
		if ok, err := node.moveToNext(btree); !ok || err != nil {
			return false, err
		}
		currentNode, err := btree.getCurrentNode()
		if err != nil {
			return false, nil
		}
		// Replace the requested item for delete with the next item found on leaf node,
		// so we can delete that instead & make it happen on the leaf.
		// Deletion on leaf nodes is easier to repair/fix respective leaf branch.
		node.Slots[index] = currentNode.Slots[btree.currentItemRef.getNodeItemIndex()]
		if err = btree.saveNode(node); err != nil {
			return false, err
		}
		if ok, err := currentNode.removeItemOnNodeWithNilChild(btree, btree.currentItemRef.getNodeItemIndex()); ok || err != nil {
			if ok {
				// Make the current item pointer point to null since we just deleted the current item.
				btree.setCurrentItemId(NilUUID, 0)
				btree.Store.Count--
			}
			return ok, err
		}
		node = currentNode
	}
	err = node.fixVacatedSlot(btree)
	if err != nil {
		return false, err
	}
	// Make the current item pointer point to null since we just deleted the current item.
	btree.setCurrentItemId(NilUUID, 0)
	// TODO: Register Store change to transaction manager (on V2) so it can get persisted.
	// Not needed in in-memory (V1) version.
	btree.Store.Count--

	return true, nil
}

// IsValueDataInNodeSegment is true if Item's Values are stored in the Node segment together
// with the Items' Keys.
// Always true in in-memory B-Tree.
func (btree *Btree[TK, TV]) IsValueDataInNodeSegment() bool {
	return btree.Store.IsValueDataInNodeSegment
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
func (btree *Btree[TK, TV]) IsUnique() bool {
	return btree.Store.IsUnique
}

// saveNode will prepare & persist (if needed) the Node to the backend
// via NodeRepository call. When Transaction Manager is implemented, this
// will just register the modified/new node in the transaction session
// so it can get persisted on tranaction commit.
func (btree *Btree[TK, TV]) saveNode(node *Node[TK, TV]) error {
	if node.Id.IsNil() {
		node.Id = NewUUID()
	}
	return btree.storeInterface.NodeRepository.Upsert(node)
}

// removeNode will remove the node from backend repository.
func (btree *Btree[TK, TV]) removeNode(node *Node[TK, TV]) error {
	if node.Id.IsNil() {
		return nil
	}
	return btree.storeInterface.NodeRepository.Remove(node.Id)
}

func (btree *Btree[TK, TV]) getCurrentNode() (*Node[TK, TV], error) {
	n, err := btree.getNode(btree.currentItemRef.nodeId)
	if n == nil {
		return nil, err
	}
	return n, nil
}

func (btree *Btree[TK, TV]) getRootNode() (*Node[TK, TV], error) {
	if btree.Store.RootNodeId.IsNil() {
		// Create new Root Node if nil, implied new btree.
		var root = newNode[TK, TV](btree.getSlotLength())
		root.newId(NilUUID)
		btree.Store.RootNodeId = root.Id
		return root, nil
	}
	root, err := btree.getNode(btree.Store.RootNodeId)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("Can't retrieve Root Node w/ logical Id '%v'", btree.Store.RootNodeId)
	}
	return root, nil
}

func (btree *Btree[TK, TV]) getNode(id UUID) (*Node[TK, TV], error) {
	n, e := btree.storeInterface.NodeRepository.Get(id)
	if e != nil {
		return nil, e
	}
	return n, nil
}

func (btree *Btree[TK, TV]) setCurrentItemId(nodeId UUID, itemIndex int) {
	btree.currentItem = nil
	if btree.currentItemRef.nodeId == nodeId && btree.currentItemRef.getNodeItemIndex() == itemIndex {
		return
	}
	btree.currentItemRef.nodeId = nodeId
	btree.currentItemRef.nodeItemIndex = itemIndex
}

func (btree *Btree[TK, TV]) isUnique() bool {
	return btree.Store.IsUnique
}

func (btree *Btree[TK, TV]) getSlotLength() int {
	return btree.Store.SlotLength
}

func (btree *Btree[TK, TV]) isCurrentItemSelected() bool {
	return btree.currentItemRef.getNodeId() != NilUUID
}

// distribute function allows B-Tree to avoid using recursion. I.e. - instead of the node calling
// a recursive function that distributes or moves an item from a source node to a vacant slot somewhere
// in the sibling nodes, distribute allows a controller(distribute)-controllee(node.DistributeLeft or XxRight)
// pattern and avoids recursion.
func (btree *Btree[TK, TV]) distribute() {
	for btree.distributeAction.sourceNode != nil {
		log.Debug(fmt.Sprintf("Distribute item with key(%v) of node Id(%v) to left(%v).",
			btree.distributeAction.item.Key, btree.distributeAction.sourceNode.Id, btree.distributeAction.distributeToLeft))
		n := btree.distributeAction.sourceNode
		btree.distributeAction.sourceNode = nil
		item := btree.distributeAction.item
		btree.distributeAction.item = nil

		// Node DistributeLeft or XxRight contains actual logic of "item distribution".
		if btree.distributeAction.distributeToLeft {
			n.distributeToLeft(btree, item)
		} else {
			n.distributeToRight(btree, item)
		}
	}
}

// promote function allows B-Tree to avoid using recursion. I.e. - instead of the node calling
// a recursive function that promotes a sub-tree "parent" node for insert on a vacant slot,
// promote allows a controller(btree.promote)-controllee(node.promote) pattern and avoid recursion.
func (btree *Btree[TK, TV]) promote() {
	for btree.promoteAction.targetNode != nil {
		log.Debug(fmt.Sprintf("Promote will promote a Node with Id %v.", btree.promoteAction.targetNode.Id))
		n := btree.promoteAction.targetNode
		i := btree.promoteAction.slotIndex
		btree.promoteAction.targetNode = nil
		btree.promoteAction.slotIndex = 0
		// Node's promote method contains actual logic to promote a (new parent outcome of
		// splittin a full node) node to higher up.
		n.promote(btree, i)
	}
}
