package btree

import (
	"fmt"
)

// Btree manages items using B-tree data structure and algorithm.
// Store contains the  
type Btree[TKey Comparable, TValue any] struct{
	Store Store
	StoreInterface StoreInterface[TKey, TValue]
	TempSlots []*Item[TKey, TValue]
	TempChildren []UUID
	CurrentItem CurrentItemRef
}

type CurrentItemRef struct{
	NodeAddress Handle
	NodeItemIndex int
}

func NewBtree[TKey Comparable, TValue any](store Store, si StoreInterface[TKey, TValue]) *Btree[TKey, TValue]{
	var b3 = Btree[TKey, TValue]{
		Store: store,
		StoreInterface: si,
		TempSlots: make([]*Item[TKey, TValue], store.NodeSlotCount+1),
		TempChildren: make([]UUID, store.NodeSlotCount+2),
	}
	return &b3
}

func (btree *Btree[TKey, TValue]) rootNode() (*Node[TKey, TValue], error) {
	if btree.Store.RootNodeId.IsEmpty() {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeId = NewHandle(btree.StoreInterface.VirtualIdRepository.NewUUID())
		var root = NewNode[TKey, TValue](btree.Store.NodeSlotCount)
		root.Id = btree.Store.RootNodeId
		return root, nil
	}
	root, e := btree.getNode(btree.Store.RootNodeId)
	if e != nil {return nil, e}
	if root == nil{
		return nil, fmt.Errorf("Can't retrieve Root Node w/ Id '%s'", btree.Store.RootNodeId.ToString())
	}
	return root, nil
}

func (btree *Btree[TKey, TValue]) getNode(id Handle) (*Node[TKey, TValue], error){
	n, e := btree.StoreInterface.NodeRepository.Get(id)
	if e != nil {return nil, e}
	return n, nil
}

func (btree *Btree[TKey, TValue]) setCurrentItemAddress(nodeAddress Handle, itemIndex int){
	btree.CurrentItem.NodeAddress = nodeAddress;
	btree.CurrentItem.NodeItemIndex = itemIndex;
}

func (btree *Btree[TKey, TValue]) isUnique() bool{
	return btree.Store.IsUnique
}

func (btree *Btree[TKey, TValue]) Add(key TKey, value TValue) (bool, error) {
	var itm = Item[TKey, TValue]{
		Key:key,
		Value:value,
	}
	node, err := btree.rootNode()
	if err != nil {return false, err}
	return node.add(btree, &itm);
}

func (btree *Btree[TKey, TValue]) Update(key TKey, value TValue) (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) UpdateCurrentItem(newValue TValue) (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) Remove(key TKey) (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) RemoveCurrentItem() (bool, error){
	return false, nil
}

func (btree *Btree[TKey, TValue]) MoveTo(key TKey, firstItemWithKey bool) (bool, error) {

	m := make(map[string]int)
	v,_ := m["foo"]
	return v == 0, nil

//	return false
}

func (btree *Btree[TKey, TValue]) MoveToFirst() (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) MoveToLast() (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) MoveToNext() (bool, error){
	return false, nil
}
func (btree *Btree[TKey, TValue]) MoveToPrevious()( bool, error){
	return false, nil
}
