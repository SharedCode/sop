package btree

import (
	"fmt"
)

// Btree manages items using B-tree data structure and algorithm.
type Btree struct{
	Store Store
	StoreInterface StoreInterface
	TempSlots []Item
	TempChildren []UUID
	CurrentItem CurrentItemRef
}

type CurrentItemRef struct{
	NodeAddress Handle
	NodeItemIndex int
}

func NewBtree(store Store, si StoreInterface) *Btree{
	if !store.ItemSerializer.IsValid(){
		// provide string key/value type handlers if not provided or invalid.
		store.ItemSerializer.CreateDefaultKVTypeHandlers()
	}
	var b3 = Btree{
		Store: store,
		StoreInterface: si,
		TempSlots: make([]Item, store.NodeSlotCount+1),
		TempChildren: make([]UUID, store.NodeSlotCount+2),
	}	
	return &b3
}

func (btree *Btree) rootNode() (*Node, error) {
	if btree.Store.RootNodeID.IsEmpty() {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeID = NewHandle(btree.StoreInterface.VirtualIDRepository.NewUUID())
		var root = NewNode(btree.Store.NodeSlotCount)
		root.ID = btree.Store.RootNodeID
		return root, nil
	}
	root, e := btree.getNode(btree.Store.RootNodeID)
	if e != nil {return nil, e}
	if root == nil{
		return nil, fmt.Errorf("Can't retrieve Root Node w/ ID '%s'", btree.Store.RootNodeID.ToString())
	}
	return root, nil
}

func (btree *Btree) getNode(id Handle) (*Node, error){
	n, e := btree.StoreInterface.NodeRepository.Get(id)
	if e != nil {return nil, e}
	return n, nil
}

func (btree *Btree) setCurrentItemAddress(nodeAddress Handle, itemIndex int){
	btree.CurrentItem.NodeAddress = nodeAddress;
	btree.CurrentItem.NodeItemIndex = itemIndex;
}

func (btree *Btree) isUnique() bool{
	return btree.Store.IsUnique
}

func (btree *Btree) Add(key interface{}, value interface{}) (bool, error) {
	if key == nil{
		panic("key can't be nil.")
	}
	var itm = Item{
		Key:key,
		Value:value,
	}
	node, err := btree.rootNode()
	if err != nil {return false, err}
	return node.add(btree, itm);
}

func (btree *Btree) Update(key interface{}, value interface{}) (bool, error){
	return false, nil
}
func (btree *Btree) UpdateCurrentItem(newValue interface{}) (bool, error){
	return false, nil
}
func (btree *Btree) Remove(key interface{}) (bool, error){
	return false, nil
}
func (btree *Btree) RemoveCurrentItem() (bool, error){
	return false, nil
}

func (btree *Btree) MoveTo(key interface{}, firstItemWithKey bool) (bool, error) {

	m := make(map[string]int)
	v,_ := m["foo"]
	return v == 0, nil

//	return false
}

func (btree *Btree) MoveToFirst() (bool, error){
	return false, nil
}
func (btree *Btree) MoveToLast() (bool, error){
	return false, nil
}
func (btree *Btree) MoveToNext() (bool, error){
	return false, nil
}
func (btree *Btree) MoveToPrevious()( bool, error){
	return false, nil
}
