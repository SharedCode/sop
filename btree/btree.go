package btree

import (
	"fmt"
)

type Btree struct{
	Store *Store
	StoreInterface *StoreInterface
	TempSlots []Item
	TempChildren []UUID
	CurrentItem CurrentItemRef
}

type CurrentItemRef struct{
	NodeAddress *Handle
	NodeItemIndex int
}

func NewBtree(store *Store, si *StoreInterface) *Btree{
	if !store.ItemSerializer.IsValid(){
		// provide string key/value type handlers if not provided (invalid).
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
	if btree.Store.RootNodeID == nil {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeID = NewHandle(btree.StoreInterface.VirtualIDRepository.NewUUID())
		return NewNode(btree.Store.NodeSlotCount), nil
	}
	root, e := btree.getNode(btree.Store.RootNodeID)
	if e != nil {return nil, e}
	if root == nil{
		return nil, fmt.Errorf("Can't retrieve Root Node w/ ID '%s'", btree.Store.RootNodeID.ToString())
	}
	return root, nil
}

func (btree *Btree) getNode(id *Handle) (*Node, error){
	n, e := btree.StoreInterface.NodeRepository.Get(id)
	if e != nil {return nil, e}
	return n, nil
}

// func (btree *Btree) setCurrentItem(){
// }

func (btree *Btree) setCurrentItemAddress(nodeAddress *Handle, itemIndex int){
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

// Search will find the Item with the specified key, position the Btree record
// pointer to the found Item and return true if found, otherwise false.
// 'gotoFirstOccurrence' parameter is useful when Btree allows duplicate keyed items 
// (Unique = false). This is a hint which will cause Btree to position the record pointer 
// to the first Item that has the specified key.
// Typical case is, to traverse the tree examining each Item with this same key.
func (btree *Btree) Search(key interface{}, gotoFirstOccurrence bool) bool {

	return false
}
