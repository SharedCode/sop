package btree

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
	return &Btree{
		Store: store,
		StoreInterface: si,
		TempSlots: make([]Item, store.NodeSlotCount+1),
		TempChildren: make([]UUID, store.NodeSlotCount+2),
	}
}

func (btree *Btree) rootNode() (*Node, error) {
	if btree.Store.RootNodeID == nil {
		// create new Root Node, if nil (implied new btree).
		btree.Store.RootNodeID = NewHandle(btree.StoreInterface.VirtualIDRepository.NewUUID())
		return &Node{ID: btree.Store.RootNodeID}, nil
	}
	return btree.StoreInterface.NodeRepository.Get(btree.Store.RootNodeID)
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
