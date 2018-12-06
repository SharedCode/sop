package btree

import "../transaction"

type Btree struct{
	store *Store
	nodeRepository NodeRepository
}

func (btree *Btree) rootNode() (*Node, error) {
	return btree.nodeRepository.Get(btree.store.RootNodeID)
}

func (btree *Btree) setCurrentItem(){

}

func (btree *Btree) setCurrentItemAddress(nodeAddress UUID, itemIndex int){
	// if (CurrentItem == null) {return}
	// CurrentItem.NodeAddress = itemNodeAddress;
	// CurrentItem.NodeItemIndex = itemIndex;
}

func (btree *Btree) isUnique() bool{
	return btree.store.IsUnique
}

func NewBtree(store *Store, nodeRepo NodeRepository, trans transaction.Transaction) BtreeInterface{
	var r = Btree{
		store:store,
		nodeRepository:nodeRepo,
	}
	return &r;
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
