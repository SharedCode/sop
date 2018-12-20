package btree

func (node *Node) insertSlotItem(item Item, position int){
	copy(node.Slots[position+1:], node.Slots[position:])
	node.Slots[position] = item
}
