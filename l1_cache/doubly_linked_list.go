package l1_cache

// node represents a node in the doubly linked list
type node[T any] struct {
	data T
	prev *node[T]
	next *node[T]
}

// doublyLinkedList represents the doubly linked list
type doublyLinkedList[T any] struct {
	head *node[T]
	tail *node[T]
	size int
}

// newDoublyLinkedList creates a new empty doubly linked list
func newDoublyLinkedList[T any]() *doublyLinkedList[T] {
	return &doublyLinkedList[T]{nil, nil, 0}
}

// count returns the number of elements in the list
func (dll *doublyLinkedList[T]) count() int {
	return dll.size
}

// IsEmpty checks if the list is empty
func (dll *doublyLinkedList[T]) isEmpty() bool {
	return dll.head == nil
}

// AddToHead adds a new node with the given data to the head of the list
func (dll *doublyLinkedList[T]) addToHead(data T) *node[T] {
	newNode := &node[T]{data: data, prev: nil, next: dll.head}
	if dll.head != nil {
		dll.head.prev = newNode
	} else {
		dll.tail = newNode
	}
	dll.head = newNode
	dll.size++
	return newNode
}

// AddToTail adds a new node with the given data to the tail of the list
func (dll *doublyLinkedList[T]) addToTail(data T) *node[T] {
	newNode := &node[T]{data: data, prev: dll.tail, next: nil}
	if dll.tail != nil {
		dll.tail.next = newNode
	} else {
		dll.head = newNode
	}
	dll.tail = newNode
	dll.size++
	return newNode
}

// DeleteFromHead removes the node from the head of the list
func (dll *doublyLinkedList[T]) deleteFromHead() (T, bool) {
	var d T
	if dll.isEmpty() {
		return d, false
	}
	data := dll.head.data
	if dll.head == dll.tail {
		dll.head = nil
		dll.tail = nil
	} else {
		dll.head = dll.head.next
		dll.head.prev = nil
	}
	dll.size--
	return data, true
}

// DeleteFromTail removes the node from the tail of the list
func (dll *doublyLinkedList[T]) deleteFromTail() (T, bool) {
	var d T
	if dll.isEmpty() {
		return d, false
	}
	data := dll.tail.data
	if dll.head == dll.tail {
		dll.head = nil
		dll.tail = nil
	} else {
		dll.tail = dll.tail.prev
		dll.tail.next = nil
	}
	dll.size--
	return data, true
}

// Delete & unchain the node "n" from the doubly linked list.
func (dll *doublyLinkedList[T]) delete(n *node[T]) bool {
	if n == nil {
		return false
	}
	p := n.prev
	if p != nil {
		p.next = n.next
	}
	nxt := n.next
	if nxt != nil {
		nxt.prev = p
	}
	n.next = nil
	n.prev = nil

	dll.size--
	return true
}

// PeekHead returns the data of the head node without removing it
func (dll *doublyLinkedList[T]) peekHead() (T, bool) {
	var d T
	if dll.isEmpty() {
		return d, false
	}
	return dll.head.data, true
}

// PeekTail returns the data of the tail node without removing it
func (dll *doublyLinkedList[T]) peekTail() (T, bool) {
	var d T
	if dll.isEmpty() {
		return d, false
	}
	return dll.tail.data, true
}
