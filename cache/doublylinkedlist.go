package cache

// node represents an element in the doubly linked list.
type node[T any] struct {
	data T
	prev *node[T]
	next *node[T]
}

// doublyLinkedList is a minimal, allocation-friendly doubly linked list used by MRU caches.
type doublyLinkedList[T any] struct {
	head *node[T]
	tail *node[T]
	size int
}

// newDoublyLinkedList creates a new empty doubly linked list.
func newDoublyLinkedList[T any]() *doublyLinkedList[T] {
	return &doublyLinkedList[T]{nil, nil, 0}
}

// count returns the number of elements in the list.
func (dll *doublyLinkedList[T]) count() int {
	return dll.size
}

// isEmpty reports whether the list has no elements.
func (dll *doublyLinkedList[T]) isEmpty() bool {
	return dll.head == nil
}

// addToHead inserts a new node with data at the head of the list and returns it.
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

// deleteFromTail removes and returns the tail node's data.
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

// delete unchains the node n from the list.
func (dll *doublyLinkedList[T]) delete(n *node[T]) bool {
	if n == nil {
		return false
	}

	if n == dll.head {
		dll.head = n.next
	}
	if n == dll.tail {
		dll.tail = n.prev
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
