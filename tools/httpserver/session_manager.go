package main

import (
	"sync"
	"time"

	"github.com/sharedcode/sop/ai"
)

type sessionNode struct {
	sessionID string
	agent     ai.Agent[map[string]any]
	mutex     sync.Mutex // lock for the agent's operations to prevent concurrent usage
	prev      *sessionNode
	next      *sessionNode
	lastUse   time.Time
}

// SessionManager provides an MRU/LRU bounded cache for agent sessions
type SessionManager struct {
	mu       sync.Mutex // Note: we use Mutex rather than RWMutex because Get operations mutate the list (Move to MRU)
	lookup   map[string]*sessionNode
	head     *sessionNode
	tail     *sessionNode
	capacity int
}

func NewSessionManager(capacity int) *SessionManager {
	return &SessionManager{
		lookup:   make(map[string]*sessionNode),
		capacity: capacity,
	}
}

// Get returns the agent for the specified sessionID and promotes it to the MRU (head).
func (sm *SessionManager) Get(sessionID string) (ai.Agent[map[string]any], bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	node, ok := sm.lookup[sessionID]
	if !ok {
		return nil, false
	}

	node.lastUse = time.Now()
	sm.moveToHead(node)
	return node.agent, true
}

// Put adds or updates the session. If capacity is reached, it evicts the LRU (tail).
func (sm *SessionManager) Put(sessionID string, agent ai.Agent[map[string]any]) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if node, ok := sm.lookup[sessionID]; ok {
		node.agent = agent
		node.lastUse = time.Now()
		sm.moveToHead(node)
		return
	}

	if len(sm.lookup) >= sm.capacity {
		sm.evictLRU()
	}

	node := &sessionNode{
		sessionID: sessionID,
		agent:     agent,
		lastUse:   time.Now(),
	}
	sm.lookup[sessionID] = node
	sm.addToHead(node)
}

// Close removes a session explicitly.
func (sm *SessionManager) Close(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if node, ok := sm.lookup[sessionID]; ok {
		sm.removeNode(node)
		delete(sm.lookup, sessionID)
	}
}

func (sm *SessionManager) moveToHead(node *sessionNode) {
	if sm.head == node {
		return
	}
	sm.removeNode(node)
	sm.addToHead(node)
}

func (sm *SessionManager) addToHead(node *sessionNode) {
	node.next = sm.head
	node.prev = nil
	if sm.head != nil {
		sm.head.prev = node
	}
	sm.head = node
	if sm.tail == nil {
		sm.tail = node
	}
}

func (sm *SessionManager) removeNode(node *sessionNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		sm.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		sm.tail = node.prev
	}
}

func (sm *SessionManager) evictLRU() {
	if sm.tail == nil {
		return
	}
	lru := sm.tail
	sm.removeNode(lru)
	delete(sm.lookup, lru.sessionID)
}

// GetOrCreate retrieves the session and its lock, safely resolving races.
func (sm *SessionManager) GetOrCreate(sessionID string, builder func() ai.Agent[map[string]any]) (ai.Agent[map[string]any], *sync.Mutex) {
	sm.mu.Lock()
	if node, ok := sm.lookup[sessionID]; ok {
		node.lastUse = time.Now()
		sm.moveToHead(node)
		sm.mu.Unlock()
		return node.agent, &node.mutex
	}
	sm.mu.Unlock()

	agent := builder()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Double check
	if node, ok := sm.lookup[sessionID]; ok {
		node.lastUse = time.Now()
		sm.moveToHead(node)
		return node.agent, &node.mutex
	}

	if len(sm.lookup) >= sm.capacity {
		sm.evictLRU()
	}

	node := &sessionNode{
		sessionID: sessionID,
		agent:     agent,
		lastUse:   time.Now(),
	}
	sm.lookup[sessionID] = node
	sm.addToHead(node)

	return node.agent, &node.mutex
}

// RemoveStale evicts all sessions that have not been used within the given ttl duration.
// Returns the number of sessions evicted.
func (sm *SessionManager) RemoveStale(ttl time.Duration) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	evictedCount := 0

	// The tail is the least recently used. If it's stale, remove it and check the new tail.
	for sm.tail != nil {
		if now.Sub(sm.tail.lastUse) >= ttl {
			lru := sm.tail
			sm.removeNode(lru)
			delete(sm.lookup, lru.sessionID)
			evictedCount++
		} else {
			// Since it's an MRU/LRU list, if the tail isn't stale, nothing else is.
			break
		}
	}
	return evictedCount
}
