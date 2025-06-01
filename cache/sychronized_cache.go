package cache

import (
	"sync"

	"github.com/SharedCode/sop"
)

type sync_cache[TK comparable, TV any] struct {
	cache  Cache[TK, TV]
	locker *sync.Mutex
}

// NewSynchronizedCache returns a Cache instance that is thread safe.
func NewSynchronizedCache[TK comparable, TV any](minCapacity, maxCapacity int) Cache[TK, TV] {
	return &sync_cache[TK, TV]{
		cache:  NewCache[TK, TV](minCapacity, maxCapacity),
		locker: &sync.Mutex{},
	}
}

func (sc *sync_cache[TK, TV]) Set(items []sop.KeyValuePair[TK, TV]) {
	sc.locker.Lock()
	sc.cache.Set(items)
	sc.locker.Unlock()
}
func (sc *sync_cache[TK, TV]) Get(keys []TK) []TV {
	sc.locker.Lock()
	defer sc.locker.Unlock()
	return sc.cache.Get(keys)
}

func (sc *sync_cache[TK, TV]) Delete(keys []TK) {
	sc.locker.Lock()
	sc.cache.Delete(keys)
	sc.locker.Unlock()
}

func (sc *sync_cache[TK, TV]) Clear() {
	sc.locker.Lock()
	sc.cache.Clear()
	sc.locker.Unlock()
}

func (sc sync_cache[TK, TV]) Count() int {
	return sc.cache.Count()
}

func (sc sync_cache[TK, TV]) IsFull() bool {
	return sc.cache.IsFull()
}

func (sc sync_cache[TK, TV]) Evict() {
	sc.cache.Evict()
}
