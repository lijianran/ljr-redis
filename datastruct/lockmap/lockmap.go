// 2021.12.11
// ConcurrentDict 只能保证对单个 key 操作的并发安全性
// LockMap

package lockmap

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

// 哈希 map 锁
type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)

	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}

	return &Locks{
		table: table,
	}
}

// 哈希算法
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & uint32(hashCode)
}

// 上写锁 write
func (locks *Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

// 开写锁 write
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

// 上读锁 read
func (locks *Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

// 开读锁 read
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

// 避免循环等待死锁
// 所有协程都按照相同的顺序加锁
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]bool)

	for _, key := range keys {
		index := locks.spread(fnv32(key))
		indexMap[index] = true
	}

	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}

	// 排序
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})

	return indices
}

// 复杂指令 上写锁 write
func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Lock()
	}
}

// 复杂指令 开写锁 write
func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

// 复杂指令 上读锁 read
func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

// 复杂指令 开读锁 read
func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

// 复杂指令 上读写锁 允许 writeKeys 和 readKeys 中存在重复的 key
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	// 全部锁列表
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)

	// 写锁列表
	writeIndices := locks.toLockIndices(writeKeys, false)
	writeIndiceSet := make(map[uint32]struct{}, 0)

	// 是否上写锁
	for _, index := range writeIndices {
		writeIndiceSet[index] = struct{}{}
	}

	for _, index := range indices {
		_, w := writeIndiceSet[index]
		mu := locks.table[index]
		if w {
			// 写锁
			mu.Lock()
		} else {
			// 读锁
			mu.RLock()
		}
	}
}

// 复杂指令 开读写锁
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	// 全部锁列表
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, true) // 逆序开锁

	// 写锁列表
	writeIndices := locks.toLockIndices(writeKeys, true) // 逆序开锁
	writeIndiceSet := make(map[uint32]struct{}, 0)

	// 是否开写锁
	for _, index := range writeIndices {
		writeIndiceSet[index] = struct{}{}
	}

	for _, index := range indices {
		_, w := writeIndiceSet[index]
		mu := locks.table[index]
		if w {
			// 开写锁
			mu.Unlock()
		} else {
			// 开读锁
			mu.RUnlock()
		}
	}
}
