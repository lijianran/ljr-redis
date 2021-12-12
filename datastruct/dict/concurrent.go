// 2021.12.11
// 分段锁策略实现 HashMap
// ConcurrentDict 可以保证对单个 key 操作的并发安全性

package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

// 通过 sharding lock 锁实现的线程安全的 map
type ConcurrentDict struct {
	table      []*Shard // shard 存储块
	count      int32    // 数据总数
	shardCount int      // shard 个数
}

type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 计算 shard 个数
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	if n < 0 {
		return math.MaxInt32
	}

	return n + 1
}

// 根据 shard 的个数创建 ConcurrentDict
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)

	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}

	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}

	return d
}

// 哈希算法
const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 散列，第一次哈希，定位要存储的 shard 位置
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	// 当 size 为 2 的整数幂时 hashCode % size == (size - 1) & hashCode
	return (tableSize - 1) & uint32(hashCode)
}

// 获取 shard 存储块
func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// 数据总数加一
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

// 数据总数减一
func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// 获取随机 key
func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

/* ------------------ 安排 datastruct/dict 接口  ------------------ */

// Get 方法
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算哈希 code
	hashCode := fnv32(key)
	// 获取 shard 索引
	index := dict.spread(hashCode)
	// 获取 shard 存储块
	shard := dict.getShard(index)

	// 上读锁
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	val, exists = shard.m[key]
	return
}

// Len 方法
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	// 原子量加载 不需要上锁
	return int(atomic.LoadInt32(&dict.count))
}

// Put 方法
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算哈希 code
	hashCode := fnv32(key)
	// 获取 shard 索引
	index := dict.spread(hashCode)
	// 获取 shard 存储块
	shard := dict.getShard(index)

	// 上写锁
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		// key 存在，直接修改 value
		shard.m[key] = val
		return 0
	}

	// key 不存在，新增
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfAbsent key 不存在
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算哈希 code
	hashCode := fnv32(key)
	// 获取 shard 索引
	index := dict.spread(hashCode)
	// 获取 shard 存储块
	shard := dict.getShard(index)

	// 上写锁
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		// key 存在
		return 0
	}
	// key 不存在，新增
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists key 存在
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算哈希 code
	hashCode := fnv32(key)
	// 获取 shard 索引
	index := dict.spread(hashCode)
	// 获取 shard 存储块
	shard := dict.getShard(index)

	// 上写锁
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		// key 存在 更新数据
		shard.m[key] = val
		return 1
	}
	// key 不存在 不更新
	return 0
}

// Remove 删除数据
func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算哈希 code
	hashCode := fnv32(key)
	// 获取 shard 索引
	index := dict.spread(hashCode)
	// 获取 shard 存储块
	shard := dict.getShard(index)

	// 上写锁
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		// 有数据 删除
		delete(shard.m, key)
		// 数据总数减一
		dict.decreaseCount()
		return 1
	}

	return 0
}

// 循环遍历
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, shard := range dict.table {
		shard.mutex.RLock()
		func() {
			defer shard.mutex.Unlock()

			for key, value := range shard.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

// 获取所有 keys
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// 获取随机 limit 个 key 可能包含重复的
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	for i := 0; i < limit; {
		// 随机取一个 shard 存储块
		shard := dict.getShard(uint32(rand.Intn(shardCount)))
		if shard == nil {
			// 没有数据
			continue
		}

		// 随机取一个 key
		key := shard.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}

	return result
}

// 获取随机 limit 个 key 不包含重复的
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make(map[string]bool, 0)
	for len(result) < limit {
		// 随机取一个 shard 存储块
		shard := dict.getShard(uint32(rand.Intn(shardCount)))
		if shard == nil {
			// 没有数据
			continue
		}

		// 随机取一个 key
		key := shard.RandomKey()
		if key != "" {
			result[key] = true
		}
	}

	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}

	return arr
}

// 清空数据
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}
