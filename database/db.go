// 2021.12.11
// 数据储存引擎基础功能

package database

import (
	"strings"
	"sync"
	"time"

	"ljr-redis/datastruct/dict"
	"ljr-redis/datastruct/lockmap"
	"ljr-redis/interface/database"
	"ljr-redis/interface/redis"
	"ljr-redis/lib/logger"
	"ljr-redis/lib/timewheel"
	"ljr-redis/redis/reply"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// 数据库抽象，能存储数据和执行指令
type DB struct {
	index      int       // 数据库索引
	data       dict.Dict // 字典数据类型
	ttlMap     dict.Dict // time.Time
	versionMap dict.Dict // version (uint32)
	// dict.Dict 能保证并发安全

	// lockmap 用于处理复杂指令 rpush incr ...
	locker *lockmap.Locks

	// 停止数据操作 execFlushDB
	stopWorld sync.WaitGroup

	// AOF
	addAof func(CmdLine)
}

// executor ExecFunc
// 指令执行接口
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// prepare  PreFunc
// 复杂指令与处理
type PreFunc func(args [][]byte) ([]string, []string)

// undo     UndoRunc
// 取消指令执行
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// 指令
type CmdLine = [][]byte

// 返回数据库实例
func MakeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lockmap.Make(lockerSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// 执行指令
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		// 开始执行复杂指令
		return StartMulti(c)

	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		// 取消执行复杂指令
		return DiscardMulti(c)

	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		// 执行复杂指令
		return execMulti(db, c)

	} else if cmdName == "watch" {
		// 参数个数不少于 2
		if !validateArity(-2, cmdLine) {
			return reply.MakeArgNumErrReply(cmdName)
		}
		// watch
		return Watch(db, c, cmdLine[1:])

	}

	if c != nil && c.InMultiState() {
		// 要执行复杂指令，将指令加入队列
		EnqueueCmd(c, cmdLine)
		return reply.MakeQueuedReply()
	}

	// 执行普通指令
	return db.execNormalCommand(cmdLine)
}

// 执行普通指令
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		// return reply.MakeErrReply("ERR unkonwn command '" + cmdName + "'")
		return reply.MakeErrReply("爷还没写 '" + cmdName + "' 这个指令")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare((cmdLine[1:]))

	db.addVersion(write...)
	db.RWLocks(write, read)

	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// 检查指令参数个数
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---------- lock 执行复杂指令 ------------ */

func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---------- version ------------ */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

/* ------------ Time To Live 过期时间 ------------ */

func genExpireTask(key string) string {
	return "expire:" + key
}

// 设置 key 的 ttlCmd
func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	// 记录过期时间
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)

	// 设置定时任务 key 过期后自动删除
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)

		logger.Info("expire:" + key)
		rawExpireTime, ok := db.ttlMap.Get(key)

		if !ok {
			// ttl 可能在等待 lock 的时候更新
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// 取消 key 的 ttl
func (db *DB) Persist(key string) {
	db.stopWorld.Wait()
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// 检查是否在 ttl 内
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		// 已经到期
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		// 到期删除
		db.Remove(key)
	}
	return expired
}

/* ------------ data ------------ */

// 获取数据
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	db.stopWorld.Wait()

	raw, ok := db.data.Get(key)
	if !ok {
		// 没有数据
		return nil, false
	}
	if db.IsExpired(key) {
		// 到期
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// 存储数据
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.Put(key, entity)
}

// 修改数据
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfExists(key, entity)
}

// 插入数据，只在 key 不存在时
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfAbsent(key, entity)
}

// 从 db 中删除 key
func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// 从 db 中删除 keys 返回成功删除的个数
func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWorld.Wait()
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return
}

// 清空数据库
func (db *DB) Flush() {
	db.stopWorld.Add(1)
	defer db.stopWorld.Done()

	db.data.Clear()
	db.ttlMap.Clear()
	db.locker = lockmap.Make(lockerSize)
}
