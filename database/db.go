// 2021.12.11
// 数据储存引擎基础功能

package database

import (
	"strings"

	"ljr-redis/datastruct/dict"
	"ljr-redis/datastruct/lockmap"
	"ljr-redis/interface/redis"
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
	// stopWorld sync.WaitGroup

	// AOF
	addAof func(CmdLine)
}

// executor ExecFunc
// prepare  PreFunc
// undo     UndoRunc
// 命令执行接口
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// 复杂命令与处理
type PreFunc func(args [][]byte) ([]string, []string)

// 取消命令执行
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// 命令
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

	return db.execNormalCommand(cmdLine)
}

// 执行普通命令
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

// 检查命令参数个数
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---------- lock 执行复杂命令 ------------ */
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---------- add version ------------ */
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
