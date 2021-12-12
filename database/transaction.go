// 2021.12.11
// 单机事务

package database

import (
	"strings"

	"ljr-redis/interface/redis"
	"ljr-redis/redis/reply"
)

// 开始执行复杂指令
func StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		// 已经在执行复杂指令，不能叠加
		return reply.MakeErrReply("ERR MULTI calls can not be nested")
	}
	// 设置在执行复杂指令
	conn.SetMultiState(true)
	return reply.MakeOkReply()
}

// 取消还未执行的复杂指令
func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		// 还没有开始执行复杂指令，不能取消
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}
	// 清空指令队列
	conn.ClearQueuedCmds()
	// 设置没有执行复杂指令
	conn.SetMultiState(false)
	return reply.MakeOkReply()
}

// 获取滚回的指令
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

// 执行复杂指令
func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}

// Watch 设置 watching keys
func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	// 获取 watching 列表
	watching := conn.GetWatching()
	// 获取参数
	for _, bkey := range args {
		// []byte 转成 string
		key := string(bkey)
		watching[key] = db.GetVersion(key)
	}
	return reply.MakeOkReply()
}

// 判断 watching keys 是否改变
func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			// 改变了
			return true
		}
	}
	// 没改变
	return false
}

// 将指令加入待执行的复杂指令队列
func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 取指令
	cmd, ok := cmdTable[cmdName]
	if !ok {
		// 指令列表中没有，未知指令
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// if forbiddenInMulti.Has(cmdName) {
	// 	return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	// }
	if cmd.prepare == nil {
		// 不是复杂指令
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		// 和 redis 不同，不执行参数个数错误的指令
		return reply.MakeArgNumErrReply(cmdName)
	}
	// 插入指令到队列
	conn.EnqueueCmd(cmdLine)
	return reply.MakeQueuedReply()
}

// 原子隔离 执行复杂指令
func (db *DB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	// prepare 准备
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)

	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	// 设置 watching
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	if isWatchingChanged(db, watching) {
		// watching keys 改变了 abort 终止
		return reply.MakeEmptyMultiBulkReply()
	}

	// 执行指令
	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))

	for _, cmdLine := range cmdLines {
		// 回滚日志
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		// 上锁执行 返回响应
		result := db.execWithLock(cmdLine)
		// 如果是失败的操作 头部: '-'
		if reply.IsErrorReply(result) {
			aborted = true
			// 不滚回失败的指令
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		// 加入结果响应集合
		results = append(results, result)
	}

	if !aborted {
		// 成功
		db.addVersion(writeKeys...)
		return reply.MakeMultiRawReply(results)
	}

	// 如果 abort 就 undo 回滚
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		// 逆向回滚
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}

	return reply.MakeErrReply("Exec abort transaction discaded because of previous errors.")
}

// 上锁执行指令
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 取指令
	cmd, ok := cmdTable[cmdName]

	if !ok {
		return reply.MakeErrReply("ERR unknow command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	// 执行
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}
