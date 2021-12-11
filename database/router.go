// 2021.12.11
// 路由

package database

import "strings"

// 指令列表
var cmdTable = make(map[string]*command)

// 抽象指令
type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int
	// flags    int
}

// 注册指令
func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int) {
	// arity 代表参数个数
	// arity < 0 表示 len(args) >= -arity
	// 例如: get.arity=2  mget.arity=-2
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
	}
}
