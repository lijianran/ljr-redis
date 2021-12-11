// 2021.12.11
// 路由

package database

// 指令列表
var cmdTable = make(map[string]*command)

// 抽象指令
type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int
	flags    int
}
