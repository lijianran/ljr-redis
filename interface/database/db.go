// 2021.12.11

package database

import "ljr-redis/interface/redis"

// 指令
type CmdLine = [][]byte

// 数据库引擎接口
type DB interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply

	AfterClientClose(c redis.Connection)
	Close()
}

// 数据实例 key 绑定到 string, list, hast, set 等等
type DataEntity struct {
	Data interface{}
}
