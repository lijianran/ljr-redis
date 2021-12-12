// 2021.12.12
// 系统功能

package database

import (
	"ljr-redis/config"
	"ljr-redis/interface/redis"
	"ljr-redis/redis/reply"
)

// Ping
func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("指令 ping 的参数个数错误")
	}
}

// 认证
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("指令 auth 的参数个数错误")
	}
	if config.Properties.RequirePass == "" {
		return reply.MakeErrReply("还没有设置密码")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Properties.RequirePass != passwd {
		return reply.MakeErrReply("密码错误")
	}
	return &reply.OkReply{}
}

// 是否认证
// func isAuthenticated(c redis.Connection) bool {
// 	if config.Properties.RequirePass == "" {
// 		return true
// 	}
// 	return c.GetPassword() == config.Properties.RequirePass
// }

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func init() {
	// ping 参数个数大于 1
	RegisterCommand("ping", Ping, noPrepare, nil, -1)
}
