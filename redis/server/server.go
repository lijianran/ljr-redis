// 2021.12.11
// redis server

package server

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	database2 "ljr-redis/database"
	"ljr-redis/interface/database"
	"ljr-redis/lib/logger"
	"ljr-redis/lib/sync/atomic"
	"ljr-redis/redis/connection"
	"ljr-redis/redis/parser"
	"ljr-redis/redis/reply"
)

var (
	unknowErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RedisHandler struct {
	activeConn sync.Map       // *client placeholder
	db         database.DB    // redis 底层存储
	closing    atomic.Boolean // 拒绝新客户端 client 和新的请求 request
}

// 服务器构造器，返回 redis 服务器实例
func MakeRedisHandler() *RedisHandler {
	// var db database.DB

	// 单机模式
	db := database2.NewStandaloneServer()
	return &RedisHandler{
		db: db,
	}
}

// 处理 redis 客户端的连接
func (h *RedisHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 关闭连接，拒绝请求
		_ = conn.Close()
	}

	// 创建新连接，存储到连接池
	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	// 开始解析请求
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			// 读取错误
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 关闭连接
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			// 协议错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			continue
		}

		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}

		// 执行指令
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknowErrReplyBytes)
		}

	}
}

// 关闭客户端连接
func (h *RedisHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// 关闭 redis 服务器
func (h *RedisHandler) Close() error {
	logger.Info("ljredis shutting down...")
	h.closing.Set(true)

	// 逐个关闭连接池中的活动连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		// 关闭客户端连接
		_ = client.Close()
		return true
	})

	h.db.Close()
	return nil
}
