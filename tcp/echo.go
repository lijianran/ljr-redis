// 2021.12.10
// Echo 客户端

package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"

	"ljr-redis/lib/logger"
	"ljr-redis/lib/sync/atomic"
	"ljr-redis/lib/sync/wait"
)

type Client struct {
	// tcp
	Conn net.Conn

	Waiting wait.Wait
}

type EchoHandler struct {
	// 连接池
	activeConn sync.Map

	// 关闭标识位
	closing atomic.Boolean
}

// 返回 Echo 服务器实例
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// 处理 Echo 连接
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 正在关闭中的 handler 不会处理新连接
	if h.closing.Get() {
		conn.Close()
	}

	client := &Client{
		Conn: conn,
	}
	// 存储 activate 的连接
	h.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				// 从连接池中删除
				h.activeConn.Delete(conn)
			} else {
				logger.Warn(err)
			}

			return
		}

		client.Waiting.Add(1)

		b := []byte(msg)
		conn.Write(b)

		client.Waiting.Done()
	}

}

// 关闭客户端连接
func (c *Client) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// 关闭服务器
func (h *EchoHandler) Close() error {
	logger.Info("handle shutting down...")
	h.closing.Set(true)

	// 逐个关闭连接池中的活动连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		// 关闭客户端
		client.Close()
		return true
	})

	return nil
}
