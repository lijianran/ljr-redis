// 2021.12.11
// redis client

package client

import (
	"net"
	"runtime/debug"
	"sync"
	"time"

	"ljr-redis/interface/redis"
	"ljr-redis/lib/logger"
	"ljr-redis/lib/sync/wait"
	"ljr-redis/redis/parser"
	"ljr-redis/redis/reply"
)

type Client struct {
	conn        net.Conn      // 服务端的 tcp 连接
	pendingReqs chan *request // 等待发送
	waitingReqs chan *request // 等待响应
	ticker      *time.Ticker  // 心跳计时器
	addr        string

	working *sync.WaitGroup // 完成所有的请求
}

type request struct {
	id        uint64      // 请求 id
	args      [][]byte    // 上行参数
	reply     redis.Reply // 服务器响应
	heartbeat bool        // 是否为心跳请求
	waiting   *wait.Wait  // 等待异步处理
	err       error       // 错误消息
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// 客户端构造器
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	// ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},
	}, nil
}

// 启动客户端
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	// 发送请求
	go client.handleWrite()
	// 读取响应
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
	// 心跳
	go client.heartbeat()
}

// 关闭客户端
func (client *Client) Close() {
	client.ticker.Stop()

	// 停止新的请求
	close(client.pendingReqs)

	// 等待处理中的请求完成
	client.working.Wait()

	// 关闭连接 释放资源
	_ = client.conn.Close()
	close(client.waitingReqs)
}

// 定时发送心跳
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

// 发送心跳请求
func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)

	defer client.working.Done()

	// 发送心跳请求
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

// 发送请求到 redis 服务器
func (client *Client) Send(args [][]byte) redis.Reply {
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)

	defer client.working.Done()

	// 等待发送 入队列
	client.pendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		// 请求超时
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		// 请求失败
		return reply.MakeErrReply("request failed")
	}
	return request.reply
}

// 处理错误请求
func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}

	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}

	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()

	return nil
}

// 写协程入口
func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// 发送请求到 redis 服务器
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	// 序列化请求
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	// 写数据 发送请求
	_, err := client.conn.Write(bytes)
	i := 0
	// 三次重新请求
	for err != nil && i < 3 {
		// 有错误
		err = client.handleConnectionError(err)
		if err != nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}

	if err == nil {
		// 发送请求成功，等待 redis 服务器响应
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

// 读协程入口
func (client *Client) handleRead() error {
	// 解析协议
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			client.finishRequest(reply.MakeErrReply(payload.Err.Error()))
			continue
		}

		client.finishRequest(payload.Data)
	}
	return nil
}

// 收到 redis 服务器的响应 完成请求
func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	request := <-client.waitingReqs
	if request == nil {
		return
	}

	// 响应数据
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}
