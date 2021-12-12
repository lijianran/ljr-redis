// 2021.12.11
// redis connection

package connection

import (
	"net"
	"sync"
	"time"

	"ljr-redis/lib/sync/wait"
)

type Connection struct {
	conn         net.Conn          // tcp 连接
	waitingReply wait.Wait         // 等待直到服务器响应
	mu           sync.Mutex        // 服务器响应时上锁
	subs         map[string]bool   // 订阅
	password     string            // 密码
	multiState   bool              // 是否在执行复杂指令
	queue        [][][]byte        // 复杂指令队列
	watching     map[string]uint32 // watching
	selectedDB   int               // 选择的数据库
}

// 创建新连接
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// 关闭客户端连接
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// 远程客户端连接地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

/* ----------- 安排 interface/redis/conn.go 接口 ----------- */

// 通过 tcp 连接传递响应给客户端
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)

	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

// 设置 Auth 密码
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// 获取 Auth 密码
func (c *Connection) GetPassword() string {
	return c.password
}

/* ----------- 客户端订阅 ----------- */
// 订阅
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool, 0)
	}
	c.subs[channel] = true
}

// 取消订阅
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// 订阅个数
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// 获取所有订阅
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}

	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

/* ----------- 复杂指令 ----------- */

// 是否在执行复杂指令
func (c *Connection) InMultiState() bool {
	return c.multiState
}

// 设置是否在执行复杂指令
func (c *Connection) SetMultiState(state bool) {
	if !state {
		// 取消执行复杂指令需要清空数据
		c.watching = nil
		c.queue = nil
	}
	c.multiState = state
}

// 获取指令队列
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// 插入指令到队列
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// 清空指令队列
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// 获取 watching 列表
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32, 0)
	}
	return c.watching
}

/* ----------- 多数据库 ----------- */

// 获取当前数据库索引
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// 选择数据库
func (c *Connection) SelectDB(dbIndex int) {
	c.selectedDB = dbIndex
}
