// 2021.12.11
// redis 客户端连接

package redis

// Connection 抽象单个对 redis 客户端的连接
type Connection interface {
	// 传递响应到客户端
	Write([]byte) error
	// 设置 Auth 密码
	SetPassword(string)
	// 获取 Auth 密码
	GetPassword() string

	/* client 客户端订阅 channels */
	// 订阅
	Subscribe(channel string)
	// 取消订阅
	UnSubscribe(channel string)
	// 订阅个数
	SubsCount() int
	// 获取所有订阅
	GetChannels() []string

	/* 复杂指令 */
	// 是否在执行复杂指令
	InMultiState() bool
	// 设置是否在执行复杂指令
	SetMultiState(bool)
	// 获取指令队列
	GetQueuedCmdLine() [][][]byte
	// 插入指令到队列
	EnqueueCmd([][]byte)
	// 清空指令队列
	ClearQueuedCmds()
	// 获取 watching 列表
	GetWatching() map[string]uint32

	/* 多数据库 */
	// 获取当前数据库索引
	GetDBIndex() int
	// 选择数据库
	SelectDB(int)
}
