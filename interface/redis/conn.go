// 2021.12.11
// redis 客户端连接

package redis

// Connection 抽象单个对 redis 客户端的连接
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// client 客户端订阅 channels
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
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

	// used for multi database
	GetDBIndex() int
	SelectDB(int)
}
