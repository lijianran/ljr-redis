package redis

// RESP 协议消息接口
type Reply interface {
	ToBytes() []byte
}
