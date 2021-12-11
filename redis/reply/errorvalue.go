// 2021.12.11
// reply 错误

package reply

/* --------- 未知错误 --------- */
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

// marshals 安排 redis.Reply ToBytes
func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func (r *UnknownErrReply) Error() string {
	return "Err unknown"
}

/* --------- 指令参数个数错误 --------- */
type ArgNumErrReply struct {
	Cmd string
}

// marshals 安排 redis.Reply ToBytes
func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}
