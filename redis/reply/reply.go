// 2021.12.10
// reply 响应数据

package reply

import (
	"bytes"
	"strconv"

	"ljr-redis/interface/redis"
)

var (
	nullBulkReplyBytes = []byte("$-1")

	CRLF = "\r\n"
)

/* --------- 简单字符串 状态 OK --------- */
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// marshal ToBytes 安排 redis.Reply
func (r *StatusReply) ToBytes() []byte {
	// +OK\r\n
	return []byte("+" + r.Status + CRLF)
}

/* --------- 错误信息 Error --------- */
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// 服务器错误
type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func IsErrorReply(reply redis.Reply) bool {
	// 如果是错误 '-' 返回 true
	return reply.ToBytes()[0] == '-'
}

// marshal ToBytes 安排 redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	// -ERR Invalid Synatx\r\n
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

/* --------- 整数 Integer reply --------- */
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// marshal ToBytes 安排 redis.Reply
func (r *IntReply) ToBytes() []byte {
	// :12\r\n
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* --------- Multi Bulk 字符串数组 Reply --------- */
// 字符串数组
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// marshal ToBytes 安排 redis.Reply
func (r *MultiBulkReply) ToBytes() []byte {
	// *3/r/n
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}

	return buf.Bytes()
}

/* --------- Bulk String 字符串 Reply --------- */
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// marshal ToBytes 安排 redis.Reply
func (r *BulkReply) ToBytes() []byte {
	// $3/r/n
	// foo/r/n
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Multi Raw 复杂指令响应 Reply ---- */

// MultiRawReply store complex list structure, for example GeoPos command
type MultiRawReply struct {
	// 多个指令的响应
	Replies []redis.Reply
}

// MakeMultiRawReply creates MultiRawReply
func MakeMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	// *3
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	// 多态 ToBytes
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}
