// 2021.12.10
// reply 常量

package reply

/* ------------ Multi Bulk Strings 为空 *0 ------------ */
var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct{}

// marshal 安排 redis.Reply ToBytes
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

/* ------------ Bulk String 为空 $0 ------------ */
var nullBulkBytes = []byte("$-1\r\n")

type NullBulkReply struct{}

// marshal 安排 redis.Reply ToBytes
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

/* ------------ QueuedReply is +QUEUED ------------ */
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// marshal 安排 redis.Reply ToBytes
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

var theQueuedReply = new(QueuedReply)

func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}

/* ------------ OkReply is +OK ------------ */
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

// marshal 安排 redis.Reply ToBytes
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply {
	return theOkReply
}
