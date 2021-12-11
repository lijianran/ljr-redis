// 2021.12.11
// 测试协议解析器

package parser

import (
	"bytes"
	"io"
	"testing"

	"ljr-redis/interface/redis"
	"ljr-redis/lib/byteutil"
	"ljr-redis/redis/reply"
)

func TestParseStream(t *testing.T) {
	// 序列化
	replies := []redis.Reply{
		reply.MakeIntReply(1),
		reply.MakeStatusReply("OK"),
		reply.MakeErrReply("ERR unknown"),
		reply.MakeBulkReply([]byte("a\r\nb")), // 测试二进制安全性
		reply.MakeNullBulkReply(),
		reply.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		reply.MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + reply.CRLF)) // 测试文本协议

	// 预期结果
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, reply.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	// 测试协议解析功能
	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		// 错误
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}

		// 数据为空
		if payload.Data == nil {
			t.Error("empty data")
			return
		}

		// 判断是否符合预期数据
		exp := expected[i]
		i++
		if !byteutil.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			// 解析失败
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}
