// 2021.12.10
// redis RESCP 协议解析器

package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"ljr-redis/interface/redis"
	"ljr-redis/lib/logger"
	"ljr-redis/redis/reply"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

// 流式处理的接口适合提供给客户端 / 服务端使用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 解析一个 []byte 返回 redis.Reply
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)

	payload := <-ch
	if payload == nil {
		return nil, errors.New("no reply")
	}

	return payload.Data, payload.Err
}

// 解析数据
func parse0(reader io.Reader, ch chan<- *Payload) {
	// readingMutiline := false
	// expectedArgsCount := 0
	// var args [][]byte
	// var bulkLen int64
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	var state readState
	bufReader := bufio.NewReader(reader)
	var err error
	var msg []byte

	for {
		// 读取一行
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		// 读取错误
		if err != nil {
			// 处理错误
			if ioErr {
				// 读取错误
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}

			// 协议错误
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 单行： Status Int Error
		// 多行： BulkString MultiBulkStrings

		// 处理每一行的数据
		if !state.readingMutiline {
			if msg[0] == '*' {
				// 多行参数 MultiBulkStrings
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					// 重置读取状态
					state = readState{}
					continue
				}

				if state.expectedArgsCount == 0 {
					// 多行参数为空
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					// 重置读取状态
					state = readState{}
					continue
				}

			} else if msg[0] == '$' {
				// 单行参数
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					// 单行参数为空
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{}
					continue
				}

			} else {
				// 读取单行 Status Int Error
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}

		} else {
			// 读后续数据
			// BulkString MultiBulkStrings 除头部外的数据
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}

			// 如果 finished
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}

				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}

		}

	}

}

// 解析状态
type readState struct {
	// 是否读取多行
	readingMutiline bool
	// 参数个数 *
	expectedArgsCount int
	// 读取类型 */$
	msgType byte
	// 参数数据
	args [][]byte
	// 参数长度 $
	bulkLen int64
}

// 是否解析完毕
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 读取单行数据
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 {
		// 读取简单字符串 Simple String / Error / Integer
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 读取 Bulk String (Multi Bulk Strings)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}

		state.bulkLen = 0
	}

	return msg, false, nil
}

// 解析 Multi Bulk Strings 的头部: *2
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var (
		err          error
		expectedLine uint64
	)

	// 解析参数个数
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}

	if expectedLine == 0 {
		// 没有参数
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// 参数类型
		state.msgType = msg[0]
		// 需要读取多行
		state.readingMutiline = true
		// 参数个数 MultiBulkStrings 中 BulkString 的个数
		state.expectedArgsCount = int(expectedLine)
		// 参数存储空间
		state.args = make([][]byte, 0, expectedLine)

		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 解析 Bulk String 的头部: $3
func parseBulkHeader(msg []byte, state *readState) error {
	var err error

	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}

	if state.bulkLen == -1 {
		// null bulk 没有字符串
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMutiline = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}

}

// 解析单行数据
func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply

	switch msg[0] {
	case '+':
		// 简单字符串
		result = reply.MakeStatusReply(str[1:])

	case '-':
		// 错误
		result = reply.MakeErrReply(str[1:])

	case ':':
		// 整数
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)

	default:
		// 字符串 / 数组
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}

	return result, nil
}

// 读取 Multi Bulk Strings 除头部之外的数据
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// 读取字符串 Bulk 的长度
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 {
			// 没有 Bulk 在 Multi Bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}

	return nil
}
