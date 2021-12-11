// 2021.12.11
// 测试客户端

package client

import (
	"ljr-redis/redis/reply"
	"testing"
)

func TestClient(t *testing.T) {
	client, err := MakeClient("localhost:6399")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*reply.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}
}
