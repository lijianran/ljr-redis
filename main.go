// 2021.12.10

package main

import (
	"ljr-redis/tcp"
)

func main() {

	tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: "127.0.0.1:8000",
	}, tcp.MakeEchoHandler())
}
