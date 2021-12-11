// 2021.12.10

package main

import (
	"ljr-redis/lib/logger"
	"ljr-redis/redis/server"
	"ljr-redis/tcp"
)

var banner = `
.__       __                   .___.__        
|  |     |__|______   ____   __| _/|__| ______
|  |     |  \_  __ \_/ __ \ / __ | |  |/  ___/
|  |__   |  ||  | \/\  ___// /_/ | |  |\___ \ 
|____/\__|  ||__|    \___  >____ | |__/____  >
     \______|            \/     \/         \/
`

func main() {
	// echo
	// tcp.ListenAndServeWithSignal(&tcp.Config{
	// 	Address: "127.0.0.1:8000",
	// }, tcp.MakeEchoHandler())

	// redis
	print(banner)
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: "127.0.0.1:6399",
	}, server.MakeRedisHandler())
	if err != nil {
		logger.Error(err)
	}
}
