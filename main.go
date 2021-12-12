// 2021.12.10

package main

import (
	"fmt"
	"os"

	"ljr-redis/config"
	"ljr-redis/lib/logger"
	RedisServer "ljr-redis/redis/server"
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

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFileName: "",
	MaxClients:     1000,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	// echo
	// tcp.ListenAndServeWithSignal(&tcp.Config{
	// 	Address: "127.0.0.1:8000",
	// }, tcp.MakeEchoHandler())

	// redis
	print(banner)

	configFileName := os.Getenv("CONFIG")
	if configFileName == "" {
		if fileExists("redis.conf") {
			// 读取配置文件加载
			config.SetupConfig("redis.config")
		} else {
			// 加载默认配置
			config.Properties = defaultProperties
		}
	} else {
		// 从 CONFIG 环境变量中读取配置文件路径并加载
		config.SetupConfig(configFileName)
	}

	// 启动服务器
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeRedisHandler())

	if err != nil {
		logger.Error(err)
	}
}
