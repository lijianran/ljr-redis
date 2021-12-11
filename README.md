# ljr-redis

Go 实现的简易版 Redis

# 目标

1. 熟悉 Golang 语言
2. 了解 Redis

## 开发日志

### 2021.12.11

1. 实现并发安全的哈希表 ConcurrentMap
2. 实现部分单机事务
3. 实现数据库引擎和 DB-Server
4. 初步实现 ljredis 服务器
5. 初步实现 ljredis 客户端

### 2021.12.10

1. 实现简单的 Tcp 服务器，实现 Echo 功能
2. 实现 Redis 协议解析器