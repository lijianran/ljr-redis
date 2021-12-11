// 2021.12.10
// Echo 服务器

package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"ljr-redis/lib/logger"
)

// func ListenAndServe(address string) {
// 	// 绑定监听地址
// 	listener, err := net.Listen("tcp", address)
// 	if err != nil {
// 		log.Fatal(fmt.Sprintf("listen error: %v", err))
// 	}

// 	// 错误处理 error handling
// 	defer listener.Close()

// 	log.Println(fmt.Sprintf("bind: %s, start listening...", address))

// 	for {
// 		// Accept 阻塞
// 		// 直到有新的连接建立 或者 listen 中断才会返回
// 		connection, err := listener.Accept()
// 		if err != nil {
// 			// 通常错误是由于 listener 被关闭导致无法继续监听导致的错误
// 			log.Fatal(fmt.Sprintf("accept error: %v", err))
// 		}

// 		// 处理
// 		go Handle(connection)
// 	}
// }

// func Handle(connection net.Conn) {
// 	// bufio 标准库
// 	reader := bufio.NewReader(connection)

// 	for {
// 		// 遇到 '\n' 之前一直阻塞
// 		msg, err := reader.ReadString('\n')
// 		if err != nil {
// 			// 连接中断或者读取完毕
// 			if err == io.EOF {
// 				log.Println("connection close")
// 			} else {
// 				log.Println(err)
// 			}
// 			return
// 		}

// 		b := []byte(msg)
// 		connection.Write(b)
// 	}
// }

// func main() {
// 	ListenAndServe(":8000")
// }

// 抽象应用层服务器
type Handler interface {
	// 虚函数
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

// 抽象配置
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServeWithSignal(cfg *Config, handler Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh

		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// 开始监听
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// 监听并提供服务，收到 closeChan 的消息后关闭
func ListenAndServe(listener net.Listener, handler Handler, closeChan <-chan struct{}) {
	// 监听关闭消息
	go func() {
		<-closeChan
		logger.Info("shutting down...")

		_ = listener.Close()
		_ = handler.Close()
	}()

	// 错误处理 error handling
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}

		logger.Info("accept link")

		// 开启 goroutine
		waitDone.Add(1)
		go func() {
			defer func() {
				// 出错关闭协程
				waitDone.Done()
			}()
			// 处理连接
			handler.Handle(ctx, conn)
		}()

	}

	// 等待所有协程结束
	waitDone.Wait()

}
