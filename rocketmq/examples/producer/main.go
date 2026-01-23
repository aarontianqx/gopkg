package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/rocketmq"
)

func main() {
	common.InitLogger(
		logimpl.WithLevel(slog.LevelDebug),
		logimpl.WithAddSource(true),
		logimpl.WithOutput(os.Stdout),
		logimpl.WithFormat("text"),
	)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		common.Logger().Info("Received signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	// 定义参数
	topic := "marci_test_topic"
	accessKey := os.Getenv("ACCESS_KEY_ID")
	secretKey := os.Getenv("ACCESS_KEY_SECRET")

	// 创建生产者，使用简化的配置
	producer, err := rocketmq.RegisterProducer(ctx, rocketmq.ProducerConfig{
		// 必需的配置项
		Endpoint: "rmq-cn-to3445cxv07-vpc.cn-beijing.rmq.aliyuncs.com:8080", // RocketMQ 服务端点
		Topics:   []string{topic},                                           // 要生产的主题列表

		// 可选的认证配置
		AccessKey:    accessKey,
		AccessSecret: secretKey,
	})
	if err != nil {
		common.Logger().Error("Failed to register producer", "err", err)
		return
	}

	// 发送同步消息
	common.Logger().Info("Sending synchronous messages...")
	for i := 0; i < 100; i++ {
		msg := &rocketmq.Message{
			Topic: topic,
			Tag:   "test-tag",
			Keys:  []string{fmt.Sprintf("key-%d", i)},
			Body:  []byte(fmt.Sprintf("This is a synchronous message - %d", i)),
		}
		err = producer.Send(ctx, msg)
		if err != nil {
			common.Logger().Error("Failed to send synchronous message", "err", err)
		} else {
			common.Logger().Info("Successfully sent synchronous message", "key", msg.Keys[0])
		}
	}

	// 发送带 MessageGroup 的顺序消息（同一用户的消息会发到同一个 queue）
	common.Logger().Info("Sending ordered messages with MessageGroup...")
	for i := 0; i < 10; i++ {
		userID := fmt.Sprintf("user_%d", i%3) // 3 个用户
		msg := &rocketmq.Message{
			Topic:        topic,
			Tag:          "order-event",
			Keys:         []string{fmt.Sprintf("order-%d", i)},
			MessageGroup: userID, // 同一 userID 的消息会发到同一 queue
			Body:         []byte(fmt.Sprintf("Order event for %s - order %d", userID, i)),
		}
		err = producer.Send(ctx, msg)
		if err != nil {
			common.Logger().Error("Failed to send ordered message", "err", err)
		} else {
			common.Logger().Info("Successfully sent ordered message", "userID", userID, "orderID", i)
		}
	}

	// 发送异步消息
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		messageNum := i
		go func() {
			defer wg.Done()

			msg := &rocketmq.Message{
				Topic: topic,
				Tag:   "async-tag",
				Keys:  []string{fmt.Sprintf("async-key-%d", messageNum)},
				Body:  []byte(fmt.Sprintf("This is async message #%d", messageNum)),
			}

			common.Logger().Info("Sending asynchronous message", "number", messageNum)

			// 使用回调函数发送异步消息
			producer.SendAsync(ctx, msg,
				func(err error) {
					if err != nil {
						common.Logger().Error("Failed to send async message", "number", messageNum, "err", err)
						return
					}
					common.Logger().Info("Successfully delivered async message", "number", messageNum)
				})
		}()
	}

	// 发送延迟消息
	common.Logger().Info("Sending delayed message...")
	delayMsg := &rocketmq.Message{
		Topic: topic,
		Tag:   "delay-tag",
		Keys:  []string{"delay-key-1"},
		Body:  []byte("This is a message that will be delivered after 5 seconds"),
	}
	err = producer.SendDelay(ctx, delayMsg, 5*time.Second)
	if err != nil {
		common.Logger().Error("Failed to send delayed message", "err", err)
	} else {
		common.Logger().Info("Successfully sent delayed message", "key", delayMsg.Keys[0])
	}

	// 等待所有异步消息发送完成或者上下文被取消
	common.Logger().Info("Waiting for all messages to be sent...")
	go func() {
		wg.Wait()
		cancel()
	}()

	select {
	case <-ctx.Done():
		common.Logger().Info("All messages sent successfully")
		common.Logger().Info("Context cancelled before all messages could be sent")
	case <-time.After(10 * time.Second):
		common.Logger().Info("Timed out waiting for messages to be sent")
	}

	// 按照最佳实践：先取消上下文（已经在信号处理器中完成）
	// 然后等待所有生产者和消费者停止
	common.Logger().Info("Waiting for producers to stop...")
	rocketmq.WaitStop()
	common.Logger().Info("All producers stopped, exiting")
}
