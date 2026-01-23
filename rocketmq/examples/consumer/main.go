package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/rocketmq"
)

// TestConsumer implements the rocketmq.IConsumer interface
type TestConsumer struct {
	config rocketmq.ConsumerConfig
}

// GetConfig returns the consumer configuration
func (c *TestConsumer) GetConfig() *rocketmq.ConsumerConfig {
	return &c.config
}

// Handle processes a message from RocketMQ.
// The Message contains all metadata for business-side routing.
func (c *TestConsumer) Handle(ctx context.Context, msg *rocketmq.Message) error {
	log := common.LoggerCtx(ctx)

	// Access message metadata for routing/filtering
	log.Info("Received message",
		"topic", msg.Topic,
		"tag", msg.Tag,
		"keys", msg.Keys,
		"messageGroup", msg.MessageGroup,
		"body", string(msg.Body),
	)

	// Example: Route based on Tag
	switch msg.Tag {
	case "order-event":
		return c.handleOrderEvent(ctx, msg)
	case "test-tag":
		return c.handleTestMessage(ctx, msg)
	default:
		log.Info("Processing generic message")
	}

	// Simulate some processing time
	time.Sleep(2 * time.Second)
	log.Info("Message processing completed")
	return nil
}

func (c *TestConsumer) handleOrderEvent(ctx context.Context, msg *rocketmq.Message) error {
	log := common.LoggerCtx(ctx)
	log.Info("Handling order event",
		"messageGroup", msg.MessageGroup, // This is the user ID for ordered messages
		"body", string(msg.Body),
	)
	return nil
}

func (c *TestConsumer) handleTestMessage(ctx context.Context, msg *rocketmq.Message) error {
	log := common.LoggerCtx(ctx)
	log.Info("Handling test message", "body", string(msg.Body))
	return nil
}

func main() {
	common.InitLogger(
		logimpl.WithLevel(slog.LevelDebug),
		logimpl.WithAddSource(true),
		logimpl.WithOutput(os.Stdout),
		logimpl.WithFormat("text"),
	)
	// Create a cancelable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		fmt.Printf("Received signal: %v, initiating shutdown\n", sig)
		cancel()
	}()

	// Configure and register consumers
	accessKey := os.Getenv("ACCESS_KEY_ID")
	secretKey := os.Getenv("ACCESS_KEY_SECRET")
	consumer1 := &TestConsumer{
		config: rocketmq.ConsumerConfig{
			JobName:       "test-consumer-1",
			Topic:         "marci_test_topic",
			Endpoint:      "rmq-cn-to3445cxv07-vpc.cn-beijing.rmq.aliyuncs.com:8080", // Change to your RocketMQ endpoint
			ConsumerGroup: "group_local_test_1",
			// Optional authentication
			AccessKey:         accessKey,
			AccessSecret:      secretKey,
			InvisibleDuration: 60 * time.Second,
			WorkerNum:         3, // Enable 3 concurrent workers for faster processing
		},
	}

	// Register consumer with auto-start
	rocketmq.RegisterConsumer(ctx, consumer1, true)

	sw := true

	// Start all registered consumers (only those with the switch enabled will actually start)
	rocketmq.StartAllConsumers(ctx)

	go func(ctx context.Context) {
		defer common.Recovery(ctx)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sw = !sw
				rocketmq.SetConsumerSwitch("test-consumer-1", sw)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	fmt.Println("Consumers started. Press Ctrl+C to exit.")

	// Wait for context cancellation
	<-ctx.Done()
	fmt.Println("Shutting down...")

	// Wait for all consumers to stop
	rocketmq.WaitStop()
	fmt.Println("All consumers stopped, exiting")
}
