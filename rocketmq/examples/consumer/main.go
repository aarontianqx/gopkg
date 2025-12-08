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

// Handle processes a message from RocketMQ
func (c *TestConsumer) Handle(ctx context.Context, value []byte) error {
	log := common.LoggerCtx(ctx)
	log.Info("Received message", "message", string(value))
	// Simulate some processing time (increased to show concurrent processing benefits)
	time.Sleep(2 * time.Second)
	log.Info("Message processing completed", "message", string(value))
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
	//defer cancel()

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
			Topic:         "test_topic",
			Endpoint:      "rocketmq.example.org:8080", // Change to your RocketMQ endpoint
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
	//// Create another consumer that's initially disabled
	//consumer2 := &TestConsumer{
	//	config: rocketmq.ConsumerConfig{
	//		JobName:       "test-consumer-2",
	//		Topic:         "test_topic",
	//		Endpoint:      "rocketmq.example.org:8080", // Change to your RocketMQ endpoint
	//		ConsumerGroup: "group_local_test_2",
	//	},
	//}
	//
	//// Register consumer without auto-start
	//rocketmq.RegisterConsumer(ctx, consumer2, false)

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

	//After 5 seconds, enable the second consumer
	//time.AfterFunc(5*time.Second, func() {
	//	common.Logger().Info("Enabling second consumer")
	//	rocketmq.SetConsumerSwitch("test-consumer-2", true)
	//})

	// After 15 seconds, disable the first consumer
	//time.AfterFunc(15*time.Second, func() {
	//	common.Logger().Info("Disabling first consumer")
	//	rocketmq.SetConsumerSwitch("test-consumer-1", false)
	//})

	fmt.Println("Consumers started. Press Ctrl+C to exit.")

	// Wait for context cancellation
	<-ctx.Done()
	fmt.Println("Shutting down...")

	// Wait for all consumers to stop
	rocketmq.WaitStop()
	fmt.Println("All consumers stopped, exiting")
}
