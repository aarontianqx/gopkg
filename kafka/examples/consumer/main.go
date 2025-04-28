package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/kafka"
)

// TestConsumer implements the kafka.Consumer interface
type TestConsumer struct {
	config kafka.ConsumerConfig
}

// GetConfig returns the consumer configuration
func (c *TestConsumer) GetConfig() *kafka.ConsumerConfig {
	return &c.config
}

// Handle processes a message from Kafka
func (c *TestConsumer) Handle(ctx context.Context, value []byte) error {
	log := common.LoggerCtx(ctx)
	log.Info("Received message", "message", string(value))
	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)
	return nil
}

func main() {
	common.Init(common.LogConfig{
		Level:     "debug",   // "debug", "info", "warn", "error"
		AddSource: false,     // Include source file and line numbers
		Output:    os.Stdout, // Output destination
		Format:    "json",    // "json" or "text"
	})
	// Create a cancelable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		fmt.Printf("Received signal: %v, initiating shutdown\n", sig)
		cancel()
	}()

	// Configure and register consumers
	consumer1 := &TestConsumer{
		config: kafka.ConsumerConfig{
			JobName:          "test-consumer-1",
			Topic:            "test_topic",
			BootstrapServers: "localhost:9092",
			ConsumerGroup:    "test_consumer_group-1",
			// Set to false to ensure offsets are committed only after successful processing (best practice)
			AutoCommit: false,
		},
	}

	consumer2 := &TestConsumer{
		config: kafka.ConsumerConfig{
			JobName:          "test-consumer-2",
			Topic:            "test_topic",
			BootstrapServers: "localhost:9092",
			ConsumerGroup:    "test_consumer_group-2",
			// Set to false to ensure offsets are committed only after successful processing (best practice)
			AutoCommit: false,
		},
	}

	// Register consumers
	kafka.RegisterConsumer(ctx, consumer1, true)
	kafka.RegisterConsumer(ctx, consumer2, true)

	// Start all consumers
	fmt.Println("Starting consumers...")
	kafka.StartAllConsumers(ctx)

	// Demonstrate disabling a consumer after a delay
	time.AfterFunc(10*time.Second, func() {
		fmt.Println("Disabling test-consumer-2...")
		kafka.SetConsumerSwitch("test-consumer-2", false)
	})

	// Wait for context cancellation (from signal handler)
	<-ctx.Done()
	fmt.Println("Context cancelled, waiting for consumers to stop...")

	// Wait for all consumers to finish
	kafka.WaitStop()
	fmt.Println("All consumers stopped, exiting")
}
