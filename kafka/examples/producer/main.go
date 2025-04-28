package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/kafka"
)

func main() {
	common.Init(common.LogConfig{
		Level:     "debug",   // "debug", "info", "warn", "error"
		AddSource: false,     // Include source file and line numbers
		Output:    os.Stdout, // Output destination
		Format:    "json",    // "json" or "text"
	})

	// Create a context that can be cancelled
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

	// Register a producer with synchronous and asynchronous capabilities
	producer, err := kafka.RegisterProducer(ctx, kafka.ProducerConfig{
		BootstrapServers: "localhost:9092",
		RequiredAcks:     sarama.WaitForAll,        // Most reliable setting
		Compression:      sarama.CompressionSnappy, // Good balance of CPU/bandwidth
		MaxRetries:       5,
		RetryBackoff:     200 * time.Millisecond,
		EnableIdempotent: kafka.BoolPtr(true), // Prevents duplicate messages
		// Enable both sync and async producers
		EnableSyncProducer:  kafka.BoolPtr(true),
		EnableAsyncProducer: kafka.BoolPtr(true),
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	// No need to explicitly call Close, WaitStop will handle it

	// Define target topic
	topic := "feature_temp"

	// Demonstrate synchronous sending
	fmt.Println("Sending synchronous messages...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("sync-key-%d", i)
		err = producer.Send(ctx, topic, []byte(key), []byte(fmt.Sprintf("This is a synchronous message - %d", i)))
		if err != nil {
			log.Printf("Failed to send synchronous message: %v", err)
		} else {
			fmt.Println("Successfully sent synchronous message!")
		}
	}

	// Demonstrate asynchronous sending with callback
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		messageNum := i
		go func() {
			defer wg.Done()

			key := fmt.Sprintf("async-key-%d", messageNum)
			value := fmt.Sprintf("This is async message #%d", messageNum)

			fmt.Printf("Sending asynchronous message #%d...\n", messageNum)

			// Send message asynchronously with callback
			producer.SendAsync(ctx, topic, []byte(key), []byte(value),
				func(msg *sarama.ProducerMessage, err error) {
					if err != nil {
						log.Printf("Failed to send message #%d: %v", messageNum, err)
						return
					}
					fmt.Printf("Successfully delivered message #%d to topic: %s, partition: %d, offset: %d\n",
						messageNum, msg.Topic, msg.Partition, msg.Offset)
				})
		}()
	}

	// Wait for all async messages to be sent or until context is cancelled
	fmt.Println("Waiting for all messages to be sent...")
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		fmt.Println("All messages sent successfully")
	case <-ctx.Done():
		fmt.Println("Context cancelled before all messages could be sent")
	case <-time.After(10 * time.Second):
		fmt.Println("Timed out waiting for messages to be sent")
	}

	// Follow best practice: first cancel context (already done in signal handler)
	// then wait for all producers and consumers to stop
	fmt.Println("Waiting for producers to stop...")
	kafka.WaitStop()
	fmt.Println("All producers stopped, exiting")
}
