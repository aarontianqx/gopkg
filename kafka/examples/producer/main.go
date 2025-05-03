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

	"github.com/IBM/sarama"
	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/kafka"
)

func main() {
	common.InitLogger(
		logimpl.WithLevel(slog.LevelDebug),
		logimpl.WithAddSource(true),
		logimpl.WithOutput(os.Stdout),
		logimpl.WithFormat("json"),
	)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := common.LoggerCtx(ctx)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("Received signal, initiating shutdown", "signal", sig)
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
		log.Error("Failed to create producer", "err", err)
		return
	}
	// No need to explicitly call Close, WaitStop will handle it

	// Define target topic
	topic := "feature_temp"

	// Demonstrate synchronous sending
	log.Info("Sending synchronous messages...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("sync-key-%d", i)
		err = producer.Send(ctx, topic, []byte(key), []byte(fmt.Sprintf("This is a synchronous message - %d", i)))
		if err != nil {
			log.Error("Failed to send synchronous message", "err", err)
		} else {
			log.Info("Successfully sent synchronous message!")
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

			log.Info("Sending asynchronous message", "messageNum", messageNum)

			// Send message asynchronously with callback
			producer.SendAsync(ctx, topic, []byte(key), []byte(value),
				func(msg *sarama.ProducerMessage, err error) {
					if err != nil {
						log.Error("Failed to send message", "messageNum", messageNum, "err", err)
						return
					}
					log.Info("Successfully delivered message #%d to topic: %s, partition: %d, offset: %d\n",
						messageNum, msg.Topic, msg.Partition, msg.Offset)
				})
		}()
	}

	// Wait for all async messages to be sent or until context is cancelled
	log.Info("Waiting for all messages to be sent...")
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		log.Info("All messages sent successfully")
	case <-ctx.Done():
		log.Info("Context cancelled before all messages could be sent")
	case <-time.After(10 * time.Second):
		log.Info("Timed out waiting for messages to be sent")
	}

	// Follow best practice: first cancel context (already done in signal handler)
	// then wait for all producers and consumers to stop
	log.Info("Waiting for producers to stop...")
	kafka.WaitStop()
	log.Info("All producers stopped, exiting")
}
