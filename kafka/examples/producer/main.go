package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/kafka"
)

var BROKERS = strings.Split("alikafka-pre-cn-em942wkwo001-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-em942wkwo001-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-em942wkwo001-3-vpc.alikafka.aliyuncs.com:9092", ",")

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

	// Register a producer with synchronous and asynchronous capabilities
	producer, err := kafka.RegisterProducer(ctx, kafka.ProducerConfig{
		Brokers:          BROKERS,
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
		common.Logger().Error("Failed to register producer", "err", err)
	}
	// No need to explicitly call Close, WaitStop will handle it

	// Define target topic
	topic := "feature_temp"

	// Demonstrate synchronous sending
	common.Logger().Info("Sending synchronous message...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("sync-key-%d", i)
		err = producer.Send(ctx, topic, []byte(key), []byte(fmt.Sprintf("This is a synchronous message - %d", i)))
		if err != nil {
			common.Logger().Error("Failed to send synchronous message", "err", err)
		} else {
			common.Logger().Info("Successfully sent synchronous message!")
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

			common.Logger().Info("Sending asynchronous message", "number", messageNum)

			// Send message asynchronously with callback
			producer.SendAsync(ctx, topic, []byte(key), []byte(value),
				func(msg *sarama.ProducerMessage, err error) {
					if err != nil {
						common.Logger().Error("Failed to send message", "number", messageNum, "err", err)
						return
					}
					common.Logger().Info("Successfully delivered message",
						"number", messageNum,
						"topic", msg.Topic,
						"partition", msg.Partition,
						"offset", msg.Offset)
				})
		}()
	}

	// Wait for all async messages to be sent or until context is cancelled
	common.Logger().Info("Waiting for all messages to be sent...")
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		common.Logger().Info("All messages sent successfully")
	case <-ctx.Done():
		common.Logger().Info("Context cancelled before all messages could be sent")
	case <-time.After(10 * time.Second):
		common.Logger().Info("Timed out waiting for messages to be sent")
	}

	// Follow best practice: first cancel context (already done in signal handler)
	// then wait for all producers and consumers to stop
	common.Logger().Info("Waiting for producers to stop...")
	kafka.WaitStop()
	common.Logger().Info("All producers stopped, exiting")
}
