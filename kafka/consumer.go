package kafka

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"

	"github.com/aarontianqx/gopkg/common"
)

// HandlerFunc processes message values from Kafka
type HandlerFunc func(ctx context.Context, value []byte) error

// IConsumer defines the interface for Kafka message consumers
type IConsumer interface {
	GetConfig() *ConsumerConfig
	Handle(ctx context.Context, value []byte) error
}

// consumerProxy handles the lifecycle of a Kafka consumer
type consumerProxy struct {
	jobName       string
	brokers       []string
	topics        []string
	consumerGroup string
	saramaConfig  *sarama.Config
	client        sarama.ConsumerGroup
	handler       HandlerFunc
}

// StartDaemon runs the consumer event loop in a goroutine
func (proxy *consumerProxy) StartDaemon(ctx context.Context) {
	jobCtx := common.ContextWithBaseLogInfo(ctx, &common.BaseLogInfo{
		JobName: proxy.jobName,
	})
	defer common.Recovery(jobCtx)
	log := common.LoggerCtx(jobCtx)

	var (
		switchConsume        bool
		errChan              chan error
		backoffTime          time.Duration = 1 * time.Second
		maxBackoff           time.Duration = 60 * time.Second // Maximum backoff time: 60 seconds
		consecutiveErrors    int           = 0
		maxConsecutiveErrors int           = 10 // Maximum consecutive errors before giving up
	)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Check consumer switch state
			if s := GetConsumerSwitch(proxy.jobName); s != switchConsume {
				switchConsume = s
				if switchConsume {
					log.Info("consumer switch on, start consumer")
					errChan = proxy.Start(jobCtx)
					// Reset backoff time and consecutive error count
					backoffTime = 1 * time.Second
					consecutiveErrors = 0
				} else {
					errChan = nil
					log.Info("consumer switch off, try to stop consumer")
					proxy.Stop(jobCtx)
				}
			}
		case err, ok := <-errChan:
			// Channel was closed or nil
			if !ok || errChan == nil {
				continue
			}

			// Only count as error if not nil
			if err != nil {
				consecutiveErrors++

				if consecutiveErrors > maxConsecutiveErrors {
					log.Error("too many consecutive errors, stopping retry",
						"consecutive_errors", consecutiveErrors,
						"max_errors", maxConsecutiveErrors,
						"job_name", proxy.jobName,
						"last_error", err)

					// Disable consumer, requires user intervention to re-enable
					SetConsumerSwitch(proxy.jobName, false)
					switchConsume = false
					errChan = nil
					proxy.Stop(jobCtx)
					continue
				}

				log.Error("consume message failed, retrying with backoff",
					"err", err,
					"backoff_seconds", backoffTime.Seconds(),
					"consecutive_errors", consecutiveErrors,
					"job_name", proxy.jobName)

				// Stop current consumer
				proxy.Stop(jobCtx)

				// Use exponential backoff strategy
				time.Sleep(backoffTime)

				// Try to restart consumer
				errChan = proxy.Start(jobCtx)

				// Exponentially increase backoff time, but not exceeding the maximum
				backoffTime = time.Duration(math.Min(
					float64(backoffTime*2),
					float64(maxBackoff),
				))
			}

		case <-ctx.Done():
			log.Debug("context canceled, stopping kafka consumer")
			proxy.Stop(jobCtx)
			return
		}
	}
}

// Setup implements sarama.ConsumerGroupHandler
func (proxy *consumerProxy) Setup(session sarama.ConsumerGroupSession) error {
	common.Logger().Debug("consumer setup complete")
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler
func (proxy *consumerProxy) Cleanup(session sarama.ConsumerGroupSession) error {
	common.Logger().Debug("consumer cleanup complete")
	return nil
}

// ConsumeClaim implements the sarama.ConsumerGroupHandler interface
func (proxy *consumerProxy) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	common.Logger().Debug("new consumer claim",
		"generation_id", session.GenerationID(), "topic", claim.Topic(),
		"partition", claim.Partition(), "initial_offset", claim.InitialOffset(),
		"job_name", proxy.jobName, "group", proxy.consumerGroup)

	// Track message count for batch commits
	var messageCount int32

	// Atomic flag to track whether there are any uncommitted messages
	var hasUncommittedMessages int32

	// Create done channel for handling exit logic
	done := make(chan struct{})

	// Only start periodic commit goroutine if AutoCommit is disabled
	if !proxy.saramaConfig.Consumer.Offsets.AutoCommit.Enable {
		go func() {
			commitTicker := time.NewTicker(5 * time.Second)
			defer commitTicker.Stop()

			for {
				select {
				case <-commitTicker.C:
					// Only commit if there are uncommitted messages
					if atomic.LoadInt32(&hasUncommittedMessages) == 1 {
						session.Commit()
						atomic.StoreInt32(&hasUncommittedMessages, 0)
						atomic.StoreInt32(&messageCount, 0)
						common.Logger().Debug("periodic commit executed",
							"job_name", proxy.jobName, "group", proxy.consumerGroup)
					}
				case <-done:
					return
				case <-session.Context().Done():
					return
				}
			}
		}()
	}

	// Message processing loop
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// Channel is closed
				close(done)
				return nil
			}

			err := proxy.ConsumeSingle(session, msg)
			if err == nil && !proxy.saramaConfig.Consumer.Offsets.AutoCommit.Enable {
				// Mark message only when processing succeeds
				session.MarkMessage(msg, "")

				// Set uncommitted flag
				atomic.StoreInt32(&hasUncommittedMessages, 1)

				// Increment message count atomically
				newCount := atomic.AddInt32(&messageCount, 1)

				// Execute a commit if enough messages have accumulated
				if newCount >= 100 {
					session.Commit()
					atomic.StoreInt32(&messageCount, 0)
					atomic.StoreInt32(&hasUncommittedMessages, 0)
					common.Logger().Debug("batch commit executed",
						"message_count", 100,
						"job_name", proxy.jobName, "group", proxy.consumerGroup)
				}
			}
		case <-session.Context().Done():
			// Handle session context completion - rebalance or shutdown
			common.Logger().Debug("session context done, exiting consumption loop",
				"job_name", proxy.jobName, "group", proxy.consumerGroup)
			close(done)

			// Final commit of any remaining marked messages
			if !proxy.saramaConfig.Consumer.Offsets.AutoCommit.Enable && atomic.LoadInt32(&hasUncommittedMessages) == 1 {
				session.Commit()
				common.Logger().Debug("final commit executed before exit",
					"job_name", proxy.jobName, "group", proxy.consumerGroup)
			}
			return nil
		}
	}
}

// ConsumeSingle processes a single Kafka message
func (proxy *consumerProxy) ConsumeSingle(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	// Create a context with job information
	ctx := common.ContextWithBaseLogInfo(context.Background(), &common.BaseLogInfo{
		RequestID: common.GenLogID(),
		JobName:   proxy.jobName,
	})

	defer common.Recovery(ctx)
	log := common.LoggerCtx(ctx)
	log.Debug("message received from kafka",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset)

	// Extract and pass only the message value to handler
	err := proxy.handler(ctx, msg.Value)
	if err != nil {
		log.Error("kafka message handling failed",
			"err", err,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"value_size", len(msg.Value))
	}
	return err
}

// Start begins consuming messages from Kafka
func (proxy *consumerProxy) Start(ctx context.Context) chan error {
	log := common.LoggerCtx(ctx)
	errC := make(chan error, 1) // Buffered channel to prevent goroutine leaks

	// init client
	client, err := sarama.NewConsumerGroup(proxy.brokers, proxy.consumerGroup, proxy.saramaConfig)
	if err != nil {
		errC <- fmt.Errorf("error creating consumer group client: %w", err)
		return errC
	}

	proxy.client = client
	globalWg.Add(1)

	// consume errors from Kafka broker
	go func() {
		for err := range client.Errors() {
			log.Error("kafka broker error", "err", err)
			// Forward broker errors to the main error channel if they represent a critical failure
			if err != nil {
				select {
				case errC <- fmt.Errorf("kafka broker error: %w", err):
				default:
					// Channel is full or closed, just log the error
					log.Error("error channel full", "err", err)
				}
			}
		}
	}()

	// Start the consumption loop in a separate goroutine
	go func() {
		defer common.Recovery(ctx)

		// Loop to handle rebalancing - Consume() returns when rebalancing happens or context is cancelled
		for {
			// Check if the context is done or client has been closed
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			if proxy.client == nil {
				// Client has been closed
				return
			}

			// This will return when rebalancing or context is canceled
			err := client.Consume(ctx, proxy.topics, proxy)

			// Normal rebalancing case - Consume returns nil when rebalancing happens
			if err == nil {
				log.Debug("consumer rebalancing, resuming consumption",
					"group", proxy.consumerGroup)
				continue
			}

			// If we received a real error, send it to the error channel
			errC <- err
			return
		}
	}()

	return errC
}

// Stop gracefully shuts down the consumer
func (proxy *consumerProxy) Stop(ctx context.Context) {
	if proxy.client == nil {
		return
	}

	log := common.LoggerCtx(ctx)
	client := proxy.client
	proxy.client = nil // Set to nil first to signal to all goroutines to stop

	// Close the client
	if err := client.Close(); err != nil {
		log.Error("failed to stop consumer", "err", err)
	} else {
		log.Debug("consumer stopped")
	}

	globalWg.Done()
}
