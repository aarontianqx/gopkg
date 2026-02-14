package rocketmq

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// HandlerFunc processes messages from RocketMQ.
type HandlerFunc func(ctx context.Context, msg *Message) error

// IConsumer defines the interface for RocketMQ message consumers
type IConsumer interface {
	GetConfig() *ConsumerConfig
	Handle(ctx context.Context, msg *Message) error
}

// consumerProxy handles the lifecycle of a RocketMQ consumer
type consumerProxy struct {
	jobName           string
	topic             string
	filterExpression  *rmq_client.FilterExpression
	maxMessageNum     int32
	invisibleDuration time.Duration
	config            *rmq_client.Config
	client            rmq_client.SimpleConsumer
	handler           HandlerFunc
	started           bool           // Tracks if the consumer is started
	mu                sync.Mutex     // Protects started field
	workerNum         int            // Number of concurrent workers
	workerSemaphore   chan struct{}  // Controls number of concurrent workers
	workerWg          sync.WaitGroup // Tracks workers for this consumer
}

// StartDaemon runs the consumer event loop in a goroutine
func (proxy *consumerProxy) StartDaemon(ctx context.Context) {
	jobCtx := logimpl.ContextWithBaseLogInfo(ctx, &logimpl.BaseLogInfo{
		RequestID:     common.GenLogID(), // Set a unique request ID for the daemon goroutine to track its lifecycle
		JobName:       proxy.jobName,
		Topic:         proxy.topic,
		ConsumerGroup: proxy.config.ConsumerGroup,
	})
	log := common.LoggerCtx(jobCtx)

	var switchConsume bool
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	log.Info("Consumer daemon started")

	for {
		select {
		case <-ticker.C:
			// Check if switch state has changed
			if s := GetConsumerSwitch(proxy.jobName); s != switchConsume {
				switchConsume = s
				if switchConsume {
					log.Info("Consumer switch enabled, starting consumer.")
					proxy.Start(jobCtx)
				} else {
					log.Info("Consumer switch disabled, waiting for message processing to complete...")
					proxy.workerWg.Wait()
					log.Info("Message processing completed, stopping client.")
					proxy.Stop(jobCtx)
				}
			}

		case <-ctx.Done():
			log.Info("Context cancelled, waiting for message processing to complete...")
			proxy.workerWg.Wait() // Wait for all workers to finish
			log.Info("Message processing completed, stopping client.")
			proxy.Stop(jobCtx)
			return
		}
	}
}

// Start initializes and starts the consumer client, subscribing to its configured topic.
func (proxy *consumerProxy) Start(ctx context.Context) {
	log := common.LoggerCtx(ctx)

	proxy.mu.Lock()
	defer proxy.mu.Unlock()

	// Check if consumer is already started
	if proxy.started {
		log.Debug("Consumer client already running, no action taken.")
		return
	}

	log.Info("Starting consumer client.")

	// Create SimpleConsumer instance without starting it
	client, err := rmq_client.NewSimpleConsumer(
		proxy.config,
		rmq_client.WithSimpleAwaitDuration(30*time.Second),
		rmq_client.WithSimpleSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			proxy.topic: proxy.filterExpression,
		}),
	)
	if err != nil {
		log.Error("Failed to create RocketMQ SimpleConsumer", "error", err)
		return
	}
	log.Info("RocketMQ SimpleConsumer instance created successfully")

	err = client.Start()
	if err != nil {
		log.Error("Start consumer client failed.", "error", err)
		return
	}

	proxy.client = client
	proxy.started = true
	globalWg.Add(1)
	log.Info("Consumer client started and subscribed successfully, launching message processor.")
	go proxy.processMessages(ctx, client)
}

// isStarted checks if the consumer is already started
func (proxy *consumerProxy) isStarted() bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return proxy.started
}

// processMessages continuously receives and processes messages from the subscribed topic.
func (proxy *consumerProxy) processMessages(ctx context.Context, client rmq_client.SimpleConsumer) {
	defer globalWg.Done()
	defer proxy.workerWg.Wait()
	defer common.Recovery(ctx)
	log := common.LoggerCtx(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("Context cancelled, stopping message processor.")
			return
		default:
			// Check consumer state and shutdown signal before receiving messages
			if !GetConsumerSwitch(proxy.jobName) || !proxy.isStarted() {
				log.Info("Consumer switch disabled or consumer stopped, stopping message processor.")
				return
			}

			// Calculate how many messages to receive based on available workers
			availableWorkers := cap(proxy.workerSemaphore) - len(proxy.workerSemaphore)
			if availableWorkers == 0 {
				// No available workers, wait a bit before checking again
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Limit maxMessageNum to available workers to avoid over-pulling
			receiveNum := min(proxy.maxMessageNum, int32(availableWorkers))

			// Receive messages from broker
			msgs, err := client.Receive(ctx, receiveNum, proxy.invisibleDuration)
			if err != nil {
				if errRpc, ok := rmq_client.AsErrRpcStatus(err); ok && errRpc.GetCode() == int32(v2.Code_MESSAGE_NOT_FOUND) {
					// no new message, yield briefly
					time.Sleep(100 * time.Millisecond)
					continue
				} else if strings.Contains(err.Error(), "CODE=DEADLINE_EXCEEDED") {
					// Long polling timeout, retry immediately
					continue
				} else {
					log.Warn("Error receiving messages, will retry", "error", err)
					time.Sleep(3 * time.Second)
					continue
				}
			}

			log.Debug("Received messages.", "count", len(msgs))

			// Process messages concurrently using worker pool
			for _, msg := range msgs {
				// Acquire worker slot (blocks if no workers available)
				proxy.workerSemaphore <- struct{}{}

				// Process message in a separate goroutine
				proxy.workerWg.Add(1)
				go func(messageView *rmq_client.MessageView) {
					defer proxy.workerWg.Done()
					defer func() { <-proxy.workerSemaphore }() // Release worker slot
					proxy.consumeSingleAndAck(client, messageView)
				}(msg)
			}
		}
	}
}

func (proxy *consumerProxy) consumeSingleAndAck(client rmq_client.SimpleConsumer, mv *rmq_client.MessageView) {
	ctx := buildConsumeContext(proxy, mv)
	defer common.Recovery(ctx)

	log := common.LoggerCtx(ctx)

	// Convert SDK MessageView to our unified Message type
	msg := fromMessageView(mv)

	log.Info("Handling message.", "tag", msg.Tag, "keys", msg.Keys, "messageGroup", msg.MessageGroup)
	if err := proxy.handler(ctx, msg); err != nil {
		log.Error("failed to handle message.", "error", err)
		return
	}

	if err := client.Ack(ctx, mv); err != nil {
		log.Error("failed to ack message", "error", err)
	}

	log.Info("Message processed and Acked successfully.")
}

// Stop gracefully stops the consumer client.
// This involves stopping the underlying RocketMQ client.
func (proxy *consumerProxy) Stop(ctx context.Context) {
	log := common.LoggerCtx(ctx)

	proxy.mu.Lock()
	defer proxy.mu.Unlock()

	if proxy.started && proxy.client != nil {
		log.Info("Stopping consumer client.")
		if err := proxy.client.GracefulStop(); err != nil {
			log.Error("Error stopping consumer client.", "error", err)
		} else {
			log.Info("Consumer client stopped successfully.")
		}
		proxy.started = false
		proxy.client = nil
	} else {
		log.Debug("Consumer client already stopped or not initialized.")
	}
}
