package rocketmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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
	handler           HandlerFunc
	mu                sync.Mutex // Protects session field
	workerNum         int        // Number of concurrent workers
	prefetchSize      int        // Buffered prefetch queue size
	session           *consumerSession
	sessionSeq        atomic.Uint64
}

type consumerSession struct {
	client rmq_client.SimpleConsumer
	cancel context.CancelFunc
	done   chan struct{}
	seq    uint64
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
					proxy.startSession(jobCtx)
				} else {
					log.Info("Consumer switch disabled, stopping consumer session.")
					proxy.stopSession(jobCtx)
				}
			}

			if switchConsume && !proxy.isStarted() {
				log.Info("Consumer switch is enabled, trying to recover consumer session.")
				proxy.startSession(jobCtx)
			}

		case <-ctx.Done():
			log.Info("Context cancelled, stopping consumer session.")
			proxy.stopSession(jobCtx)
			return
		}
	}
}

// startSession initializes and starts a full consumer session.
func (proxy *consumerProxy) startSession(ctx context.Context) {
	daemonLog := common.LoggerCtx(ctx)

	proxy.mu.Lock()
	if proxy.session != nil {
		proxy.mu.Unlock()
		daemonLog.Debug("Consumer client already running, no action taken.")
		return
	}
	sessionSeq := proxy.sessionSeq.Add(1)
	sessionCtx := logimpl.ContextWithBaseLogInfo(ctx, &logimpl.BaseLogInfo{
		RequestID:     common.GenLogID(),
		JobName:       proxy.jobName,
		Topic:         proxy.topic,
		ConsumerGroup: proxy.config.ConsumerGroup,
	})
	sessionCtx, cancel := context.WithCancel(sessionCtx)
	proxy.mu.Unlock()
	log := common.LoggerCtx(sessionCtx)

	log.Info("Starting consumer client.", "sessionSeq", sessionSeq)

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
		cancel()
		return
	}
	log.Info("RocketMQ SimpleConsumer instance created successfully")

	err = client.Start()
	if err != nil {
		log.Error("Start consumer client failed.", "error", err)
		cancel()
		return
	}

	session := &consumerSession{
		client: client,
		cancel: cancel,
		done:   make(chan struct{}),
		seq:    sessionSeq,
	}

	proxy.mu.Lock()
	if proxy.session != nil {
		proxy.mu.Unlock()
		daemonLog.Warn("Consumer session already exists, stopping duplicated client.", "sessionSeq", sessionSeq)
		cancel()
		if stopErr := client.GracefulStop(); stopErr != nil {
			daemonLog.Error("Failed to stop duplicated consumer client", "error", stopErr, "sessionSeq", sessionSeq)
		}
		return
	}
	proxy.session = session
	proxy.mu.Unlock()

	globalWg.Add(1)
	log.Info("Consumer client started and subscribed successfully, launching message processor.", "sessionSeq", sessionSeq)
	go proxy.runSession(sessionCtx, session)
}

// isStarted checks if the consumer is already started
func (proxy *consumerProxy) isStarted() bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return proxy.session != nil
}

func (proxy *consumerProxy) runSession(ctx context.Context, session *consumerSession) {
	defer globalWg.Done()
	defer common.Recovery(ctx)
	log := common.LoggerCtx(ctx)
	defer close(session.done)
	log.Info("Consumer session pipeline started.", "sessionSeq", session.seq)

	prefetchCh := make(chan *rmq_client.MessageView, proxy.prefetchSize)

	var pipelineWg sync.WaitGroup

	pipelineWg.Add(1)
	go func() {
		defer pipelineWg.Done()
		proxy.fetchLoop(ctx, session.client, prefetchCh)
	}()

	for workerID := 0; workerID < max(proxy.workerNum, 1); workerID++ {
		pipelineWg.Add(1)
		go func() {
			defer pipelineWg.Done()
			proxy.workerLoop(session.client, prefetchCh)
		}()
	}

	pipelineWg.Wait()

	if err := session.client.GracefulStop(); err != nil {
		log.Error("Error stopping consumer client.", "error", err, "sessionSeq", session.seq)
	} else {
		log.Info("Consumer client stopped successfully.", "sessionSeq", session.seq)
	}

	proxy.mu.Lock()
	if proxy.session == session {
		proxy.session = nil
	}
	proxy.mu.Unlock()
}

func (proxy *consumerProxy) fetchLoop(ctx context.Context, client rmq_client.SimpleConsumer, prefetchCh chan *rmq_client.MessageView) {
	defer close(prefetchCh)
	log := common.LoggerCtx(ctx)

	for {
		receiveNum, ok := proxy.nextReceiveNum(ctx, prefetchCh)
		if !ok {
			return
		}

		msgs, err := client.Receive(ctx, receiveNum, proxy.invisibleDuration)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}

			if errRpc, ok := rmq_client.AsErrRpcStatus(err); ok && errRpc.GetCode() == int32(v2.Code_MESSAGE_NOT_FOUND) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}

			if strings.Contains(err.Error(), "CODE=DEADLINE_EXCEEDED") {
				continue
			}

			log.Warn("Error receiving messages, will retry", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		log.Debug("Received messages.", "count", len(msgs))
		for _, msg := range msgs {
			// Once fetched from broker, the message must be handed to workers.
			// Do not abort midway and leave a partial batch to be redelivered later.
			prefetchCh <- msg
		}

		if ctx.Err() != nil {
			log.Debug("Fetch loop exits after draining fetched batch due to cancelled context.")
			return
		}
	}
}

func (proxy *consumerProxy) nextReceiveNum(ctx context.Context, prefetchCh <-chan *rmq_client.MessageView) (int32, bool) {
	for {
		freeSlots := cap(prefetchCh) - len(prefetchCh)
		if freeSlots > 0 {
			return min(proxy.maxMessageNum, int32(freeSlots)), true
		}

		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (proxy *consumerProxy) workerLoop(client rmq_client.SimpleConsumer, prefetchCh <-chan *rmq_client.MessageView) {
	for mv := range prefetchCh {
		proxy.consumeSingleAndAck(client, mv)
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

// stopSession gracefully stops the current consumer session.
func (proxy *consumerProxy) stopSession(ctx context.Context) {
	log := common.LoggerCtx(ctx)

	proxy.mu.Lock()
	session := proxy.session
	proxy.mu.Unlock()

	if session == nil {
		log.Debug("Consumer client already stopped or not initialized.")
		return
	}

	log.Info("Stopping consumer session...", "sessionSeq", session.seq)
	session.cancel()
	<-session.done
	log.Info("Consumer session stopped.", "sessionSeq", session.seq)
}

func (proxy *consumerProxy) resolvePrefetchSize() int {
	if proxy.prefetchSize > 0 {
		return proxy.prefetchSize
	}

	base := max(proxy.workerNum*2, int(proxy.maxMessageNum))
	return max(base, 1)
}

func (proxy *consumerProxy) withResolvedDefaults() {
	if proxy.maxMessageNum <= 0 {
		proxy.maxMessageNum = 32
	}
	if proxy.invisibleDuration <= 0 {
		proxy.invisibleDuration = 30 * time.Second
	}
	if proxy.workerNum <= 0 {
		proxy.workerNum = 1
	}
	proxy.prefetchSize = proxy.resolvePrefetchSize()
}
