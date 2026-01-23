package rocketmq

import (
	"context"
	"sync"
	"time"

	"github.com/aarontianqx/gopkg/common"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

// IProducer defines methods for producing messages to RocketMQ
type IProducer interface {
	// Send sends a message synchronously
	Send(ctx context.Context, msg *Message) error
	// SendAsync sends a message asynchronously
	SendAsync(ctx context.Context, msg *Message, callback func(error))
	// SendDelay sends a delayed message
	SendDelay(ctx context.Context, msg *Message, delay time.Duration) error
	// SendDelayAsync sends a delayed message asynchronously
	SendDelayAsync(ctx context.Context, msg *Message, delay time.Duration, callback func(error))
	// Close gracefully shuts down the producer
	Close() error
	// GetID returns the producer's unique identifier
	GetID() string
}

// producer is a wrapper around the RocketMQ client producer,
// managing its lifecycle and an association with the global wait group for graceful shutdown.
type producer struct {
	id       string
	client   rmq_client.Producer
	wg       *sync.WaitGroup
	stopOnce sync.Once
}

// GetID returns the producer's unique identifier
func (p *producer) GetID() string {
	return p.id
}

func (p *producer) Send(ctx context.Context, msg *Message) error {
	rmqMsg := msg.toRocketMQMessage()

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, rmqMsg)

	// The Send method of the underlying client is blocking.
	_, err := p.client.Send(ctx, rmqMsg)
	if err != nil {
		common.LoggerCtx(ctx).Error("Failed to send synchronous message.",
			"topic", msg.Topic, "tag", msg.Tag, "keys", msg.Keys, "error", err)
	}
	return err
}

func (p *producer) SendAsync(ctx context.Context, msg *Message, callback func(error)) {
	rmqMsg := msg.toRocketMQMessage()

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, rmqMsg)

	// The SendAsync method of the underlying client is non-blocking.
	// The provided callback will be invoked upon completion or error.
	p.client.SendAsync(ctx, rmqMsg, func(_ context.Context, _ []*rmq_client.SendReceipt, err error) {
		// Error logging for async send failures is handled within this callback wrapper if an error occurs.
		if err != nil {
			common.LoggerCtx(ctx).Error("Failed to send asynchronous message.",
				"topic", msg.Topic, "tag", msg.Tag, "keys", msg.Keys, "error", err)
		}
		callback(err)
	})
}

func (p *producer) SendDelay(ctx context.Context, msg *Message, delay time.Duration) error {
	rmqMsg := msg.toRocketMQDelayMessage(delay)

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, rmqMsg)

	_, err := p.client.Send(ctx, rmqMsg)
	if err != nil {
		common.LoggerCtx(ctx).Error("Failed to send delayed message.",
			"topic", msg.Topic, "tag", msg.Tag, "keys", msg.Keys, "delay", delay.String(), "error", err)
	}
	return err
}

func (p *producer) SendDelayAsync(ctx context.Context, msg *Message, delay time.Duration, callback func(error)) {
	rmqMsg := msg.toRocketMQDelayMessage(delay)

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, rmqMsg)

	p.client.SendAsync(ctx, rmqMsg, func(_ context.Context, _ []*rmq_client.SendReceipt, err error) {
		if err != nil {
			common.LoggerCtx(ctx).Error("Failed to send asynchronous delayed message.",
				"topic", msg.Topic, "tag", msg.Tag, "keys", msg.Keys, "delay", delay.String(), "error", err)
		}
		callback(err)
	})
}

// Close gracefully shuts down the producer.
// It ensures that wg.Done() is called exactly once for producers managed by RegisterProducer.
func (p *producer) Close() error {
	log := common.Logger() // Use a general logger as context might be done.
	var err error
	p.stopOnce.Do(func() {
		log.Info("Attempting to stop producer client.", "id", p.id)
		err = p.client.GracefulStop()
		if err != nil {
			log.Error("Error stopping producer client.", "id", p.id, "error", err)
		} else {
			log.Info("Producer client stopped successfully.", "id", p.id)
		}

		// Remove from global producers map if registered
		if p.id != "" {
			removeProducer(p.id)
		}

		// wg is non-nil only for producers created via RegisterProducer.
		if p.wg != nil {
			p.wg.Done()
			log.Debug("Producer called wg.Done().", "id", p.id)
		}
	})
	return err
}
