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
	Send(ctx context.Context, topic, tag string, keys []string, value []byte) error
	// SendAsync sends a message asynchronously
	SendAsync(ctx context.Context, topic, tag string, keys []string, value []byte, callback func(error))
	// SendDelay sends a delayed message
	SendDelay(ctx context.Context, topic, tag string, keys []string, value []byte, delay time.Duration) error
	// SendDelayAsync sends a delayed message asynchronously
	SendDelayAsync(ctx context.Context, topic, tag string, keys []string, value []byte, delay time.Duration, callback func(error))
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

func (p *producer) Send(ctx context.Context, topic, tag string, keys []string, value []byte) error {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  value,
	}
	if tag != "" {
		msg.SetTag(tag)
	}
	if len(keys) > 0 {
		msg.SetKeys(keys...)
	}

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, msg)

	// The Send method of the underlying client is blocking.
	_, err := p.client.Send(ctx, msg)
	if err != nil {
		common.LoggerCtx(ctx).Error("Failed to send synchronous message.", "topic", topic, "keys", keys, "error", err)
	}
	return err
}

func (p *producer) SendAsync(ctx context.Context, topic, tag string, keys []string, value []byte, callback func(error)) {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  value,
	}
	if tag != "" {
		msg.SetTag(tag)
	}
	if len(keys) > 0 {
		msg.SetKeys(keys...)
	}

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, msg)

	// The SendAsync method of the underlying client is non-blocking.
	// The provided callback will be invoked upon completion or error.
	p.client.SendAsync(ctx, msg, func(_ context.Context, _ []*rmq_client.SendReceipt, err error) {
		// Error logging for async send failures is handled within this callback wrapper if an error occurs.
		if err != nil {
			common.LoggerCtx(ctx).Error("Failed to send asynchronous message.", "topic", topic, "keys", keys, "error", err)
		}
		callback(err)
	})
}

func (p *producer) SendDelay(ctx context.Context, topic, tag string, keys []string, value []byte, delay time.Duration) error {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  value,
	}
	if tag != "" {
		msg.SetTag(tag)
	}
	if len(keys) > 0 {
		msg.SetKeys(keys...)
	}

	deliveryTimestamp := time.Now().Add(delay)
	msg.SetDelayTimestamp(deliveryTimestamp)

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, msg)

	_, err := p.client.Send(ctx, msg)
	if err != nil {
		common.LoggerCtx(ctx).Error("Failed to send delayed message.", "topic", topic, "keys", keys, "delay", delay.String(), "error", err)
	}
	return err
}

func (p *producer) SendDelayAsync(ctx context.Context, topic, tag string, keys []string, value []byte, delay time.Duration, callback func(error)) {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  value,
	}
	if tag != "" {
		msg.SetTag(tag)
	}
	if len(keys) > 0 {
		msg.SetKeys(keys...)
	}

	deliveryTimestamp := time.Now().Add(delay)
	msg.SetDelayTimestamp(deliveryTimestamp)

	// Inject tracing and request identifiers into message properties.
	injectTraceToMessage(ctx, msg)

	p.client.SendAsync(ctx, msg, func(_ context.Context, _ []*rmq_client.SendReceipt, err error) {
		if err != nil {
			common.LoggerCtx(ctx).Error("Failed to send asynchronous delayed message.", "topic", topic, "keys", keys, "delay", delay.String(), "error", err)
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
