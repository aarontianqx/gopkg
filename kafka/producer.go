package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"

	"github.com/aarontianqx/gopkg/common"
)

// IProducer defines methods for producing messages to Kafka
type IProducer interface {
	// Send sends a message to Kafka and returns delivery status
	Send(ctx context.Context, topic string, key []byte, value []byte) error
	// SendAsync sends a message to Kafka asynchronously
	SendAsync(ctx context.Context, topic string, key []byte, value []byte, callback func(*sarama.ProducerMessage, error))
	// Close gracefully shuts down the producer
	Close() error
	// GetID returns the unique identifier for this producer
	GetID() string
}

// producer implements IProducer interface
type producer struct {
	id            string
	brokers       []string
	saramaConfig  *sarama.Config
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	closeChan     chan struct{}
	closeOnce     sync.Once
}

// GetID returns the unique identifier for this producer
func (p *producer) GetID() string {
	return p.id
}

// Send implements IProducer interface
func (p *producer) Send(ctx context.Context, topic string, key []byte, value []byte) error {
	if p.syncProducer == nil {
		return fmt.Errorf("sync producer not initialized, please enable EnableSyncProducer in config")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	// Apply any message transformations
	p.injectTraceContext(ctx, msg)

	_, _, err := p.syncProducer.SendMessage(msg)
	if err != nil {
		common.LoggerCtx(ctx).Error("Failed to send message",
			"topic", topic,
			"error", err,
		)
		return fmt.Errorf("failed to send message to topic %s: %w", topic, err)
	}

	return nil
}

// SendAsync implements IProducer interface
func (p *producer) SendAsync(ctx context.Context, topic string, key []byte, value []byte, callback func(*sarama.ProducerMessage, error)) {
	if p.asyncProducer == nil {
		if callback != nil {
			msg := &sarama.ProducerMessage{Topic: topic}
			callback(msg, fmt.Errorf("async producer not initialized, please enable EnableAsyncProducer in config"))
		}
		common.LoggerCtx(ctx).Error("Async producer not initialized",
			"topic", topic,
			"producer_id", p.id)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	// Store callback in metadata for access in handler
	if callback != nil {
		msg.Metadata = callback
	}

	// Apply any message transformations
	p.injectTraceContext(ctx, msg)

	// Send message to async producer
	select {
	case p.asyncProducer.Input() <- msg:
		// Message accepted by producer
	case <-ctx.Done():
		// Context was canceled before message could be sent
		if callback != nil {
			callback(msg, ctx.Err())
		}
	}
}

// Close implements IProducer interface
func (p *producer) Close() error {
	var err error

	p.closeOnce.Do(func() {
		// Signal error handler to exit
		close(p.closeChan)

		// Close producers
		var asyncErr, syncErr error

		if p.asyncProducer != nil {
			asyncErr = p.asyncProducer.Close()
		}

		if p.syncProducer != nil {
			syncErr = p.syncProducer.Close()
		}

		// Combine errors if both failed
		if asyncErr != nil && syncErr != nil {
			err = fmt.Errorf("async producer: %v; sync producer: %v", asyncErr, syncErr)
		} else if asyncErr != nil {
			err = asyncErr
		} else if syncErr != nil {
			err = syncErr
		}

		// Save ID for logging
		producerID := p.id

		// Remove from global map using a separate function to avoid lock nesting
		removeProducer(producerID)

		// Log closure status
		if err != nil {
			common.Logger().Error("Producer closed with error", "id", producerID, "error", err)
		} else {
			common.Logger().Debug("Producer closed successfully", "id", producerID)
		}

		// Decrease WaitGroup counter
		globalWg.Done()
	})

	return err
}

// handleErrors processes errors from async producer
func (p *producer) handleErrors(ctx context.Context) {
	defer common.Recovery(ctx)

	log := common.LoggerCtx(ctx)

	for {
		select {
		case <-p.closeChan:
			return

		case err, ok := <-p.asyncProducer.Errors():
			if !ok {
				return // Channel closed
			}
			log.Error("Async producer error",
				"topic", err.Msg.Topic,
				"error", err.Err,
			)

			// Call callback with error if it exists
			if err.Msg.Metadata != nil {
				if callback, ok := err.Msg.Metadata.(func(*sarama.ProducerMessage, error)); ok {
					callback(err.Msg, err.Err)
				}
			}

		case msg, ok := <-p.asyncProducer.Successes():
			if !ok {
				return // Channel closed
			}

			// Call callback with success if it exists
			if msg.Metadata != nil {
				if callback, ok := msg.Metadata.(func(*sarama.ProducerMessage, error)); ok {
					callback(msg, nil)
				}
			}
		}
	}
}

// injectTraceContext is a placeholder for any future tracing integration
func (p *producer) injectTraceContext(ctx context.Context, msg *sarama.ProducerMessage) {
	// Tracing functionality removed
}
