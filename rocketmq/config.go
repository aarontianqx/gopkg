package rocketmq

import (
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// ProducerConfig holds configuration for a RocketMQ producer
type ProducerConfig struct {
	// Required settings
	Endpoint     string   // RocketMQ server endpoint
	Topics       []string // Topics to produce to
	AccessKey    string
	AccessSecret string
}

// ConsumerConfig holds configuration for a RocketMQ consumer
type ConsumerConfig struct {
	JobName           string        // Unique job identifier
	Topic             string        // Topic to consume (single topic)
	Endpoint          string        // RocketMQ server endpoint
	ConsumerGroup     string        // Consumer group name
	AccessKey         string        // Access key for authentication
	AccessSecret      string        // Access Secret for authentication
	TagExpression     string        // Tag filtering expression, e.g., "tag1 || tag2" (optional)
	MaxMessageNum     int32         // Maximum number of messages to receive in a single poll (default: 32)
	InvisibleDuration time.Duration // Message invisible duration after being pulled (default: 30s). This is used as the await time for Receive calls.
	WorkerNum         int           // Number of concurrent workers for message processing (default: 1)
}

// toRocketMQConfig converts ProducerConfig to rmq_client.Config
func (c *ProducerConfig) toRocketMQConfig() *rmq_client.Config {
	config := &rmq_client.Config{
		Endpoint: c.Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    c.AccessKey,
			AccessSecret: c.AccessSecret,
		},
	}

	return config
}

// toRocketMQConfig converts ConsumerConfig to rmq_client.Config
func (c *ConsumerConfig) toRocketMQConfig() *rmq_client.Config {
	config := &rmq_client.Config{
		Endpoint:      c.Endpoint,
		ConsumerGroup: c.ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    c.AccessKey,
			AccessSecret: c.AccessSecret,
		},
	}

	return config
}
