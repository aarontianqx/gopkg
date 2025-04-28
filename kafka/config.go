package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

// ProducerConfig holds configuration for a Kafka producer
type ProducerConfig struct {
	BootstrapServers string
	// Additional producer-specific settings
	RequiredAcks     sarama.RequiredAcks     // Default: WaitForAll
	Compression      sarama.CompressionCodec // Default: None
	MaxRetries       int                     // Default: 3
	RetryBackoff     time.Duration           // Default: 100ms
	EnableIdempotent *bool                   // Default: false (requires version >= 0.11.0.0)
	MaxMessageBytes  int                     // Default: 1000000 (1MB)
	// Controls producer type
	EnableSyncProducer  *bool // Default: true
	EnableAsyncProducer *bool // Default: false
}

// BoolPtr creates a pointer to a boolean value
func BoolPtr(b bool) *bool {
	return &b
}

// toSaramaConfig converts ProducerConfig to sarama.Config
func (c *ProducerConfig) toSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()

	// Set Kafka version to a reasonable default
	saramaConfig.Version = sarama.V2_6_2_0

	// Configure producer settings
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	// Apply custom config settings
	if c.RequiredAcks != 0 {
		saramaConfig.Producer.RequiredAcks = c.RequiredAcks
	} else {
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	}

	if c.Compression != 0 {
		saramaConfig.Producer.Compression = c.Compression
	}

	if c.MaxRetries > 0 {
		saramaConfig.Producer.Retry.Max = c.MaxRetries
	} else {
		saramaConfig.Producer.Retry.Max = 3
	}

	if c.RetryBackoff > 0 {
		saramaConfig.Producer.Retry.Backoff = c.RetryBackoff
	} else {
		saramaConfig.Producer.Retry.Backoff = 100 * time.Millisecond // default 100ms
	}

	// Check if idempotence is explicitly set
	if c.EnableIdempotent != nil && *c.EnableIdempotent {
		saramaConfig.Producer.Idempotent = true
		saramaConfig.Net.MaxOpenRequests = 1 // Required for idempotence
	}

	// Set maximum message size if specified
	if c.MaxMessageBytes > 0 {
		saramaConfig.Producer.MaxMessageBytes = c.MaxMessageBytes
	}

	return saramaConfig
}

// ConsumerConfig holds configuration for a Kafka consumer
type ConsumerConfig struct {
	JobName          string // Job identifier
	Topic            string
	BootstrapServers string
	ConsumerGroup    string
	// Additional consumer-specific settings
	AutoCommit        bool                     // Default: false (recommended to set false to ensure messages are committed only after successful processing)
	InitialOffset     int64                    // Default: OffsetNewest
	IsolationLevel    sarama.IsolationLevel    // Default: ReadCommitted
	BalanceStrategies []sarama.BalanceStrategy // Default: [BalanceStrategyRange]
	SessionTimeout    time.Duration            // Default: 10s
	HeartbeatInterval time.Duration            // Default: 3s
	MaxProcessingTime time.Duration            // Default: 100ms
	MaxMessageBytes   int32                    // Default: 1MB, increase for larger messages
}

// toSaramaConfig converts ConsumerConfig to sarama.Config
func (c *ConsumerConfig) toSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_2_0
	cfg.ClientID = c.JobName

	// Default strategy is range if not specified
	if len(c.BalanceStrategies) > 0 {
		cfg.Consumer.Group.Rebalance.GroupStrategies = c.BalanceStrategies
	} else {
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	}

	cfg.Consumer.Return.Errors = true

	// Use provided AutoCommit setting or default to true
	cfg.Consumer.Offsets.AutoCommit.Enable = c.AutoCommit

	// Use provided InitialOffset or default to OffsetNewest
	if c.InitialOffset != 0 {
		cfg.Consumer.Offsets.Initial = c.InitialOffset
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest // Changed from OffsetOldest to OffsetNewest
	}

	// Use provided IsolationLevel or default to ReadCommitted
	if c.IsolationLevel != 0 {
		cfg.Consumer.IsolationLevel = c.IsolationLevel
	} else {
		cfg.Consumer.IsolationLevel = sarama.ReadCommitted
	}

	// Set session timeout if provided
	if c.SessionTimeout > 0 {
		cfg.Consumer.Group.Session.Timeout = c.SessionTimeout
	}

	// Set heartbeat interval if provided
	if c.HeartbeatInterval > 0 {
		cfg.Consumer.Group.Heartbeat.Interval = c.HeartbeatInterval
	}

	// Set max processing time if provided
	if c.MaxProcessingTime > 0 {
		cfg.Consumer.MaxProcessingTime = c.MaxProcessingTime
	}

	// Set maximum message size
	if c.MaxMessageBytes > 0 {
		cfg.Consumer.Fetch.Max = c.MaxMessageBytes
		// Also ensure that the broker's configured maximum is respected
		cfg.Consumer.Fetch.Default = c.MaxMessageBytes
	}

	return cfg
}
