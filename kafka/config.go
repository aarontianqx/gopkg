package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aarontianqx/gopkg/common"
)

// TLSConfig holds SSL/TLS configuration options for Kafka connections
type TLSConfig struct {
	// Enable TLS for connections to Kafka
	Enable bool
	// The path to the client's certificate in PEM format
	CertFile string
	// The path to the client's private key in PEM format
	KeyFile string
	// The path to the CA certificate file in PEM format for server certificate verification
	CAFile string
	// Whether to skip verification of the server's certificate chain and host name
	InsecureSkipVerify bool
	// Client authentication type (none, request, require)
	ClientAuth string
}

// SASLConfig holds SASL authentication options for Kafka connections
type SASLConfig struct {
	// Enable SASL authentication
	Enable bool
	// SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Mechanism string
	// Username for SASL authentication
	Username string
	// Password for SASL authentication
	Password string
	// Whether to perform SASL handshake (Sarama default: true)
	Handshake bool
}

// ProducerConfig holds configuration for a Kafka producer
type ProducerConfig struct {
	Brokers []string
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
	// SSL/TLS configuration
	TLS *TLSConfig
	// SASL configuration
	SASL *SASLConfig
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

	// Configure TLS if provided
	if c.TLS != nil && c.TLS.Enable {
		tlsConfig, err := configureTLS(c.TLS)
		if err != nil {
			// Log the error but don't fail - will attempt connection without TLS
			common.Logger().Error("Failed to configure TLS", "error", err)
		} else {
			saramaConfig.Net.TLS.Enable = true
			saramaConfig.Net.TLS.Config = tlsConfig
		}
	}

	// Configure SASL if provided
	if c.SASL != nil && c.SASL.Enable {
		if err := configureSASL(saramaConfig, c.SASL); err != nil {
			common.Logger().Error("Failed to configure SASL", "error", err)
		}
	}

	return saramaConfig
}

// ConsumerConfig holds configuration for a Kafka consumer
type ConsumerConfig struct {
	JobName       string // Job identifier
	Topics        []string
	Brokers       []string
	ConsumerGroup string
	// Additional consumer-specific settings
	AutoCommit        bool                     // Default: false (recommended to set false to ensure messages are committed only after successful processing)
	InitialOffset     int64                    // Default: OffsetNewest
	IsolationLevel    sarama.IsolationLevel    // Default: ReadCommitted
	BalanceStrategies []sarama.BalanceStrategy // Default: [BalanceStrategyRange]
	SessionTimeout    time.Duration            // Default: 10s
	HeartbeatInterval time.Duration            // Default: 3s
	MaxProcessingTime time.Duration            // Default: 100ms
	MaxMessageBytes   int32                    // Default: 1MB, increase for larger messages
	// SSL/TLS configuration
	TLS *TLSConfig
	// SASL configuration
	SASL *SASLConfig
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

	// Configure TLS if provided
	if c.TLS != nil && c.TLS.Enable {
		tlsConfig, err := configureTLS(c.TLS)
		if err != nil {
			// Log the error but don't fail - will attempt connection without TLS
			common.Logger().Error("Failed to configure TLS", "error", err)
		} else {
			cfg.Net.TLS.Enable = true
			cfg.Net.TLS.Config = tlsConfig
		}
	}

	// Configure SASL if provided
	if c.SASL != nil && c.SASL.Enable {
		if err := configureSASL(cfg, c.SASL); err != nil {
			common.Logger().Error("Failed to configure SASL", "error", err)
		}
	}

	return cfg
}

// configureTLS creates a tls.Config from the TLSConfig options
func configureTLS(config *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	// Load client certificate and key if provided
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = certPool
	}

	// Configure client authentication mode
	switch config.ClientAuth {
	case "none":
		tlsConfig.ClientAuth = tls.NoClientCert
	case "request":
		tlsConfig.ClientAuth = tls.RequestClientCert
	case "require":
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// configureSASL applies SASL settings to sarama.Config
func configureSASL(cfg *sarama.Config, sasl *SASLConfig) error {
	if sasl.Username == "" {
		return fmt.Errorf("SASL username is required when SASL is enabled")
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.User = sasl.Username
	cfg.Net.SASL.Password = sasl.Password
	// Default handshake true unless explicitly set to false
	cfg.Net.SASL.Handshake = true
	if !sasl.Handshake {
		cfg.Net.SASL.Handshake = false
	}

	mechanism := strings.ToUpper(strings.TrimSpace(sasl.Mechanism))
	switch mechanism {
	case sarama.SASLTypePlaintext:
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case sarama.SASLTypeSCRAMSHA256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = scramClientGeneratorSHA256
	case sarama.SASLTypeSCRAMSHA512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = scramClientGeneratorSHA512
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", sasl.Mechanism)
	}
	return nil
}
