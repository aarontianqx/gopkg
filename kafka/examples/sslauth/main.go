package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/kafka"
)

// SSLConsumer implements the kafka.Consumer interface
type SSLConsumer struct {
	config kafka.ConsumerConfig
}

// GetConfig returns the consumer configuration
func (c *SSLConsumer) GetConfig() *kafka.ConsumerConfig {
	return &c.config
}

// Handle processes a message from Kafka
func (c *SSLConsumer) Handle(ctx context.Context, value []byte) error {
	log := common.LoggerCtx(ctx)
	log.Info("Received message", "message", string(value))
	return nil
}

func main() {
	common.InitLogger(
		logimpl.WithLevel(slog.LevelDebug),
		logimpl.WithAddSource(true),
		logimpl.WithOutput(os.Stdout),
		logimpl.WithFormat("text"),
	)

	// Create a cancelable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := common.LoggerCtx(ctx)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("Received signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	// Example 1: Using CA certificate (most common approach)
	// ----------------------------------------------------
	log.Info("Setting up SSL with CA certificate authentication")

	// Configure and register consumer with SSL/TLS
	consumer := &SSLConsumer{
		config: kafka.ConsumerConfig{
			JobName:       "ssl-consumer",
			Topics:        []string{"secure_topic"},
			Brokers:       []string{"kafka-broker:9093"}, // Note: SSL usually uses port 9093 or 9094
			ConsumerGroup: "ssl_consumer_group",
			AutoCommit:    false,
			// SSL Configuration
			TLS: &kafka.TLSConfig{
				Enable:             true,
				CAFile:             "/path/to/ca.crt",
				InsecureSkipVerify: false, // Set to true to skip server certificate verification (not recommended for production)
			},
		},
	}

	// Register consumer
	kafka.RegisterConsumer(ctx, consumer, true)

	// Example 2: Using client certificates (mutual TLS)
	// ----------------------------------------------------
	log.Info("Setting up producer with mutual TLS authentication")

	// Create a producer with SSL/TLS
	producer, err := kafka.RegisterProducer(ctx, kafka.ProducerConfig{
		Brokers:      []string{"kafka-broker:9093"},
		RequiredAcks: sarama.WaitForAll,
		Compression:  sarama.CompressionSnappy,
		// SSL Configuration with client certificate authentication
		TLS: &kafka.TLSConfig{
			Enable:             true,
			CAFile:             "/path/to/ca.crt",
			CertFile:           "/path/to/client.crt",
			KeyFile:            "/path/to/client.key",
			InsecureSkipVerify: false,
		},
	})
	if err != nil {
		log.Error("Failed to create SSL producer", "err", err)
		return
	}

	// Example 3: Using JKS files (recommended to convert to PEM)
	// ----------------------------------------------------
	log.Info("If you have JKS files, convert them to PEM format:")
	log.Info("- For keystore: keytool -importkeystore -srckeystore keystore.jks -destkeystore keystore.p12 -srcstoretype JKS -deststoretype PKCS12")
	log.Info("- Then: openssl pkcs12 -in keystore.p12 -out client.key -nodes -nocerts")
	log.Info("- And: openssl pkcs12 -in keystore.p12 -out client.crt -nokeys")
	log.Info("- For truststore: keytool -exportcert -keystore truststore.jks -file ca.crt -rfc")

	// Start consumers and send test message
	log.Info("Starting consumers...")
	kafka.StartAllConsumers(ctx)

	// Send a test message
	err = producer.Send(ctx, "secure_topic", []byte("test-key"), []byte("This is a secure test message"))
	if err != nil {
		log.Error("Failed to send message", "err", err)
	} else {
		log.Info("Successfully sent message to secure topic")
	}

	// Run for a few seconds then exit
	time.Sleep(5 * time.Second)

	// Wait for context cancellation or explicitly cancel for this example
	cancel()
	log.Info("Context cancelled, waiting for components to stop...")

	// Wait for all consumers and producers to finish
	kafka.WaitStop()
	log.Info("All components stopped, exiting")
}
