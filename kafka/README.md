# Kafka Package

This package provides a high-level wrapper around the Sarama library for Kafka interaction in Go applications.
It implements both consumer and producer functionality with built-in support for graceful shutdown and
connection management.

## Features

- **Managed Consumers**
  - Multiple consumer support with consistent lifecycle management
  - Dynamic start/stop control via context and switches
  - Graceful shutdown handling for clean application exits
  - Exponential backoff retry mechanism for error recovery
  - Manual Offset management for ensuring at-least-once processing semantics

- **Managed Producers**
  - Support for both synchronous and asynchronous message production
  - Configurable creation of only the needed producer type (sync, async, or both)
  - Configurable retry, compression, and delivery guarantees
  - Callback support for asynchronous message delivery status

## Consumer Usage

### Basic Setup

```go
import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/aarontianqx/gopkg/kafka"
)

// Implement the Consumer interface
type MyConsumer struct {
    config kafka.ConsumerConfig
}

func (c *MyConsumer) GetConfig() *kafka.ConsumerConfig {
    return &c.config
}

func (c *MyConsumer) Handle(ctx context.Context, value []byte) error {
    // Process message value
    log.Printf("Received message: %s", string(value))
    return nil
}

func main() {
    // Create a cancellable context for shutdown control
    ctx, cancel := context.WithCancel(context.Background())
    
    // Setup signal handling for graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        sig := <-sigChan
        log.Printf("Received signal: %v, initiating shutdown", sig)
        cancel() // Cancel the context to trigger shutdown
    }()

    // Create and register consumer
    consumer := &MyConsumer{
        config: kafka.ConsumerConfig{
            JobName:          "my-consumer",
            Topic:            "my-topic",
            BootstrapServers: "localhost:9092",
            ConsumerGroup:    "my-group",
            // It's recommended to set AutoCommit to false for at-least-once processing
            AutoCommit:      false,
        },
    }

    // Register the consumer (doesn't start consuming yet)
    kafka.RegisterConsumer(ctx, consumer)

    // Enable the consumer
    kafka.SetConsumerSwitch("my-consumer", true)

    // Start all registered consumers
    kafka.StartAllConsumers(ctx)

    // Block until context is cancelled (by signal handler)
    <-ctx.Done()
    log.Println("Context cancelled, shutting down...")

    // Wait for all consumers and producers to stop after shutdown
    kafka.WaitStop()
    log.Println("All consumers and producers stopped, exiting")
}
```

## Producer Usage

### Synchronous Producer

```go
import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    "github.com/aarontianqx/gopkg/kafka"
    "github.com/IBM/sarama"
)

func main() {
    // Create a cancellable context for shutdown control
    ctx, cancel := context.WithCancel(context.Background())
    
    // Setup signal handling for graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        sig := <-sigChan
        log.Printf("Received signal: %v, initiating shutdown", sig)
        cancel() // Cancel the context to trigger shutdown
    }()
    
    // Create and register a producer
    producer, err := kafka.RegisterProducer(ctx, kafka.ProducerConfig{
        BootstrapServers: "localhost:9092",
        RequiredAcks:     sarama.WaitForAll,
        MaxRetries:       3,
        RetryBackoff:     200 * time.Millisecond,
        // By default EnableSyncProducer is true, you can be explicit with:
        EnableSyncProducer: kafka.BoolPtr(true),
    })
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    
    // Send message synchronously
    err = producer.Send(ctx, "my-topic", []byte("key"), []byte("message value"))
    if err != nil {
        log.Printf("Failed to send message: %v", err)
    }
    
    // Wait for context cancellation (e.g., from signal handler)
    <-ctx.Done()
    log.Println("Context cancelled, shutting down...")
    
    // All producers will be automatically closed in WaitStop()
    kafka.WaitStop()
    log.Println("All producers stopped, exiting")
}
```

### Asynchronous Producer

```go
// Create an async producer
producer, err := kafka.RegisterProducer(ctx, kafka.ProducerConfig{
    BootstrapServers:    "localhost:9092",
    EnableSyncProducer:  kafka.BoolPtr(false),  // Disable sync producer if not needed
    EnableAsyncProducer: kafka.BoolPtr(true),   // Enable async producer
})
if err != nil {
    log.Fatalf("Failed to create async producer: %v", err)
}

// Send message asynchronously with callback
producer.SendAsync(ctx, "my-topic", []byte("key"), []byte("async message"),
    func(msg *sarama.ProducerMessage, err error) {
        if err != nil {
        log.Printf("Failed to send message: %v", err)
    return
    }
    log.Printf("Message delivered to topic %s, partition %d, offset %d",
    msg.Topic, msg.Partition, msg.Offset)
})
```

## Error Handling and Retry Strategy

The consumer implementation includes an exponential backoff retry mechanism when errors occur:

1. When a consumer encounters an error, it will be restarted with an initial backoff of 1 second
2. If errors continue to occur, the backoff time doubles each time (2s, 4s, 8s, ...) up to a maximum of 60 seconds
3. After 10 consecutive errors (configurable), the consumer will be disabled and require manual intervention

This helps prevent rapid restart loops that might overwhelm system resources while still allowing for automatic recovery from transient errors.

## Configuration

### Consumer Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| JobName | Unique identifier for the consumer | required |
| Topic | Kafka topic to consume | required |
| BootstrapServers | Comma-separated list of Kafka brokers | required |
| ConsumerGroup | Consumer group ID | required |
| AutoCommit | Whether to let Kafka auto-commit offsets | true (recommended: false) |
| InitialOffset | Starting offset when no committed offset exists | OffsetNewest |
| IsolationLevel | Transaction isolation level | ReadCommitted |
| BalanceStrategies | List of partition balancing strategies | [RoundRobin] |
| SessionTimeout | Consumer group session timeout | 10s |
| HeartbeatInterval | Consumer group heartbeat interval | 3s |
| MaxProcessingTime | Maximum processing time per message batch | 100ms |
| MaxMessageBytes | Maximum message size in bytes | 1MB |

### Producer Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| BootstrapServers | Comma-separated list of Kafka brokers | required |
| RequiredAcks | Acknowledgment level | WaitForAll |
| Compression | Compression codec | None |
| MaxRetries | Number of retry attempts | 3 |
| RetryBackoff | Time between retries | 100ms |
| EnableIdempotent | Enable exactly-once delivery (pointer type) | nil (false) |
| MaxMessageBytes | Maximum message size | 1MB |
| EnableSyncProducer | Enable synchronous producer (pointer type) | nil (true) |
| EnableAsyncProducer | Enable asynchronous producer (pointer type) | nil (false) |

> **Note on boolean pointer fields:** For fields like `EnableSyncProducer`, `EnableAsyncProducer`, and `EnableIdempotent`,
> a `nil` value indicates "use default", whereas a non-nil pointer value indicates an explicit setting.
> Use the provided `kafka.BoolPtr(bool)` helper function to create these values, e.g., `EnableSyncProducer: kafka.BoolPtr(true)`.

## Offset Management

By default (with `AutoCommit: false`), offsets are only committed after a message has been successfully processed by the consumer's `Handle` method. This provides at-least-once processing semantics, ensuring that messages aren't marked as processed until your business logic has completed.

If `AutoCommit: true`, Sarama will automatically commit offsets at regular intervals independent of message processing status, which could lead to message loss if a crash occurs after the offset is committed but before processing completes.

The recommended approach is:
1. Set `AutoCommit: false` in your consumer config
2. Return `nil` from your `Handle` method only when processing is successful
3. Return an error from your `Handle` method when processing fails, which will prevent offset commit 