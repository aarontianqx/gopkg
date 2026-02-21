# RocketMQ Package

This package provides a high-level wrapper around the Apache RocketMQ Clients Golang v5 SDK for easier integration in Go applications. 
It implements both consumer and producer functionality with built-in support for graceful shutdown, 
connection management, and error handling.

## Features

- **Managed Consumers**
  - Multiple consumer support with consistent lifecycle management
  - Dynamic start/stop control via consumer switches
  - Graceful shutdown handling for clean application exits
  - Simplified error handling for common failures
  - Automatic message acknowledgment

- **Managed Producers**
  - Support for both synchronous and asynchronous message production
  - Delayed message delivery support
  - Message tagging and key support
  - Centralized lifecycle management with graceful shutdown

## Consumer Usage

### Basic Setup

```go
import (
    "context"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/aarontianqx/gopkg/common"
    "github.com/aarontianqx/gopkg/common/logimpl"
    "github.com/aarontianqx/gopkg/rocketmq"
)

// Initialize the common logger
func init() {
    common.InitLogger(
        logimpl.WithLevel(slog.LevelDebug),
        logimpl.WithAddSource(true),
        logimpl.WithOutput(os.Stdout),
        logimpl.WithFormat("text"),
    )
}

// Implement the Consumer interface
type MyConsumer struct {
    config rocketmq.ConsumerConfig
}

func (c *MyConsumer) GetConfig() *rocketmq.ConsumerConfig {
    return &c.config
}

func (c *MyConsumer) Handle(ctx context.Context, msg *rocketmq.Message) error {
    // Process message
    log := common.LoggerCtx(ctx)
    log.Info("Received message", "tag", msg.Tag, "body", string(msg.Body))
    return nil
}

func main() {
    // Create a cancelable context for shutdown control
    ctx, cancel := context.WithCancel(context.Background())
    
    // Setup signal handling for graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        sig := <-sigChan
        fmt.Printf("Received signal: %v, initiating shutdown\n", sig)
        cancel() // Cancel the context to trigger shutdown
    }()
    
    // Create and register consumer
    consumer := &MyConsumer{
        config: rocketmq.ConsumerConfig{
            JobName:           "my-consumer",
            Topic:             "my-topic",
            Endpoint:          "localhost:8081",
            ConsumerGroup:     "my-group",
            MaxMessageNum:     32,              // Maximum messages to fetch in one receive call
            InvisibleDuration: 30 * time.Second, // Message invisible duration
            WorkerNum:         5,               // Enable 5 concurrent workers for faster processing
            // Authentication (if needed)
            // AccessKey:     "your-access-key",
            // AccessSecret:  "your-secret-key",
        },
    }
    
    // Register the consumer with auto-start enabled
    rocketmq.RegisterConsumer(ctx, consumer, true)
    
    // Start all registered consumers
    rocketmq.StartAllConsumers(ctx)
    
    // Block until context is cancelled (by signal handler)
    <-ctx.Done()
    fmt.Println("Context cancelled, shutting down...")
    
    // Wait for all consumers and producers to stop after shutdown
    rocketmq.WaitStop()
    fmt.Println("All consumers and producers stopped, exiting")
}
```

## Producer Usage

### Synchronous and Asynchronous Producer

```go
import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "sync"

    "github.com/aarontianqx/gopkg/common"
    "github.com/aarontianqx/gopkg/common/logimpl"
    "github.com/aarontianqx/gopkg/rocketmq"
)

func main() {
    // Initialize the common logger
    common.InitLogger(
        logimpl.WithLevel(slog.LevelDebug),
        logimpl.WithAddSource(true),
        logimpl.WithOutput(os.Stdout),
        logimpl.WithFormat("text"),
    )

    // Create a cancelable context for shutdown control
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Setup signal handling for graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        sig := <-sigChan
        common.Logger().Info("Received signal, initiating shutdown", "signal", sig)
        cancel() // Cancel the context to trigger shutdown
    }()
    
    // Define parameters
    topic := "my-topic"
    
    // Create and register a producer
    producer, err := rocketmq.RegisterProducer(ctx, rocketmq.ProducerConfig{
        Endpoint: "localhost:8081",
        Topics:   []string{topic},
        // Authentication (if needed)
        // AccessKey: "your-access-key",
        // AccessSecret: "your-secret-key",
    })
    if err != nil {
        common.Logger().Error("Failed to create producer", "error", err)
        return
    }
    
    // Send message synchronously
    common.Logger().Info("Sending synchronous messages...")
    for i := 0; i < 5; i++ {
        msg := &rocketmq.Message{
            Topic: topic,
            Tag:   "test-tag",
            Keys:  []string{fmt.Sprintf("key-%d", i)},
            Body:  []byte(fmt.Sprintf("This is a synchronous message - %d", i)),
        }
        err = producer.Send(ctx, msg)
        if err != nil {
            common.Logger().Error("Failed to send synchronous message", "error", err)
        } else {
            common.Logger().Info("Successfully sent synchronous message", "key", msg.Keys[0])
        }
    }
    
    // Send asynchronous messages
    var wg sync.WaitGroup
    for i := 0; i < 3; i++ {
        wg.Add(1)
        messageNum := i
        go func() {
            defer wg.Done()

            msg := &rocketmq.Message{
                Topic: topic,
                Tag:   "async-tag",
                Keys:  []string{fmt.Sprintf("async-key-%d", messageNum)},
                Body:  []byte(fmt.Sprintf("This is async message #%d", messageNum)),
            }

            common.Logger().Info("Sending asynchronous message", "number", messageNum)

            // Send message asynchronously with callback
            producer.SendAsync(ctx, msg, func(err error) {
                if err != nil {
                    common.Logger().Error("Failed to send async message", "number", messageNum, "error", err)
                    return
                }
                common.Logger().Info("Successfully delivered async message", "number", messageNum)
            })
        }()
    }

    // Send a delayed message
    common.Logger().Info("Sending delayed message...")
    delayMsg := &rocketmq.Message{
        Topic: topic,
        Tag:   "delay-tag",
        Keys:  []string{"delay-key-1"},
        Body:  []byte("This is a message that will be delivered after 5 seconds"),
    }
    err = producer.SendDelay(ctx, delayMsg, 5*time.Second)
    if err != nil {
        common.Logger().Error("Failed to send delayed message", "error", err)
    } else {
        common.Logger().Info("Successfully sent delayed message", "key", delayMsg.Keys[0])
    }
    
    // Wait for all async messages to be sent or context to be cancelled
    go func() {
        wg.Wait()
        common.Logger().Info("All async messages sent")
    }()
    
    // Wait for context cancellation (e.g., from signal handler)
    <-ctx.Done()
    common.Logger().Info("Context cancelled, shutting down...")
    
    // Wait for all producers to stop
    rocketmq.WaitStop()
    common.Logger().Info("All producers stopped, exiting")
}
```

## Consumer Controls

You can dynamically enable or disable specific consumers:

```go
// Enable a consumer
rocketmq.SetConsumerSwitch("my-consumer", true)

// Disable a consumer
rocketmq.SetConsumerSwitch("my-consumer", false)

// Check if a consumer is enabled
isEnabled := rocketmq.GetConsumerSwitch("my-consumer")
```

## Important Implementation Notes

1. **Pipeline Processing**: The consumer uses a fetch-worker pipeline:
   - `FetchLoop` pulls messages in batches using `MaxMessageNum`, bounded by prefetch queue free slots
   - A buffered prefetch queue decouples network IO from business handling
   - A fixed worker pool (`WorkerNum`) processes and ACKs messages in parallel

2. **Configuration Options**:
   - `MaxMessageNum`: Maximum messages to receive in a single poll (default: 32)
   - `InvisibleDuration`: Message invisible duration after being pulled (default: 30s)
   - `WorkerNum`: Number of concurrent workers for message processing (default: 1)

3. **Graceful Shutdown**: The consumer ensures clean shutdown in all scenarios:
   - Session context cancellation interrupts blocking `Receive` calls immediately
   - Once a batch is fetched, it is fully handed to workers before fetch loop exits
   - Prefetched and in-flight messages are drained through the pipeline before client stop
   - `StartDaemon` acts as the single lifecycle controller for session start/stop

4. **Error Handling**: Common RocketMQ errors are handled gracefully:
   - No available messages: continues polling
   - Network timeouts: logs and retries
   - Other errors: logs and retries after a brief delay

5. **Producer Lifecycle**: Producers are managed centrally:
   - Synchronous, asynchronous, and delayed message sending
   - Producers registered via `RegisterProducer()` are managed globally
   - Graceful shutdown via `WaitStop()`; manual closure via `Close()` if needed

6. **Distributed Tracing**: Trace context is propagated automatically via message properties:
   - `x-trace-id`, `x-span-id`: OpenTelemetry trace/span IDs
   - `x-request-id`: Request ID from `BaseLogInfo`
   - Producer injects these from `context.Context`; consumer restores them so logs share the same correlation
