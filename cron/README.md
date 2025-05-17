# Cron Package

This package provides a flexible and robust cron job scheduling system for Go applications. 
It builds upon the robfig/cron library, adding features for worker management, dynamic configuration 
updates, and graceful shutdown handling.

## Features

- **Multiple Worker Management**
  - Support for multiple concurrent workers per job
  - Dynamic worker scaling at runtime
  - Automatic worker distribution with random job key generation
  
- **Flexible Configuration**
  - Standard cron expression format support
  - Runtime configuration update via callback function
  - Optional second-level precision for fine-grained scheduling
  
- **Lifecycle Management**
  - Graceful start and stop handling
  - Context-based cancellation for concurrent shutdown
  - Panic recovery for job execution
  
- **Monitoring & Observability**
  - Integration with common package for structured logging
  - Request ID generation for job executions
  - Automatic error logging

## Usage

### Basic Setup

This example demonstrates the basic lifecycle of the cron scheduler using context 
cancellation for graceful shutdown.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/cron"
)

func myCronJob(ctx context.Context) error {
	// Use context-aware logger within the job
	log := common.LoggerCtx(ctx)
	log.Info("Executing myCronJob task...")
	// Simulate work
	time.Sleep(1 * time.Second)
	log.Info("myCronJob task finished.")
	return nil
}

func main() {
	// Use the base logger for application-level logging
	log := common.Logger()
	log.Info("Starting application...")

	// 1. Create top-level context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called eventually

	// 2. Setup signal handling to trigger cancellation
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("Received signal, initiating shutdown...", "signal", sig.String())
		cancel() // Trigger context cancellation
	}()

	// 3. Initialize and configure the scheduler
	log.Info("Initializing scheduler...")
	scheduler := cron.DefaultScheduler // Or cron.NewScheduler(false)

	// Register jobs
	err := scheduler.RegisterJob("my-job", cron.Config{
		Spec:      "*/5 * * * * *", // Run every 5 seconds for demonstration
		WorkerNum: 1,
	}, myCronJob)
	if err != nil {
		log.Error("Failed to register cron job", "error", err)
		os.Exit(1) // Or handle fatal error appropriately
	}

	// 4. Start the scheduler (runs its daemon in the background)
	scheduler.Start(ctx)
	log.Info("Scheduler started. Waiting for shutdown signal...")

	// 5. Wait for cancellation signal (from signal handler or other source)
	<-ctx.Done()
	log.Info("Shutdown signal received.")

	// 6. Wait for the scheduler to stop gracefully
	log.Info("Waiting for scheduler to stop...")
	scheduler.WaitStop()
	log.Info("Scheduler stopped gracefully.")

	log.Info("Exiting application.")
}
```

**Note on Multiple Services:** If your application runs multiple background services (e.g., an HTTP server, Kafka consumers, *and* this cron scheduler), you should use a `sync.WaitGroup` to manage their lifecycles. In that case, you would typically start each service, including the scheduler's `Start` and `WaitStop` sequence, within its own goroutine managed by the `WaitGroup`, similar to the previous version of this example.

### Dynamic Configuration Updates

You can dynamically update job configurations using the `SetConfigFunc` method:

```go
// Create a function that returns configuration based on job name
configFunc := func(jobName string) *cron.Config {
    // Get configuration from a database, config service, etc.
    // Return nil if no changes needed
    return &cron.Config{
        Spec:      "*/10 * * * *", // Update to run every 10 minutes
        WorkerNum: 5,              // Scale to 5 workers
    }
}

// Set the configuration function
scheduler.SetConfigFunc(configFunc)

// Manually trigger configuration update for a specific job
err := scheduler.SyncConfig(ctx, "my-job", cron.Config{
    Spec:      "0 */1 * * *", // Run at the start of every hour
    WorkerNum: 2,             // Scale down to 2 workers
})
```

### Creating a Scheduler with Second-level Precision

```go
// Create a scheduler with second-level precision
scheduler := cron.NewScheduler(true)

// Register a job with second-level precision
scheduler.RegisterJob("frequent-job", cron.Config{
    Spec:      "*/30 * * * * *", // Run every 30 seconds
    WorkerNum: 1,
}, myJobFunc)
```

## Configuration

### Job Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| Spec      | Cron expression string, following standard cron format | Required |
| WorkerNum | Number of concurrent workers for the job | Required |

## Error Handling

Jobs are executed with panic recovery to prevent a single job failure from crashing the application. Any panics are captured, logged, and treated as job execution failures.

If a job function returns an error, it will be logged but the job will continue to execute on its next scheduled time.

## Advanced Usage

### Creating and Using Multiple Schedulers

```go
// Create a scheduler with second-level precision
schedulerA := cron.NewScheduler(true)

// Create another scheduler with minute-level precision
schedulerB := cron.NewScheduler(false)

// Register different jobs on different schedulers
schedulerA.RegisterJob("high-frequency-job", cron.Config{
    Spec:      "*/15 * * * * *", // Every 15 seconds
    WorkerNum: 1,
}, highFrequencyTask)

schedulerB.RegisterJob("daily-job", cron.Config{
    Spec:      "0 0 * * *",      // Once per day at midnight
    WorkerNum: 5, 
}, dailyBatchTask)

// Start both schedulers
schedulerA.Start(ctx)
schedulerB.Start(ctx)

// Later, stop them gracefully
schedulerA.Stop(ctx)
schedulerB.Stop(ctx)
``` 