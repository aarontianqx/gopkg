package rocketmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aarontianqx/gopkg/common"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

var (
	globalWg    sync.WaitGroup
	switchMap   sync.Map
	consumers   = make(map[string]*consumerProxy)
	producers   = make(map[string]IProducer)
	registerMu  sync.Mutex
	producersMu sync.RWMutex
)

// RegisterConsumer registers a consumer without starting it
// If defaultStart is true, the consumer switch will be enabled by default,
// allowing it to start consuming when StartAllConsumers is called
func RegisterConsumer(ctx context.Context, consumer IConsumer, defaultStart bool) {
	config := consumer.GetConfig()
	log := common.LoggerCtx(ctx)

	log.Info("Registering new consumer.",
		"jobName", config.JobName,
		"topic", config.Topic,
		"consumerGroup", config.ConsumerGroup,
		"endpoint", config.Endpoint,
		"defaultStart", defaultStart)

	proxy := &consumerProxy{
		jobName:           config.JobName,
		topic:             config.Topic,
		maxMessageNum:     config.MaxMessageNum,
		invisibleDuration: config.InvisibleDuration,
		config:            config.toRocketMQConfig(),
		handler:           consumer.Handle,
		started:           false,
		workerNum:         config.WorkerNum,
	}
	if config.TagExpression == "" {
		proxy.filterExpression = rmq_client.SUB_ALL
	} else {
		proxy.filterExpression = rmq_client.NewFilterExpression(config.TagExpression)
	}
	if proxy.maxMessageNum <= 0 {
		proxy.maxMessageNum = 32
	}
	if proxy.invisibleDuration <= 0 {
		proxy.invisibleDuration = 30 * time.Second
	}
	if proxy.workerNum <= 0 {
		proxy.workerNum = 1
	}
	proxy.workerSemaphore = make(chan struct{}, proxy.workerNum)

	registerMu.Lock()
	consumers[config.JobName] = proxy
	registerMu.Unlock()

	if defaultStart {
		SetConsumerSwitch(config.JobName, true)
		log.Debug("Consumer switch enabled by default.")
	} else {
		SetConsumerSwitch(config.JobName, false)
		log.Debug("Consumer switch disabled by default.")
	}
}

// StartAllConsumers starts all registered consumers by launching their respective daemons
func StartAllConsumers(ctx context.Context) {
	registerMu.Lock()
	defer registerMu.Unlock()
	log := common.LoggerCtx(ctx)

	log.Info("Starting all registered consumers.", "count", len(consumers))
	if len(consumers) == 0 {
		log.Warn("No consumers registered to start.")
		return
	}

	for _, proxy := range consumers {
		go proxy.StartDaemon(ctx)
	}
}

// SetConsumerSwitch enables or disables a consumer by job name
func SetConsumerSwitch(jobName string, start bool) {
	common.Logger().Info("Setting consumer switch.", "jobName", jobName, "enabled", start)
	switchMap.Store(jobName, start)
}

// GetConsumerSwitch returns the current state of a consumer switch
func GetConsumerSwitch(jobName string) bool {
	if val, ok := switchMap.Load(jobName); ok {
		if start, typeOK := val.(bool); typeOK {
			return start
		}
		common.Logger().Warn("Invalid type for consumer switch in map.", "jobName", jobName, "type", fmt.Sprintf("%T", val))
	}
	return false
}

// RegisterProducer creates a new managed producer with the given config
// The producer's lifecycle is tied to the provided context and it contributes to the global WaitGroup
func RegisterProducer(ctx context.Context, config ProducerConfig) (IProducer, error) {
	log := common.LoggerCtx(ctx)
	log.Info("Registering new managed producer.", "endpoint", config.Endpoint, "topics", config.Topics)

	p, err := createProducer(ctx, config)
	if err != nil {
		return nil, err
	}

	// Register producer in global map
	producersMu.Lock()
	producers[p.GetID()] = p
	producersMu.Unlock()

	globalWg.Add(1)
	log.Info("Managed producer registered and started successfully.", "id", p.GetID())
	return p, nil
}

// WaitStop blocks until all registered consumers and managed producers have stopped
// This is typically called at the end of the application's main function for graceful shutdown
func WaitStop() {
	log := common.Logger()
	log.Info("Waiting for all consumers and producers to stop...")

	// First copy the producers list to avoid calling Close while holding the lock
	var producersToClose []IProducer

	producersMu.RLock()
	for _, p := range producers {
		producersToClose = append(producersToClose, p)
	}
	producersMu.RUnlock()

	// Close all producers (without holding the lock)
	log.Debug("Closing all producers...", "count", len(producersToClose))
	for _, p := range producersToClose {
		// The Close method will handle logging internally
		_ = p.Close()
	}

	// Wait for all consumer and producer goroutines to exit
	log.Debug("Waiting for all goroutines to exit...")
	globalWg.Wait()
	log.Info("All consumers and producers have stopped.")
	zlogger.Sync()
}

// removeProducer removes a producer from the global map
// This function is called in producer.Close()
func removeProducer(id string) {
	producersMu.Lock()
	delete(producers, id)
	producersMu.Unlock()
}

// createProducer is an internal function that initializes and starts a RocketMQ producer client
// It also sets up a goroutine to close the producer when the parent context is cancelled
func createProducer(ctx context.Context, config ProducerConfig) (IProducer, error) {
	log := common.LoggerCtx(ctx)
	rmqConfig := config.toRocketMQConfig()

	log.Debug("Creating RocketMQ producer client.", "endpoint", config.Endpoint, "topics", config.Topics)
	rmqProducer, err := rmq_client.NewProducer(rmqConfig, rmq_client.WithTopics(config.Topics...))
	if err != nil {
		log.Error("Failed to create RocketMQ producer client instance.", "error", err, "endpoint", config.Endpoint)
		return nil, fmt.Errorf("failed to create RocketMQ producer client for endpoint %s: %w", config.Endpoint, err)
	}

	log.Debug("Starting RocketMQ producer client.", "endpoint", config.Endpoint)
	err = rmqProducer.Start()
	if err != nil {
		log.Error("Failed to start RocketMQ producer client instance.", "error", err, "endpoint", config.Endpoint)
		return nil, fmt.Errorf("failed to start RocketMQ producer for endpoint %s: %w", config.Endpoint, err)
	}

	log.Info("RocketMQ producer client created and started successfully.", "endpoint", config.Endpoint)
	p := &producer{
		client: rmqProducer,
		wg:     &globalWg,
		id:     fmt.Sprintf("producer-%s", common.GenLogID()),
	}

	return p, nil
}
