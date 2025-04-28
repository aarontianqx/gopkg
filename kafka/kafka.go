package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/IBM/sarama"

	"github.com/aarontianqx/gopkg/common"
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
// If defaultStart is true, the consumer switch will be enabled, allowing it to start when StartAllConsumers is called
func RegisterConsumer(ctx context.Context, consumer IConsumer, defaultStart bool) {
	config := consumer.GetConfig()

	common.LoggerCtx(ctx).Debug("Registering Sarama consumer", "job_name", config.JobName, "auto_start", defaultStart)
	brokers := strings.Split(config.BootstrapServers, ",")
	topics := []string{config.Topic}

	proxy := &consumerProxy{
		jobName:       config.JobName,
		brokers:       brokers,
		topics:        topics,
		consumerGroup: config.ConsumerGroup,
		saramaConfig:  config.toSaramaConfig(),
		handler:       consumer.Handle,
	}

	registerMu.Lock()
	consumers[config.JobName] = proxy
	registerMu.Unlock()

	// Set consumer switch based on defaultStart parameter
	if defaultStart {
		SetConsumerSwitch(config.JobName, true)
	}
}

// StartAllConsumers starts all registered consumers
func StartAllConsumers(ctx context.Context) {
	registerMu.Lock()
	defer registerMu.Unlock()

	for _, proxy := range consumers {
		// Start the consumer daemon in a goroutine
		go proxy.StartDaemon(ctx)
	}
}

// SetConsumerSwitch enables or disables a consumer by job name
func SetConsumerSwitch(jobName string, start bool) {
	switchMap.Store(jobName, start)
}

// GetConsumerSwitch returns the current state of a consumer
func GetConsumerSwitch(jobName string) bool {
	var start bool
	if val, ok := switchMap.Load(jobName); !ok {
		return false
	} else if start, ok = val.(bool); !ok {
		return false
	}
	return start
}

// RegisterProducer creates and registers a producer with the given config
func RegisterProducer(ctx context.Context, config ProducerConfig) (IProducer, error) {
	log := common.LoggerCtx(ctx)

	// Check if producer types are set and apply defaults
	enableSync := true   // Default: enable sync producer
	enableAsync := false // Default: disable async producer

	// If explicitly set, use the set value
	if config.EnableSyncProducer != nil {
		enableSync = *config.EnableSyncProducer
	}

	if config.EnableAsyncProducer != nil {
		enableAsync = *config.EnableAsyncProducer
	}

	// If both producer types are disabled, default to sync producer
	if !enableSync && !enableAsync {
		enableSync = true
		log.Warn("Neither sync nor async producer enabled, defaulting to sync producer")
	}

	// Convert boolean values back to config to pass to createProducer
	configCopy := config
	configCopy.EnableSyncProducer = BoolPtr(enableSync)
	configCopy.EnableAsyncProducer = BoolPtr(enableAsync)

	p, err := createProducer(ctx, configCopy)
	if err != nil {
		return nil, err
	}

	// Register in global map
	producersMu.Lock()
	producers[p.GetID()] = p
	producersMu.Unlock()
	globalWg.Add(1)

	log.Debug("Kafka producer registered", "id", p.GetID(),
		"sync_enabled", enableSync, "async_enabled", enableAsync)
	return p, nil
}

// removeProducer removes a producer from the global map
// This function is called in producer.Close()
func removeProducer(id string) {
	producersMu.Lock()
	delete(producers, id)
	producersMu.Unlock()
}

// WaitStop blocks until all consumers and producers have stopped
func WaitStop() {
	// First copy the producers list to avoid calling Close while holding the lock
	var producersToClose []IProducer

	producersMu.RLock()
	for _, p := range producers {
		producersToClose = append(producersToClose, p)
	}
	producersMu.RUnlock()

	// Close all producers (without holding the lock)
	common.Logger().Debug("Closing all producers...", "count", len(producersToClose))
	for _, p := range producersToClose {
		// The Close method will handle logging internally
		_ = p.Close()
	}

	// Wait for all consumer and producer goroutines to exit
	common.Logger().Debug("Waiting for all goroutines to exit...")
	globalWg.Wait()
	common.Logger().Info("All consumers and producers stopped")
}

// createProducer creates a producer with the given config (internal function)
func createProducer(ctx context.Context, config ProducerConfig) (IProducer, error) {
	log := common.LoggerCtx(ctx)

	brokers := strings.Split(config.BootstrapServers, ",")
	saramaConfig := config.toSaramaConfig()

	// Generate a producer ID
	producerID := fmt.Sprintf("producer-%s", common.GenLogID())

	// Create producers based on config
	var syncProducer sarama.SyncProducer
	var asyncProducer sarama.AsyncProducer
	var err error

	// Check if sync producer should be created
	enableSync := config.EnableSyncProducer != nil && *config.EnableSyncProducer
	if enableSync {
		syncProducer, err = sarama.NewSyncProducer(brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync producer: %w", err)
		}
	}

	// Check if async producer should be created
	enableAsync := config.EnableAsyncProducer != nil && *config.EnableAsyncProducer
	if enableAsync {
		asyncProducer, err = sarama.NewAsyncProducer(brokers, saramaConfig)
		if err != nil {
			// Close sync producer if already created
			if syncProducer != nil {
				_ = syncProducer.Close()
			}
			return nil, fmt.Errorf("failed to create async producer: %w", err)
		}
	}

	// Create the producer
	p := &producer{
		id:            producerID,
		brokers:       brokers,
		saramaConfig:  saramaConfig,
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		closeChan:     make(chan struct{}),
	}

	// Start error handler goroutine if async producer is enabled
	if asyncProducer != nil {
		go p.handleErrors(ctx)
	}

	log.Debug("Kafka producer initialized", "id", producerID, "brokers", brokers)
	return p, nil
}
