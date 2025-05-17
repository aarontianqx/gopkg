// Package cron provides a flexible cron job scheduling system with support for
// multiple workers per job, dynamic configuration updates, and graceful shutdown.
package cron

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aarontianqx/gopkg/common"
	"github.com/robfig/cron/v3"
)

var (
	// DefaultScheduler uses minute-level precision.
	DefaultScheduler = NewScheduler(false)
)

// Scheduler defines the interface for managing cron jobs.
type Scheduler interface {
	// RegisterJob adds a new job to the scheduler with the given configuration.
	// If a job with the same name already exists, it logs a warning and does nothing.
	RegisterJob(jobName string, config Config, jobFunc JobFunc) error
	// SetConfigFunc sets a function that the scheduler daemon will periodically call
	// to retrieve updated configurations for registered jobs.
	SetConfigFunc(f ConfigFunc)
	// Start starts the underlying cron engine and the dynamic configuration update daemon.
	// The daemon's lifecycle is tied to the provided context.
	Start(ctx context.Context)
	// WaitStop waits for the configuration update daemon to exit and for the underlying
	// cron engine to stop gracefully, ensuring all running jobs complete.
	WaitStop()
	// SyncConfig manually triggers an update for a specific job's configuration (spec or worker number).
	SyncConfig(ctx context.Context, jobName string, config Config) error
}

// NewScheduler creates a new Scheduler instance.
// Set withSeconds to true for second-level precision.
func NewScheduler(withSeconds bool) Scheduler {
	s := &scheduler{
		daemonOnce: sync.Once{},
		stopOnce:   sync.Once{},
		regMutex:   sync.Mutex{},
		managerMap: make(map[string]*jobManager),
		configFunc: nil,
	}
	if withSeconds {
		s.c = cron.New(cron.WithSeconds())
	} else {
		s.c = cron.New()
	}
	return s
}

// scheduler implements the Scheduler interface.
type scheduler struct {
	c          *cron.Cron     // Underlying cron engine.
	daemonOnce sync.Once      // Ensures background task starts only once.
	stopOnce   sync.Once      // Ensures cron engine stops only once.
	daemonWg   sync.WaitGroup // Waits for background task exit.
	regMutex   sync.Mutex     // Protects managerMap and configFunc.
	managerMap map[string]*jobManager
	configFunc ConfigFunc // Optional function for dynamic config updates.
}

// RegisterJob implements the Scheduler interface.
func (s *scheduler) RegisterJob(jobName string, config Config, jobFunc JobFunc) error {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	if _, ok := s.managerMap[jobName]; ok {
		common.Logger().Warn("Job already registered, ignoring registration.", "job_name", jobName)
		return nil
	}
	manager := &jobManager{
		c:        s.c,
		jobName:  jobName,
		jobFunc:  jobFunc,
		config:   config,
		entryMap: make(map[string]cron.EntryID),
	}
	s.managerMap[jobName] = manager
	return manager.syncWorkerNum()
}

// SetConfigFunc implements the Scheduler interface.
func (s *scheduler) SetConfigFunc(f ConfigFunc) {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()
	s.configFunc = f
}

// Start implements the Scheduler interface.
func (s *scheduler) Start(ctx context.Context) {
	s.daemonOnce.Do(func() {
		s.c.Start()
		common.LoggerCtx(ctx).Debug("Underlying cron job runner started")
		s.daemonWg.Add(1) // Account for the startDaemon goroutine
		go s.startDaemon(ctx)
	})
}

// WaitStop implements the Scheduler interface.
func (s *scheduler) WaitStop() {
	log := common.Logger()
	log.Debug("WaitStop called. Waiting for scheduler background task to exit...")
	s.daemonWg.Wait() // Wait for startDaemon goroutine to finish.
	log.Debug("Scheduler background task exited.")

	// Initiate stop of the underlying cron engine (exactly once)
	// and wait for it to complete.
	var stopCtx context.Context
	s.stopOnce.Do(func() {
		log.Debug("Initiating shutdown of underlying cron job runner...")
		stopCtx = s.c.Stop()
		log.Debug("Underlying cron job runner stop initiated.")
	})

	if stopCtx == nil {
		// This case handles potential misuse (e.g., WaitStop called multiple times
		// or without Start), ensuring stopOnce is checked defensively.
		log.Warn("WaitStop called, but stop context from cron engine is nil (likely already stopped or never started properly). Skipping wait.")
		s.stopOnce.Do(func() {
			log.Warn("stopOnce.Do called defensively within nil stopCtx block in WaitStop.")
		})
		return
	}

	log.Debug("Waiting for active cron jobs to complete...")
	<-stopCtx.Done() // Wait for s.c.Stop() to complete.
	log.Info("Underlying cron job runner stopped gracefully.")
}

// SyncConfig implements the Scheduler interface.
func (s *scheduler) SyncConfig(ctx context.Context, jobName string, config Config) error {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	manager, ok := s.managerMap[jobName]
	if !ok {
		return errors.New("job not found: " + jobName)
	}

	return manager.updateConfig(ctx, config)
}

// startDaemon runs a background loop that primarily handles graceful shutdown listening.
// If a ConfigFunc is provided via SetConfigFunc, it also periodically checks for
// and applies dynamic configuration updates.
// It exits when the provided context is cancelled.
func (s *scheduler) startDaemon(ctx context.Context) {
	defer s.daemonWg.Done()
	defer common.Recovery(ctx)

	log := common.LoggerCtx(ctx)
	log.Debug("Scheduler background task started.")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			jobNames, f := s.getJobNamesAndConfigFunc()
			if f != nil { // Only check for updates if a ConfigFunc is actually set
				log.Debug("Checking for dynamic config updates...")
				for _, jobName := range jobNames {
					if config := f(jobName); config != nil {
						log.Info("Applying dynamic config update", "job_name", jobName, "new_spec", config.Spec, "new_workers", config.WorkerNum)
						err := s.SyncConfig(ctx, jobName, *config)
						if err != nil {
							log.Error("Failed to sync dynamic job config", "error", err, "job_name", jobName)
						}
					}
				}
			}
		case <-ctx.Done():
			log.Debug("Context cancelled, stopping scheduler background task...")
			return // Exit goroutine
		}
	}
}

// getJobNamesAndConfigFunc safely retrieves the list of registered job names and the config function.
func (s *scheduler) getJobNamesAndConfigFunc() ([]string, ConfigFunc) {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	jobNames := make([]string, 0, len(s.managerMap))
	for jobName := range s.managerMap {
		jobNames = append(jobNames, jobName)
	}
	return jobNames, s.configFunc
}
