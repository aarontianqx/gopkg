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
	DefaultScheduler = NewScheduler()
)

type Scheduler interface {
	RegisterJob(jobName string, config Config, jobFunc JobFunc) error
	SetConfigFunc(f ConfigFunc)
	Start(ctx context.Context)
	Stop(ctx context.Context)
	SyncConfig(ctx context.Context, jobName string, config Config) error
}

func NewScheduler() Scheduler {
	return &scheduler{
		c:          cron.New(),
		daemonOnce: sync.Once{},
		finish:     make(chan bool, 1),
		regMutex:   sync.Mutex{},
		managerMap: make(map[string]*jobManager),
		configFunc: nil,
	}
}

type scheduler struct {
	c          *cron.Cron
	daemonOnce sync.Once
	finish     chan bool
	regMutex   sync.Mutex
	managerMap map[string]*jobManager
	configFunc ConfigFunc
}

func (s *scheduler) RegisterJob(jobName string, config Config, jobFunc JobFunc) error {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	if _, ok := s.managerMap[jobName]; ok {
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

// SetConfigFunc for dynamically updating job config
func (s *scheduler) SetConfigFunc(f ConfigFunc) {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	s.configFunc = f
}

// Start schedule all jobs and start daemon
func (s *scheduler) Start(ctx context.Context) {
	s.c.Start()
	s.daemonOnce.Do(func() {
		go s.startDaemon()
	})
	common.Logger(ctx).Infof("all jobs started")
}

// Stop wait for all jobs to stop
func (s *scheduler) Stop(ctx context.Context) {
	s.finish <- true
	<-s.c.Stop().Done()
	common.Logger(ctx).Infof("all jobs stopped")
}

// SyncConfig manually update spec/worker_num for a job
func (s *scheduler) SyncConfig(ctx context.Context, jobName string, config Config) error {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	manager, ok := s.managerMap[jobName]
	if !ok {
		return errors.New("job not found")
	}

	return manager.updateConfig(ctx, config)
}

func (s *scheduler) startDaemon() {
	ctx := context.Background()
	defer common.Recovery(ctx)

	log := common.Logger(ctx)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			jobNames, f := s.getJobNamesAndConfigFunc()
			if f != nil {
				for _, jobName := range jobNames {
					if config := f(jobName); config != nil {
						err := s.SyncConfig(ctx, jobName, *config)
						if err != nil {
							log.WithError(err).Errorf("sync job[%s] config failed", jobName)
						}
					}
				}
			}

		case <-s.finish:
			log.Infof("stop cron scheduler daemon")
			return
		}
	}
}

func (s *scheduler) getJobNamesAndConfigFunc() ([]string, ConfigFunc) {
	s.regMutex.Lock()
	defer s.regMutex.Unlock()

	var jobNames []string
	for jobName := range s.managerMap {
		jobNames = append(jobNames, jobName)
	}
	return jobNames, s.configFunc
}
