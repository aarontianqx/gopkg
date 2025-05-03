package cron

import (
	"context"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"github.com/aarontianqx/gopkg/random"
	"github.com/robfig/cron/v3"
)

// JobFunc function to run in each job instance
type JobFunc func(ctx context.Context) error

// manager for a jobName
type jobManager struct {
	c        *cron.Cron
	jobName  string
	jobFunc  JobFunc
	config   Config
	entryMap map[string]cron.EntryID
}

// update spec/worker_num for a job
func (m *jobManager) updateConfig(ctx context.Context, config Config) error {
	if config.WorkerNum <= 0 {
		config.WorkerNum = 0
	}

	if config.Spec != m.config.Spec {
		m.config = config

		common.LoggerCtx(ctx).Info("job spec changed, restart all workers",
			"job_name", m.jobName)
		// spec changed, stop all jobs first and restart
		m.clearJobs()
		return m.syncWorkerNum()
	} else if config.WorkerNum != m.config.WorkerNum {
		m.config = config

		common.LoggerCtx(ctx).Info("job worker_num changed",
			"job_name", m.jobName)
		return m.syncWorkerNum()
	}

	// no change
	return nil
}

// unsafe add/remove jobs to keep worker_num
func (m *jobManager) syncWorkerNum() error {
	for len(m.entryMap) < m.config.WorkerNum {
		key := random.AlphanumericString(10)
		if _, ok := m.entryMap[key]; ok {
			continue
		}
		entryID, err := m.c.AddJob(m.config.Spec, m.wrappedJob(key))
		if err != nil {
			return err
		}
		m.entryMap[key] = entryID
	}

	if toClose := len(m.entryMap) - m.config.WorkerNum; toClose > 0 {
		for key, entryID := range m.entryMap {
			m.c.Remove(entryID)
			delete(m.entryMap, key)
			toClose--
			if toClose <= 0 {
				break
			}
		}
	}
	return nil
}

// unsafe clear jobs
func (m *jobManager) clearJobs() {
	for key, entryID := range m.entryMap {
		m.c.Remove(entryID)
		delete(m.entryMap, key)
	}
}

// transform JobFunc to cron/v3.Job
func (m *jobManager) wrappedJob(key string) cron.Job {
	return cron.FuncJob(func() {
		var (
			err error
			ctx = logimpl.ContextWithBaseLogInfo(context.Background(), &logimpl.BaseLogInfo{
				RequestID: common.GenLogID(),
				JobName:   m.jobName,
			})
		)

		defer func() {
			if err != nil {
				common.LoggerCtx(ctx).Error("job execution failed", "error", err, "key", key)
			}
		}()
		defer common.Recover(ctx, &err)
		//log.Info("run job", "job_name", m.jobName, "key", key)
		err = m.jobFunc(ctx)
	})
}
