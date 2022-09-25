package cron

// Config running config for job
type Config struct {
	Spec      string // https://en.wikipedia.org/wiki/Cron
	WorkerNum int    // worker number on a single process instance
}

// ConfigFunc for dynamically updating job config
type ConfigFunc func(string) *Config
