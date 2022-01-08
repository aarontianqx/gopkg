package common

const (
	KeyRequestID   = "request_id"
	KeyConsumerKey = "consumer_key"
	KeyCronProcess = "cron_process"
)

var loggerFields = map[string]bool{
	KeyRequestID:   true,
	KeyConsumerKey: true,
	KeyCronProcess: true,
}
