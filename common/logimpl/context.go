package logimpl

import "context"

// contextKeyType is a custom type for context keys to avoid collisions
type contextKeyType int

// context keys
const (
	baseLogInfoKey contextKeyType = iota // Key for BaseLogInfo struct
)

// Keys for log attributes
const (
	keyTraceID       = "traceid"
	keySpanID        = "spanid"
	keyRequestID     = "requestid"
	keyJobName       = "jobname"
	keyTopic         = "topic"
	keyConsumerGroup = "consumergroup"
	keyMessageID     = "messageid"
	keyPartition     = "partition"
	keyOffset        = "offset"
)

// BaseLogInfo holds common logging attributes managed by this package.
type BaseLogInfo struct {
	RequestID     string
	JobName       string
	Topic         string
	ConsumerGroup string
	MessageID     string
	Partition     int32
	Offset        int64
}

// ContextWithBaseLogInfo adds BaseLogInfo to a context.
func ContextWithBaseLogInfo(ctx context.Context, info *BaseLogInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, baseLogInfoKey, *info)
}

// BaseLogInfoFromContext retrieves the BaseLogInfo from context.
// Returns nil if context is nil, BaseLogInfo is not found, or type assertion fails.
func BaseLogInfoFromContext(ctx context.Context) *BaseLogInfo {
	if ctx == nil {
		return nil
	}
	info, ok := ctx.Value(baseLogInfoKey).(BaseLogInfo)
	if !ok {
		return nil
	}
	result := new(BaseLogInfo)
	*result = info
	return result
}
