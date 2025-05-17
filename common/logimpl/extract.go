package logimpl

import "context"

// ContextExtractorFunc extracts key-value pairs from a context
type ContextExtractorFunc func(ctx context.Context) []any

// defaultExtractors holds the registered context extractor functions
var defaultExtractors = []ContextExtractorFunc{extractBaseLogInfo}

// extractBaseLogInfo extracts common attributes from the BaseLogInfo struct in context.
func extractBaseLogInfo(ctx context.Context) []any {
	if ctx == nil {
		return nil
	}
	info, ok := ctx.Value(baseLogInfoKey).(BaseLogInfo)
	if !ok {
		return nil
	}
	var args []any
	if info.RequestID != "" {
		args = append(args, keyRequestID, info.RequestID)
	}
	if info.JobName != "" {
		args = append(args, keyJobName, info.JobName)
	}
	if info.Topic != "" {
		args = append(args, keyTopic, info.Topic)
	}
	if info.ConsumerGroup != "" {
		args = append(args, keyConsumerGroup, info.ConsumerGroup)
	}
	if info.MessageID != "" {
		args = append(args, keyMessageID, info.MessageID)
	}
	if info.Partition != 0 {
		args = append(args, keyPartition, info.Partition)
	}
	if info.Offset != 0 {
		args = append(args, keyOffset, info.Offset)
	}
	return args
}
