package rocketmq

import (
	"context"

	"github.com/aarontianqx/gopkg/common"
	"github.com/aarontianqx/gopkg/common/logimpl"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"go.opentelemetry.io/otel/trace"
)

// Standard property keys used to propagate tracing and logging information.
// These keys align with the logimpl package so that logs and message metadata stay consistent.
const (
	propertyTraceID   = "x-trace-id"   // OpenTelemetry Trace ID
	propertySpanID    = "x-span-id"    // OpenTelemetry Span ID
	propertyRequestID = "x-request-id" // Request ID from BaseLogInfo
)

// injectTraceToMessage injects tracing and basic logging identifiers from context into
// the RocketMQ message properties, so that downstream consumers can restore them.
func injectTraceToMessage(ctx context.Context, msg *rmq_client.Message) {
	if ctx == nil || msg == nil {
		return
	}

	// OpenTelemetry trace context
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		msg.AddProperty(propertyTraceID, spanCtx.TraceID().String())
	}
	if spanCtx.HasSpanID() {
		msg.AddProperty(propertySpanID, spanCtx.SpanID().String())
	}

	// Request-scoped identifiers from BaseLogInfo
	if info := logimpl.BaseLogInfoFromContext(ctx); info != nil {
		if info.RequestID != "" {
			msg.AddProperty(propertyRequestID, info.RequestID)
		}
	}
}

// buildConsumeContext constructs the context used for a single message consumption.
// It restores OpenTelemetry trace information from message properties, builds BaseLogInfo
// with RocketMQ-specific attributes (topic, consumer group, message ID, etc.), and injects
// everything into the context in one pass.
func buildConsumeContext(proxy *consumerProxy, msg *rmq_client.MessageView) context.Context {
	ctx := context.Background()

	if msg == nil {
		return ctx
	}

	props := msg.GetProperties()

	// Extract trace/span IDs and restore remote span context if valid.
	traceIDStr := props[propertyTraceID]
	spanIDStr := props[propertySpanID]
	if traceIDStr != "" && spanIDStr != "" {
		tid, tidErr := trace.TraceIDFromHex(traceIDStr)
		sid, sidErr := trace.SpanIDFromHex(spanIDStr)
		if tidErr == nil && sidErr == nil {
			sc := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     sid,
				Remote:     true,
				TraceFlags: trace.FlagsSampled,
			})
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}

	// Build BaseLogInfo: prefer propagated request ID, otherwise generate a new one.
	requestID := props[propertyRequestID]
	if requestID == "" {
		requestID = common.GenLogID()
	}

	info := &logimpl.BaseLogInfo{
		RequestID:     requestID,
		JobName:       proxy.jobName,
		Topic:         msg.GetTopic(),
		ConsumerGroup: proxy.config.ConsumerGroup,
		MessageID:     msg.GetMessageId(),
	}

	return logimpl.ContextWithBaseLogInfo(ctx, info)
}
