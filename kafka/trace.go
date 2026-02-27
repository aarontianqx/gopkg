package kafka

import (
	"bytes"
	"context"

	"github.com/IBM/sarama"
	"github.com/aarontianqx/gopkg/common/logimpl"
	"go.opentelemetry.io/otel/trace"
)

const (
	propertyTraceID   = "x-trace-id"
	propertySpanID    = "x-span-id"
	propertyRequestID = "x-request-id"
)

func setMessageHeader(msg *sarama.ProducerMessage, key, value string) {
	if msg == nil || key == "" || value == "" {
		return
	}
	for i := range msg.Headers {
		if bytes.Equal(msg.Headers[i].Key, []byte(key)) {
			msg.Headers[i].Value = []byte(value)
			return
		}
	}
	msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
}

func getMessageHeader(msg *sarama.ConsumerMessage, key string) string {
	if msg == nil || key == "" {
		return ""
	}
	for _, h := range msg.Headers {
		if bytes.Equal(h.Key, []byte(key)) {
			return string(h.Value)
		}
	}
	return ""
}

// injectTraceToMessage injects tracing and request IDs into Kafka headers.
func injectTraceToMessage(ctx context.Context, msg *sarama.ProducerMessage) {
	if ctx == nil || msg == nil {
		return
	}

	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		setMessageHeader(msg, propertyTraceID, spanCtx.TraceID().String())
	}
	if spanCtx.HasSpanID() {
		setMessageHeader(msg, propertySpanID, spanCtx.SpanID().String())
	}

	if info := logimpl.BaseLogInfoFromContext(ctx); info != nil && info.RequestID != "" {
		setMessageHeader(msg, propertyRequestID, info.RequestID)
	}
}

// extractTraceFromMessage restores remote tracing context from Kafka headers.
func extractTraceFromMessage(ctx context.Context, msg *sarama.ConsumerMessage) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if msg == nil {
		return ctx
	}

	traceIDStr := getMessageHeader(msg, propertyTraceID)
	spanIDStr := getMessageHeader(msg, propertySpanID)
	if traceIDStr == "" || spanIDStr == "" {
		return ctx
	}

	traceID, traceErr := trace.TraceIDFromHex(traceIDStr)
	spanID, spanErr := trace.SpanIDFromHex(spanIDStr)
	if traceErr != nil || spanErr != nil {
		return ctx
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		Remote:     true,
		TraceFlags: trace.FlagsSampled,
	})
	ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	return ctx
}
