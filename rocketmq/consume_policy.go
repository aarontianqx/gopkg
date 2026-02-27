package rocketmq

import (
	"context"

	"github.com/aarontianqx/gopkg/errext"
)

// ConsumeErrorAction defines what to do when message handling returns an error.
type ConsumeErrorAction uint8

const (
	// ConsumeErrorActionRequeue keeps the message unacked so broker can redeliver it.
	ConsumeErrorActionRequeue ConsumeErrorAction = iota
	// ConsumeErrorActionAck acks the message even when handler returns error.
	ConsumeErrorActionAck
)

// ConsumeErrorClassifier decides consume action based on message handling error.
type ConsumeErrorClassifier func(ctx context.Context, msg *Message, err error) ConsumeErrorAction

// DefaultConsumeErrorClassifier keeps existing behavior: always requeue on handler error.
func DefaultConsumeErrorClassifier(_ context.Context, _ *Message, _ error) ConsumeErrorAction {
	return ConsumeErrorActionRequeue
}

// BizErrorNonRetryableClassifier classifies common non-retryable BizErrors to Ack.
// Unknown errors and retryable status codes are requeued.
func BizErrorNonRetryableClassifier(_ context.Context, _ *Message, err error) ConsumeErrorAction {
	if err == nil {
		return ConsumeErrorActionRequeue
	}
	bizErr, ok := errext.AsBizError(err)
	if !ok {
		return ConsumeErrorActionRequeue
	}

	switch bizErr.StatusCode() {
	case errext.StatusCodeInvalidArgument,
		errext.StatusCodeNotFound,
		errext.StatusCodeAlreadyExists,
		errext.StatusCodePermissionDenied,
		errext.StatusCodeFailedPrecondition,
		errext.StatusCodeOutOfRange,
		errext.StatusCodeUnimplemented,
		errext.StatusCodeUnauthenticated:
		return ConsumeErrorActionAck
	default:
		return ConsumeErrorActionRequeue
	}
}
