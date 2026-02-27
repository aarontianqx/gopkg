package errext

// StatusCode is a protocol-agnostic status code for business errors.
//
// Values are aligned with the gRPC/Connect canonical code space (0–16)
// so that framework adapters can map directly via type conversion,
// but this type carries no dependency on any RPC framework.
type StatusCode uint32

const (
	StatusCodeOK                 StatusCode = 0
	StatusCodeCanceled           StatusCode = 1
	StatusCodeUnknown            StatusCode = 2
	StatusCodeInvalidArgument    StatusCode = 3
	StatusCodeDeadlineExceeded   StatusCode = 4
	StatusCodeNotFound           StatusCode = 5
	StatusCodeAlreadyExists      StatusCode = 6
	StatusCodePermissionDenied   StatusCode = 7
	StatusCodeResourceExhausted  StatusCode = 8
	StatusCodeFailedPrecondition StatusCode = 9
	StatusCodeAborted            StatusCode = 10
	StatusCodeOutOfRange         StatusCode = 11
	StatusCodeUnimplemented      StatusCode = 12
	StatusCodeInternal           StatusCode = 13
	StatusCodeUnavailable        StatusCode = 14
	StatusCodeDataLoss           StatusCode = 15
	StatusCodeUnauthenticated    StatusCode = 16
)

var statusCodeNames = map[StatusCode]string{
	StatusCodeOK:                 "OK",
	StatusCodeCanceled:           "Canceled",
	StatusCodeUnknown:            "Unknown",
	StatusCodeInvalidArgument:    "InvalidArgument",
	StatusCodeDeadlineExceeded:   "DeadlineExceeded",
	StatusCodeNotFound:           "NotFound",
	StatusCodeAlreadyExists:      "AlreadyExists",
	StatusCodePermissionDenied:   "PermissionDenied",
	StatusCodeResourceExhausted:  "ResourceExhausted",
	StatusCodeFailedPrecondition: "FailedPrecondition",
	StatusCodeAborted:            "Aborted",
	StatusCodeOutOfRange:         "OutOfRange",
	StatusCodeUnimplemented:      "Unimplemented",
	StatusCodeInternal:           "Internal",
	StatusCodeUnavailable:        "Unavailable",
	StatusCodeDataLoss:           "DataLoss",
	StatusCodeUnauthenticated:    "Unauthenticated",
}

func (c StatusCode) String() string {
	if name, ok := statusCodeNames[c]; ok {
		return name
	}
	return "Unknown"
}

// HTTPStatus returns the conventional HTTP status code mapping.
func (c StatusCode) HTTPStatus() int {
	switch c {
	case StatusCodeOK:
		return 200
	case StatusCodeCanceled:
		return 499
	case StatusCodeUnknown:
		return 500
	case StatusCodeInvalidArgument:
		return 400
	case StatusCodeDeadlineExceeded:
		return 504
	case StatusCodeNotFound:
		return 404
	case StatusCodeAlreadyExists:
		return 409
	case StatusCodePermissionDenied:
		return 403
	case StatusCodeResourceExhausted:
		return 429
	case StatusCodeFailedPrecondition:
		return 412
	case StatusCodeAborted:
		return 409
	case StatusCodeOutOfRange:
		return 400
	case StatusCodeUnimplemented:
		return 501
	case StatusCodeInternal:
		return 500
	case StatusCodeUnavailable:
		return 503
	case StatusCodeDataLoss:
		return 500
	case StatusCodeUnauthenticated:
		return 401
	default:
		return 500
	}
}
