package natsx

import (
	"context"
	"errors"
)

type ErrorKind string

const (
	ErrorKindConfig      ErrorKind = "config"
	ErrorKindValidation  ErrorKind = "validation"
	ErrorKindConnection  ErrorKind = "connection"
	ErrorKindUnavailable ErrorKind = "unavailable"
	ErrorKindTimeout     ErrorKind = "timeout"
	ErrorKindAuth        ErrorKind = "auth"
	ErrorKindConflict    ErrorKind = "conflict"
	ErrorKindInternal    ErrorKind = "internal"
)

type Error struct {
	Kind      ErrorKind
	Op        string
	Message   string
	Cause     error
	Retryable bool
}

func NewError(kind ErrorKind, op, message string, retryable bool) *Error {
	return wrapError(kind, op, message, retryable, nil)
}
func WrapError(kind ErrorKind, op, message string, retryable bool, cause error) *Error {
	return wrapError(kind, op, message, retryable, cause)
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := string(e.Kind)
	if e.Op != "" {
		s += ": " + e.Op
	}
	if e.Message != "" {
		s += ": " + e.Message
	} else if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func IsKind(err error, kind ErrorKind) bool {
	var target *Error
	return errors.As(err, &target) && target.Kind == kind
}

func wrapError(kind ErrorKind, op, message string, retryable bool, cause error) *Error {
	if message == "" && cause != nil {
		message = cause.Error()
	}
	return &Error{Kind: kind, Op: op, Message: message, Cause: cause, Retryable: retryable}
}
func validationError(op, message string, cause error) *Error {
	return wrapError(ErrorKindValidation, op, message, false, cause)
}
func contextError(op string, cause error) *Error {
	kind, retryable := ErrorKindUnavailable, false
	if errors.Is(cause, context.DeadlineExceeded) {
		kind, retryable = ErrorKindTimeout, true
	}
	return wrapError(kind, op, "", retryable, cause)
}
func connectionError(op string, cause error) *Error {
	return wrapError(ErrorKindConnection, op, "", true, cause)
}
func errorKind(err error) ErrorKind {
	var target *Error
	if errors.As(err, &target) {
		return target.Kind
	}
	return ErrorKindInternal
}
