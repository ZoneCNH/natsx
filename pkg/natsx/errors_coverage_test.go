package natsx

import (
	"errors"
	"fmt"
	"testing"
)

// TestNewError covers NewError field assignment, Error() text, and IsKind.
func TestNewError(t *testing.T) {
	// Arrange / Act
	e := NewError(ErrorKindValidation, "natsx.test", "invalid input", false)

	// Assert - fields
	if e.Kind != ErrorKindValidation {
		t.Fatalf("Kind = %q, want validation", e.Kind)
	}
	if e.Op != "natsx.test" {
		t.Fatalf("Op = %q, want natsx.test", e.Op)
	}
	if e.Message != "invalid input" {
		t.Fatalf("Message = %q, want invalid input", e.Message)
	}
	if e.Retryable {
		t.Fatal("Retryable = true, want false")
	}
	if e.Cause != nil {
		t.Fatalf("Cause = %v, want nil", e.Cause)
	}

	// Assert - Error() text contains kind, op, message
	wantText := "validation: natsx.test: invalid input"
	if e.Error() != wantText {
		t.Fatalf("Error() = %q, want %q", e.Error(), wantText)
	}

	// Assert - IsKind matches
	if !IsKind(e, ErrorKindValidation) {
		t.Fatal("IsKind(validation) = false, want true")
	}
}

// TestErrorUnwrap covers nil receiver and cause propagation through errors.Is.
func TestErrorUnwrap(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		// Arrange
		var e *Error

		// Act
		cause := e.Unwrap()

		// Assert
		if cause != nil {
			t.Fatalf("nil receiver Unwrap() = %v, want nil", cause)
		}
	})

	t.Run("cause propagates through errors.Is", func(t *testing.T) {
		// Arrange
		sentinel := errors.New("sentinel cause")
		e := WrapError(ErrorKindConnection, "natsx.test", "", true, sentinel)

		// Act
		cause := e.Unwrap()

		// Assert
		if cause != sentinel {
			t.Fatalf("Unwrap() = %v, want sentinel", cause)
		}
		if !errors.Is(e, sentinel) {
			t.Fatal("errors.Is(e, sentinel) = false, want true")
		}
	})
}

// TestErrorError covers nil receiver, message set, and message-empty with
// cause set (the else-if branch).
func TestErrorError(t *testing.T) {
	t.Run("nil receiver returns empty", func(t *testing.T) {
		// Arrange
		var e *Error

		// Act
		got := e.Error()

		// Assert
		if got != "" {
			t.Fatalf("nil receiver Error() = %q, want empty", got)
		}
	})

	t.Run("message set renders full text", func(t *testing.T) {
		// Arrange
		e := NewError(ErrorKindTimeout, "natsx.test", "deadline exceeded", true)

		// Act
		got := e.Error()

		// Assert
		want := "timeout: natsx.test: deadline exceeded"
		if got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("empty message with cause renders cause text", func(t *testing.T) {
		// Arrange - build manually so Message stays empty while Cause is set,
		// exercising the `else if e.Cause != nil` branch.
		cause := fmt.Errorf("underlying failure")
		e := &Error{
			Kind:  ErrorKindConnection,
			Op:    "natsx.test",
			Cause: cause,
		}

		// Act
		got := e.Error()

		// Assert
		if got == "" {
			t.Fatal("Error() = empty, want non-empty cause text")
		}
		want := "connection: natsx.test: underlying failure"
		if got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})
}

// TestIsKindCoversMismatchAndNonErrorType covers cross-kind mismatch and
// non-*Error argument.
func TestIsKindCoversMismatchAndNonErrorType(t *testing.T) {
	t.Run("different kind returns false", func(t *testing.T) {
		// Arrange
		e := NewError(ErrorKindValidation, "natsx.test", "msg", false)

		// Act / Assert
		if IsKind(e, ErrorKindTimeout) {
			t.Fatal("IsKind(validation, timeout) = true, want false")
		}
	})

	t.Run("non error type returns false", func(t *testing.T) {
		// Arrange
		plain := errors.New("plain error")

		// Act / Assert
		if IsKind(plain, ErrorKindValidation) {
			t.Fatal("IsKind(plain, validation) = true, want false")
		}
	})
}
