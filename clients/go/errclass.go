package ruvio

import (
	"context"
	"errors"
	"io"
	"net"
)

// IsTransient reports whether the same call can succeed later without any change on the caller's
// side: Ruvio could not be reached, the connection failed or closed, the call timed out
// (context.DeadlineExceeded), or the server answered LOADING, MASTERDOWN, CLUSTERDOWN, TRYAGAIN,
// READONLY or NOREPLICAS. It is false for every other server reply (WRONGTYPE, OOM, a syntax error),
// for a cancelled context (context.Canceled is the caller's own doing) and for nil.
//
// The client already follows MOVED/ASK and waits out TRYAGAIN during a slot move; IsTransient judges
// what still reaches the caller, for a retry loop or policy of your own (failsafe-go, a loop). Retry
// only when it is true and the command is safe to repeat (a read, a SET of the same value, a command
// with an idempotency key) or MayHaveRun is false:
//
//	for attempt := 0; ; attempt++ {
//		n, err := client.Incr(ctx, "visits")
//		// INCR is not safe to repeat: retry it only when it surely did not run.
//		if err == nil || attempt == 3 || !ruvio.IsTransient(err) || ruvio.MayHaveRun(err) {
//			return n, err
//		}
//	}
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var reply *Error
	if errors.As(err, &reply) {
		switch reply.Prefix() {
		case "LOADING", "MASTERDOWN", "CLUSTERDOWN", "TRYAGAIN", "READONLY", "NOREPLICAS":
			return true
		}
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClosed) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) || isConnectError(err)
}

// MayHaveRun reports whether the failed call may have changed data, so repeating a write that is
// not safe to repeat could apply it twice. It is false only for a server error reply, which the
// server sends instead of running the command, and for nil. NOREPLICAS is the exception: the write
// was applied on the primary and only the replica confirmation timed out. A failed connection, a
// timeout or a cancellation after sending may have run.
func MayHaveRun(err error) bool {
	if err == nil {
		return false
	}
	var reply *Error
	if errors.As(err, &reply) {
		return reply.Prefix() == "NOREPLICAS"
	}
	return true
}
