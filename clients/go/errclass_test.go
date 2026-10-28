package ruvio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestIsTransientCoversOutagesAndRetryableReplies(t *testing.T) {
	for _, msg := range []string{
		"LOADING Ruvio is loading the dataset in memory",
		"MASTERDOWN Link with MASTER is down",
		"CLUSTERDOWN The cluster is down",
		"TRYAGAIN slot is moving",
		"READONLY You can't write against a read only replica.",
		"NOREPLICAS Not enough good replicas to write.",
	} {
		if !IsTransient(&Error{Msg: msg}) {
			t.Errorf("%q should be transient", msg)
		}
		if !IsTransient(fmt.Errorf("wrapped: %w", &Error{Msg: msg})) {
			t.Errorf("wrapped %q should be transient", msg)
		}
	}
	for _, msg := range []string{"WRONGTYPE Operation against a key", "OOM command not allowed", "ERR syntax error", "READONLYX x", "LOAD"} {
		if IsTransient(&Error{Msg: msg}) {
			t.Errorf("%q should not be transient", msg)
		}
	}
	transient := []error{
		context.DeadlineExceeded,
		ErrClosed,
		&connError{syscall.ECONNRESET},
		&connError{ErrProtocol},
		&connectError{&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}},
		io.EOF,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Err: syscall.ECONNRESET},
	}
	for _, err := range transient {
		if !IsTransient(err) {
			t.Errorf("%v should be transient", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, ErrProtocol, ErrCrossSlot, errors.New("bad port")} {
		if IsTransient(err) {
			t.Errorf("%v should not be transient", err)
		}
	}
}

func TestMayHaveRunIsFalseOnlyForARefusingReply(t *testing.T) {
	for _, msg := range []string{"WRONGTYPE Operation against a key", "TRYAGAIN slot is moving", "READONLY replica"} {
		if MayHaveRun(&Error{Msg: msg}) {
			t.Errorf("%q surely did not run", msg)
		}
	}
	for _, err := range []error{
		&Error{Msg: "NOREPLICAS Not enough good replicas to write."},
		&Error{Msg: "NOREPLICAS"},
		context.DeadlineExceeded,
		context.Canceled,
		&connError{syscall.ECONNRESET},
		ErrProtocol,
	} {
		if !MayHaveRun(err) {
			t.Errorf("%v may have run", err)
		}
	}
	if MayHaveRun(nil) {
		t.Error("nil did not fail")
	}
}
