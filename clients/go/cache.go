// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrIdemPending is returned by Idempotent when another call with the same key is still
// running (or died before finishing). Retry later; the record expires after its ttl.
var ErrIdemPending = errors.New("ruvio: idempotent operation pending")

// ErrIdemConflict is returned by Idempotent when the key was already used with a
// different fingerprint, which usually means the caller reused an idempotency key for a
// different request.
var ErrIdemConflict = errors.New("ruvio: idempotency key reused with a different fingerprint")

// ---- cache-aside (FILLGET / FILLSET) ----------------------------------------------

// GetOrCreateOptions tunes GetOrCreate. Zero values pick the defaults.
type GetOrCreateOptions struct {
	// FillTimeout is how long the fill marker protects a filler (default 5s).
	FillTimeout time.Duration
	// Stale keeps the value readable this long after ttl (SWR); stale readers get the
	// old value and one of them refreshes in the background.
	Stale time.Duration
	// PollMin and PollMax bound the BUSY backoff (default 2ms .. 50ms).
	PollMin, PollMax time.Duration
}

// GetOrCreate returns the cached value of key, or computes it with factory exactly once
// across all callers (stampede protection), using FILLGET and FILLSET. ttl is how long
// the stored value stays fresh.
//
// On a miss one caller receives the fill token and runs factory; the others poll FILLGET
// with exponential backoff (PollMin to PollMax). It polls instead of using FILLGET WAIT
// because a blocking wait would park the shared connection. If Stale is set, a caller
// that finds a stale value gets it immediately and one of them refreshes it in the
// background.
//
// If factory fails, its error is returned and the fill marker lapses after FillTimeout,
// after which another caller may try. If storing the value fails, the computed value is
// returned together with the error. ctx cancellation stops the polling.
//
//	v, err := c.GetOrCreate(ctx, "report:today", time.Minute,
//	    func(ctx context.Context) (string, error) { return buildReport(ctx) })
func (c *cmds) GetOrCreate(ctx context.Context, key string, ttl time.Duration, factory func(ctx context.Context) (string, error), o ...GetOrCreateOptions) (string, error) {
	var opt GetOrCreateOptions
	if len(o) > 0 {
		opt = o[0]
	}
	if opt.FillTimeout <= 0 {
		opt.FillTimeout = 5 * time.Second
	}
	if opt.PollMin <= 0 {
		opt.PollMin = 2 * time.Millisecond
	}
	if opt.PollMax <= 0 {
		opt.PollMax = 50 * time.Millisecond
	}
	wait := opt.PollMin
	for {
		r, err := c.FillGet(ctx, key, opt.FillTimeout)
		if err != nil {
			return "", err
		}
		switch r.State {
		case FillHit:
			return r.Value, nil
		case FillStale:
			if r.Token != "" {
				bg := context.WithoutCancel(ctx)
				go func() {
					if v, err := factory(bg); err == nil {
						c.FillSet(bg, key, v, ttl, r.Token, opt.Stale)
					}
				}()
			}
			return r.Value, nil
		case FillFill:
			v, err := factory(ctx)
			if err != nil {
				return "", err // the marker lapses after FillTimeout
			}
			if _, err := c.FillSet(ctx, key, v, ttl, r.Token, opt.Stale); err != nil {
				return v, err
			}
			return v, nil
		}
		if err := sleep(ctx, wait); err != nil {
			return "", err
		}
		if wait *= 2; wait > opt.PollMax {
			wait = opt.PollMax
		}
	}
}

// GetOrCreateJSON is GetOrCreate for any JSON-encodable type T: the factory result is
// stored as JSON and decoded into T. It returns an error if encoding or decoding fails.
//
//	u, err := ruvio.GetOrCreateJSON(ctx, c, "user:1", time.Minute, loadUser)
func GetOrCreateJSON[T any](ctx context.Context, c *Client, key string, ttl time.Duration, factory func(ctx context.Context) (T, error), o ...GetOrCreateOptions) (T, error) {
	var out T
	s, err := c.GetOrCreate(ctx, key, ttl, func(ctx context.Context) (string, error) {
		v, err := factory(ctx)
		if err != nil {
			return "", err
		}
		b, err := json.Marshal(v)
		return string(b), err
	}, o...)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal([]byte(s), &out)
	return out, err
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type idemOptions struct{ abortOnError bool }

// IdemOption tunes Idempotent.
type IdemOption func(*idemOptions)

// WithAbortOnError makes Idempotent release the reservation with IDEM ABORT when fn
// returns an error. Off by default: a failed fn leaves the record pending until ttl.
func WithAbortOnError() IdemOption { return func(o *idemOptions) { o.abortOnError = true } }

// Idempotent runs fn at most once for key (IDEM). fingerprint should identify the
// request body (for example a hash); ttl is how long the record lives.
//
// If an earlier call completed, its stored result is returned without running fn. If one
// is still running it returns ErrIdemPending; if the fingerprint differs it returns
// ErrIdemConflict. If fn fails the record stays pending until ttl, so retries get
// ErrIdemPending. If saving the result fails, the result is returned together with that
// error.
//
// With WithAbortOnError a failing fn also sends IDEM ABORT, so a retry runs fn again at
// once instead of getting ErrIdemPending; fn's error is still the one returned, and a
// failed abort is ignored (the record then stays pending until ttl). Use it only when a
// failed fn did not take effect or is safe to run again.
//
//	out, err := c.Idempotent(ctx, "pay:"+reqID, hash, time.Hour,
//	    func(ctx context.Context) ([]byte, error) { return charge(ctx) })
func (c *cmds) Idempotent(ctx context.Context, key, fingerprint string, ttl time.Duration, fn func(ctx context.Context) ([]byte, error), opts ...IdemOption) ([]byte, error) {
	var o idemOptions
	for _, opt := range opts {
		opt(&o)
	}
	owner := randomToken()
	res, err := c.IdemBegin(ctx, key, fingerprint, owner, ttl)
	if err != nil {
		return nil, err
	}
	switch res.State {
	case IdemCompleted:
		return res.Result, nil
	case IdemPending:
		return nil, ErrIdemPending
	case IdemConflict:
		return nil, ErrIdemConflict
	}
	out, err := fn(ctx)
	if err != nil {
		if o.abortOnError {
			// The caller's context may be what failed, so the abort gets its own.
			_, _ = c.IdemAbort(context.WithoutCancel(ctx), key, fingerprint, owner)
		}
		return nil, err
	}
	if _, err := c.IdemComplete(ctx, key, fingerprint, owner, out); err != nil {
		return out, err
	}
	return out, nil
}
