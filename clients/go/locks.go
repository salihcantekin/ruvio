// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// ErrNotAcquired is returned by WithLock, WithLease and WithPermit when the lock, lease
// or permit is held elsewhere and could not be taken.
var ErrNotAcquired = errors.New("ruvio: not acquired")

func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Lock is a held distributed lock: the key holds a random Token and Unlock and Refresh
// only act while the stored value is still that token, so they never touch somebody
// else's lock. Create one with AcquireLock or AcquireLockWait.
//
// Key is the lock key. Token is the random value stored in it (it identifies this
// holder). A lock is advisory and has no fencing token; use a Lease when a stale holder
// must be rejected downstream.
type Lock struct {
	c *cmds
	// Key is the lock key.
	Key string
	// Token is the random value stored in Key that identifies this holder.
	Token string
}

// AcquireLock tries once to take the lock on key for ttl, using SET key token NX PX ttl
// with a random token.
//
// ttl must be positive: a zero or negative ttl creates a lock that never expires. It
// returns the Lock and true on success; (nil, false, nil) when someone else holds it; an
// error on a failed command or cancelled ctx.
//
//	l, ok, err := c.AcquireLock(ctx, "job:42", 30*time.Second)
//	if err == nil && ok {
//	    defer l.Unlock(ctx)
//	    // work
//	}
func (c *cmds) AcquireLock(ctx context.Context, key string, ttl time.Duration) (*Lock, bool, error) {
	token := randomToken()
	ok, err := c.SetArgs(ctx, key, token, SetOptions{TTL: ttl, NX: true})
	if err != nil || !ok {
		return nil, false, err
	}
	return &Lock{c: c, Key: key, Token: token}, true, nil
}

// AcquireLockWait keeps calling AcquireLock every `every` until it succeeds. every must
// be greater than zero. It returns ctx.Err() when ctx ends before the lock is taken, or
// the first command error.
func (c *cmds) AcquireLockWait(ctx context.Context, key string, ttl, every time.Duration) (*Lock, error) {
	for {
		l, ok, err := c.AcquireLock(ctx, key, ttl)
		if err != nil || ok {
			return l, err
		}
		if err := sleep(ctx, every); err != nil {
			return nil, err
		}
	}
}

// Refresh extends the lock to ttl from now, but only while the stored token is still
// ours (SET key token IFEQ token with a new TTL). It returns false when the lock expired
// or was taken by someone else.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) (bool, error) {
	return l.c.SetIfEq(ctx, l.Key, l.Token, l.Token, SetOptions{TTL: ttl})
}

// Unlock releases the lock with DELEX key IFEQ token. It returns true when the lock was
// ours and is now removed, false when it had already expired or changed hands.
func (l *Lock) Unlock(ctx context.Context) (bool, error) {
	return l.c.DelIfEq(ctx, l.Key, l.Token)
}

// WithLock takes the lock on key for ttl, runs fn and then unlocks it. It returns
// ErrNotAcquired if the lock is held elsewhere (it does not wait), otherwise fn's error.
// The unlock still runs when ctx is cancelled, because it uses a context that ignores
// cancellation. Make sure fn finishes within ttl, or the lock may be lost; use
// Lock.Refresh for long work.
func (c *cmds) WithLock(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	l, ok, err := c.AcquireLock(ctx, key, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer l.Unlock(context.WithoutCancel(ctx))
	return fn(ctx)
}

// ---- fenced lease and semaphore ---------------------------------------------------

// LeaseHandle is a held fenced lease (LEASE command). Token grows with every grant on
// Key, so a downstream system can reject writes carrying an older token.
type LeaseHandle struct {
	c *cmds
	// Key is the lease key.
	Key string
	// Token is the fencing token granted with this lease.
	Token int64
}

// AcquireLease takes a fenced lease on key for ttl (LEASE). It returns the handle and
// true on success, or (nil, false, nil) while another holder has the lease.
func (c *cmds) AcquireLease(ctx context.Context, key string, ttl time.Duration) (*LeaseHandle, bool, error) {
	tok, ok, err := c.Lease(ctx, key, ttl)
	if err != nil || !ok {
		return nil, false, err
	}
	return &LeaseHandle{c: c, Key: key, Token: tok}, true, nil
}

// Renew extends the lease to now + ttl with RENEW key token ttl. It returns true when this
// handle still held it, false when it had expired or been replaced.
func (l *LeaseHandle) Renew(ctx context.Context, ttl time.Duration) (bool, error) {
	return l.c.Renew(ctx, l.Key, l.Token, ttl)
}

// Release gives the lease back with RELEASE key token. It returns true when this handle
// still held it, false when it had expired or been replaced.
func (l *LeaseHandle) Release(ctx context.Context) (bool, error) {
	return l.c.Release(ctx, l.Key, l.Token)
}

// WithLease takes a lease on key for ttl, runs fn with the fencing token and releases
// the lease afterwards (even when ctx is cancelled). It returns ErrNotAcquired when the
// lease is held elsewhere, otherwise fn's error.
func (c *cmds) WithLease(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context, token int64) error) error {
	l, ok, err := c.AcquireLease(ctx, key, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer l.Release(context.WithoutCancel(ctx))
	return fn(ctx, l.Token)
}

// Permit is one held slot of a counting semaphore (SEMAPHORE command). Token identifies
// the slot in Release.
type Permit struct {
	c *cmds
	// Key is the semaphore key.
	Key string
	// Token identifies this slot in Release.
	Token int64
}

// AcquirePermit takes one of max slots on key for ttl (SEMAPHORE). It returns the Permit
// and true when a slot was free, or (nil, false, nil) when all max slots are in use. A
// slot that is not released frees itself after ttl.
func (c *cmds) AcquirePermit(ctx context.Context, key string, max int64, ttl time.Duration) (*Permit, bool, error) {
	tok, ok, err := c.Semaphore(ctx, key, max, ttl)
	if err != nil || !ok {
		return nil, false, err
	}
	return &Permit{c: c, Key: key, Token: tok}, true, nil
}

// Renew extends the slot to now + ttl with RENEW key token ttl. It returns true if this
// permit still held it, false if it had already expired.
func (p *Permit) Renew(ctx context.Context, ttl time.Duration) (bool, error) {
	return p.c.Renew(ctx, p.Key, p.Token, ttl)
}

// Release frees the slot with RELEASE key token. It returns true if this permit still
// held it, false if it had already expired.
func (p *Permit) Release(ctx context.Context) (bool, error) {
	return p.c.Release(ctx, p.Key, p.Token)
}

// WithPermit takes one of max slots on key, runs fn and releases the slot afterwards
// (even when ctx is cancelled). It returns ErrNotAcquired when no slot is free,
// otherwise fn's error.
func (c *cmds) WithPermit(ctx context.Context, key string, max int64, ttl time.Duration, fn func(ctx context.Context) error) error {
	p, ok, err := c.AcquirePermit(ctx, key, max, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer p.Release(context.WithoutCancel(ctx))
	return fn(ctx)
}

// ---- rate limit -----------------------------------------------------------------

// RateLimit counts one hit on key in a window of the given duration and allows at most
// max hits per window (LIMIT). It returns allowed (this hit is within the limit),
// remaining (hits left in the window) and any error.
//
//	ok, left, err := c.RateLimit(ctx, "api:"+userID, 100, time.Minute)
func (c *cmds) RateLimit(ctx context.Context, key string, max int64, window time.Duration) (allowed bool, remaining int64, err error) {
	remaining, allowed, err = c.Limit(ctx, key, max, window)
	return allowed, remaining, err
}
