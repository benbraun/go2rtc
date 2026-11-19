package reolink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/baichuan"
)

const (
	loginBackoffBase = 15 * time.Second
	loginBackoffMax  = 5 * time.Minute
)

// errNotShareable means the camera already has a shared connection opened
// with different transport settings, so this source needs its own.
var errNotShareable = errors.New("reolink: connection settings differ from the shared connection")

// LoginBackoffError is returned without contacting the camera while it is
// still within the cooldown that followed a refused login.
type LoginBackoffError struct {
	Remaining time.Duration
	Cause     error
}

func (e *LoginBackoffError) Error() string {
	return fmt.Sprintf("reolink: camera refused the last login (%v); next attempt in %s",
		e.Cause, e.Remaining.Round(time.Second))
}

func (e *LoginBackoffError) Unwrap() error {
	return e.Cause
}

// loginBackoff spaces out login attempts after the camera refuses one. Fast
// retries of a rejected login can make Reolink firmware lock the account out.
type loginBackoff struct {
	failures int
	until    time.Time
	cause    error
}

// sharedClient is one authenticated Baichuan connection per camera and
// credential. Every TCP source of that camera carries its preview on it.
type sharedClient struct {
	key    cameraKey
	cfg    baichuan.Config
	client *baichuan.Client
	ready  chan struct{}
	err    error
	refs   int
}

func loginRejected(err error) bool {
	var status *baichuan.StatusError
	return errors.As(err, &status) && status.Command == 1
}

func (r *Registry) backoffErrorLocked(key cameraKey, now time.Time) error {
	if b := r.backoff[key]; b != nil && now.Before(b.until) {
		return &LoginBackoffError{Remaining: b.until.Sub(now), Cause: b.cause}
	}
	return nil
}

func (r *Registry) checkBackoff(key cameraKey) error {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	return r.backoffErrorLocked(key, r.now())
}

func (r *Registry) noteLogin(key cameraKey, remote string, err error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if err == nil {
		delete(r.backoff, key)
		return
	}
	if !loginRejected(err) {
		return // Network failures already back off through go2rtc's reconnect timer.
	}
	b := r.backoff[key]
	if b == nil {
		b = &loginBackoff{}
		r.backoff[key] = b
	}
	b.failures++
	delay := loginBackoffMax
	if b.failures <= 5 {
		delay = min(loginBackoffBase<<(b.failures-1), loginBackoffMax)
	}
	b.until = r.now().Add(delay)
	b.cause = err
	r.log.Warn().Str("camera", remote).Int("failures", b.failures).Dur("retry_in", delay).
		Str("error", err.Error()).Msg("reolink camera refused login")
}

func (r *Registry) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

func clientDone(client *baichuan.Client) bool {
	select {
	case <-client.Done():
		return true
	default:
		return false
	}
}

// acquireClient returns the camera's shared connection, dialing and logging in
// once for all sources that ask at the same time.
func (r *Registry) acquireClient(ctx context.Context, key cameraKey, remote string, cfg baichuan.Config) (*sharedClient, error) {
	for {
		r.connMu.Lock()
		sc := r.clients[key]
		if sc != nil {
			select {
			case <-sc.ready:
				if sc.err != nil || clientDone(sc.client) {
					// A dead connection stays with its holders until they release it.
					delete(r.clients, key)
					r.connMu.Unlock()
					continue
				}
			default:
			}
			if sc.cfg != cfg {
				r.connMu.Unlock()
				return nil, errNotShareable
			}
			sc.refs++
			r.connMu.Unlock()
			select {
			case <-sc.ready:
			case <-ctx.Done():
				r.releaseClient(sc)
				return nil, ctx.Err()
			}
			if sc.err != nil {
				r.releaseClient(sc)
				return nil, sc.err
			}
			return sc, nil
		}
		if err := r.backoffErrorLocked(key, r.now()); err != nil {
			r.connMu.Unlock()
			return nil, err
		}
		sc = &sharedClient{key: key, cfg: cfg, ready: make(chan struct{}), refs: 1}
		r.clients[key] = sc
		r.connMu.Unlock()

		dial := r.dial
		if dial == nil {
			dial = dialAndLogin
		}
		client, err := dial(ctx, cfg, func(err error) { r.noteLogin(key, remote, err) })

		r.connMu.Lock()
		sc.client, sc.err = client, err
		if err != nil && r.clients[key] == sc {
			delete(r.clients, key)
		}
		close(sc.ready)
		r.connMu.Unlock()
		if err != nil {
			r.releaseClient(sc)
			return nil, err
		}
		return sc, nil
	}
}

func (r *Registry) releaseClient(sc *sharedClient) {
	r.connMu.Lock()
	sc.refs--
	var closing *baichuan.Client
	if sc.refs == 0 {
		if r.clients[sc.key] == sc {
			delete(r.clients, sc.key)
		}
		closing = sc.client
	}
	r.connMu.Unlock()
	if closing != nil {
		_ = closing.Close()
	}
}

func dialAndLogin(ctx context.Context, cfg baichuan.Config, note func(error)) (*baichuan.Client, error) {
	client, err := baichuan.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	err = client.Login(ctx)
	note(err)
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return client, nil
}
