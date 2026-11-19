package reolink

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/baichuan"
	"github.com/AlexxIT/go2rtc/pkg/creds"
)

func TestLoginBackoffAfterCameraRefusal(t *testing.T) {
	registry := testRegistry(nil)
	now := time.Unix(1_000_000, 0)
	registry.clock = func() time.Time { return now }
	var dials atomic.Int32
	registry.dial = func(_ context.Context, _ baichuan.Config, note func(error)) (*baichuan.Client, error) {
		dials.Add(1)
		err := &baichuan.StatusError{Command: 1, Code: 407}
		note(err)
		return nil, err
	}
	key := cameraKey{endpoint: "camera:9000"}
	cfg := baichuan.NewConfig("camera", "user", "pw")

	if _, err := registry.acquireClient(context.Background(), key, "camera:9000", cfg); !loginRejected(err) {
		t.Fatalf("first attempt: %v", err)
	}
	var backoff *LoginBackoffError
	_, err := registry.acquireClient(context.Background(), key, "camera:9000", cfg)
	if !errors.As(err, &backoff) || backoff.Remaining != loginBackoffBase || dials.Load() != 1 {
		t.Fatalf("second attempt contacted the camera or skipped the cooldown: err=%v dials=%d", err, dials.Load())
	}
	if err := registry.checkBackoff(key); err == nil {
		t.Fatal("dedicated connections ignored the cooldown")
	}

	now = now.Add(loginBackoffBase)
	_, _ = registry.acquireClient(context.Background(), key, "camera:9000", cfg)
	_, err = registry.acquireClient(context.Background(), key, "camera:9000", cfg)
	if !errors.As(err, &backoff) || backoff.Remaining != 2*loginBackoffBase || dials.Load() != 2 {
		t.Fatalf("cooldown did not double: err=%v dials=%d", err, dials.Load())
	}
	for i := 0; i < 10; i++ {
		registry.noteLogin(key, "camera:9000", &baichuan.StatusError{Command: 1, Code: 401})
	}
	_, err = registry.acquireClient(context.Background(), key, "camera:9000", cfg)
	if !errors.As(err, &backoff) || backoff.Remaining != loginBackoffMax {
		t.Fatalf("cooldown not capped: %v", err)
	}

	registry.noteLogin(key, "camera:9000", nil)
	if err := registry.checkBackoff(key); err != nil {
		t.Fatalf("successful login kept the cooldown: %v", err)
	}
}

func TestLoginBackoffIgnoresNetworkErrors(t *testing.T) {
	registry := testRegistry(nil)
	key := cameraKey{endpoint: "camera:9000"}
	registry.noteLogin(key, "camera:9000", context.DeadlineExceeded)
	registry.noteLogin(key, "camera:9000", &baichuan.StatusError{Command: 3, Code: 425})
	if err := registry.checkBackoff(key); err != nil {
		t.Fatalf("non-login failure started a cooldown: %v", err)
	}
}

func TestSharedClientDialsOnceForConcurrentSources(t *testing.T) {
	registry := testRegistry(nil)
	release := make(chan struct{})
	var dials atomic.Int32
	dialErr := errors.New("dial failed")
	registry.dial = func(_ context.Context, _ baichuan.Config, _ func(error)) (*baichuan.Client, error) {
		dials.Add(1)
		<-release
		return nil, dialErr
	}
	key := cameraKey{endpoint: "camera:9000"}
	cfg := baichuan.NewConfig("camera", "user", "pw")
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = registry.acquireClient(context.Background(), key, "camera:9000", cfg)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if dials.Load() != 1 {
		t.Fatalf("concurrent sources dialed %d times", dials.Load())
	}
	for i, err := range errs {
		if !errors.Is(err, dialErr) {
			t.Fatalf("source %d: %v", i, err)
		}
	}
	if len(registry.clients) != 0 {
		t.Fatal("failed connection stayed in the pool")
	}
}

func TestSharedClientRejectsDifferentSettings(t *testing.T) {
	registry := testRegistry(nil)
	key := cameraKey{endpoint: "camera:9000"}
	cfg := baichuan.NewConfig("camera", "user", "pw")
	registry.clients[key] = &sharedClient{key: key, cfg: cfg, ready: make(chan struct{}), refs: 1}
	other := cfg
	other.TCPQuickACK = true
	if _, err := registry.acquireClient(context.Background(), key, "camera:9000", other); !errors.Is(err, errNotShareable) {
		t.Fatalf("different settings joined the shared connection: %v", err)
	}
}

func TestUsernameIsNotMasked(t *testing.T) {
	if _, err := parseURL("reolink://plainviewer:hunter-x9@camera.local/main"); err != nil {
		t.Fatal(err)
	}
	if got := creds.SecretString("stream plainviewer"); got != "stream plainviewer" {
		t.Fatalf("username was registered as a secret: %q", got)
	}
	if got := creds.SecretString("hunter-x9"); got == "hunter-x9" {
		t.Fatal("password was not registered as a secret")
	}
}
