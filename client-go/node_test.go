package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

const testNodeSpinedPath = "/tmp/spine/spined"

// Regression test for a real deadlock: sendRegistration used to write to
// and read from n.spinedConn - a single connection shared by every entity
// a Node ever registers - with no synchronization at all. Two goroutines
// registering entities on the same Node concurrently (e.g. NewPublisher and
// NewService called from separate goroutines) raced their Read() calls
// against each other's Write()s: spined answers requests in whatever order
// it processes them, not in a way tied to which goroutine's Read() happens
// to be waiting for a response, so a goroutine could end up blocked forever
// reading a response a different goroutine's Read() had already consumed.
//
// Reproduced live before the fix: 40 goroutines calling NewPublisher
// concurrently on one Node (each with a distinct, never-before-used topic
// name) hung indefinitely; a goroutine dump showed dozens of goroutines
// permanently parked in net.(*conn).Read, all on the same connection
// object. Fixed by adding Node.spinedConnMu, held across the whole write+
// read exchange in sendRegistration.
//
// This needs a real spined - local-only mode never touches spinedConn at
// all, so it can't exercise the race - which is why this lives in its own
// file rather than alongside the rest of the package's local-only tests.
// Uses an explicit fail-fast timeout instead of relying on `go test`'s own
// default (10 minutes) so a regression reports clearly instead of just
// looking like the suite hung.
func TestNode_ConcurrentEntityRegistrationDoesNotDeadlock(t *testing.T) {
	// BUGFIX: killing spined doesn't unlink the Unix socket file it bound -
	// the OS doesn't clean that up on its own when a process holding it is
	// killed, same reason spined itself self-heals past stale files on
	// startup (network.zig's bind()). Left alone, the next test in this
	// package to call CreateNode (e.g. TestPubSub_Basic) would dial this
	// now-abandoned file and get "connection reset by peer" instead of the
	// clean "connection refused -> local-only mode" it expects.
	os.Remove(testNodeSpinedPath)
	cmd := exec.Command("../zig-out/bin/spined")
	if err := cmd.Start(); err != nil {
		t.Skipf("could not start spined (%v) - build it first with `zig build` from the repo root", err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(testNodeSpinedPath)
	}()

	// Give spined a moment to bind its socket.
	time.Sleep(300 * time.Millisecond)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()

	node, err := CreateNode("common", "concurrent_reg_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}
	if node.spinedConn == nil {
		t.Fatal("expected a real spined connection, got local-only mode")
	}

	const n = 150
	var wg sync.WaitGroup
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			topic := fmt.Sprintf("concurrent_reg_test_topic_%d", idx)
			_, err := NewPublisher[uint32](node, topic)
			errs[idx] = err
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent registration did not complete within 15s - likely deadlocked reading a response meant for a different goroutine (see Node.spinedConnMu)")
	}

	failures := 0
	for i, err := range errs {
		if err != nil {
			failures++
			if failures <= 5 {
				t.Logf("publisher %d (distinct, never-before-used topic name) got unexpected error: %v", i, err)
			}
		}
	}
	if failures > 0 {
		t.Errorf("%d/%d concurrent registrations on the same Node failed despite every topic name being distinct and unused", failures, n)
	}
}
