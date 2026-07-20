package spine

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/poisnoir/spine-go/client-go/internal/mad"
)

// Regression test for a real bug in ServiceCaller.run(): the per-request
// branch used to discard the error from backoff.Retry(sc.connect, bo) and
// fall through to sc.send() unconditionally. backoff.Retry is configured
// with WithMaxElapsedTime(0) (retries forever on its own), so it can only
// return an error here because sc.ctx became Done() while the *initial*
// connect - the only way a ServiceCaller can exist without ever having
// connected - was still retrying. In that case sc.conn is still nil, and
// falling through to sc.send() called sc.conn.Write on a nil net.Conn: an
// unrecoverable nil-pointer panic (run() has no recover()), crashing the
// whole process.
//
// This constructs the exact precondition directly (unexported fields are
// reachable since this file is `package spine`) rather than racing the Go
// scheduler to land a Call() in sc.requests before run()'s first select
// evaluates - an already-cancelled context plus one request already queued
// before run() is ever invoked, so both of run()'s select cases are
// simultaneously ready on its very first iteration. Verified before the fix:
// 243/500 trials of this exact setup panicked.
func TestServiceCaller_CancelledDuringInitialConnectDoesNotPanic(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "svccaller_cancel_regress_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	keySer, err := mad.NewMad[uint32]()
	if err != nil {
		t.Fatal(err)
	}
	valSer, err := mad.NewMad[uint32]()
	if err != nil {
		t.Fatal(err)
	}

	const trials = 200
	panics := 0
	cleanErrors := 0

	for i := 0; i < trials; i++ {
		callerCtx, callerCancel := context.WithCancel(context.Background())
		callerCancel() // already Done before run() ever looks at it

		sc := &ServiceCaller[uint32, uint32]{
			node:            node,
			serviceName:     "svccaller_cancel_regress_nonexistent_service",
			keySerializer:   keySer,
			valueSerializer: valSer,
			ctx:             callerCtx,
			isConnected:     false,
			requests:        make(chan serviceRequest[uint32, uint32], 1),
		}
		out := make(chan serviceOutput[uint32], 1)
		sc.requests <- serviceRequest[uint32, uint32]{
			ctx:    context.Background(), // not cancelled - the queued request's own ctx isn't what's racing
			input:  42,
			output: out,
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					panics++
					if panics <= 3 {
						t.Logf("trial %d: PANIC: %v", i, r)
					}
				}
			}()
			sc.run() // real, unmodified run() - terminates within 1-2 iterations either way
		}()

		select {
		case o := <-out:
			if o.err != nil {
				cleanErrors++
			}
		default:
		}
	}

	t.Logf("%d/%d trials panicked; %d/%d trials returned a clean error instead", panics, trials, cleanErrors, trials)
	if panics != 0 {
		t.Fatalf("ServiceCaller.run() panicked in %d/%d trials - a cancelled initial connect must surface as an error, not a nil sc.conn panic", panics, trials)
	}
}
