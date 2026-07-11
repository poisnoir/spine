package spine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestService(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	// BUGFIX: see pubsub_test.go — spined only accepts "common", and node names must be
	// unique within a namespace, so every CreateNode below gets its own distinct name
	// (they used to all pass "test", which would collide the moment a real spined saw
	// more than one of these in the same namespace).
	ns, err := CreateNode("common", "service_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	// BUGFIX: handler/caller were func(string)(string,error) / NewServiceCaller[string,string]
	// — mad has no string support anymore, so this failed with "unsupported type: string"
	// once it got past the namespace fix above. Swapped to uint32, using 0 as the
	// error-trigger sentinel in place of the "error" string sentinel.
	handler := func(input uint32) (uint32, error) {
		if input == 0 {
			return 0, errors.New("intentional error")
		}
		return input + 1, nil
	}

	_, err = NewService(ns, "greeter", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[uint32, uint32](ns, "greeter")
	if err != nil {
		t.Fatal(err)
	}
	// Test success case
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := caller.Call(41, ctx)
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if resp != 42 {
		t.Errorf("expected 42, got %d", resp)
	}

	// Test error case
	resp, err = caller.Call(0, ctx)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestThreadedService(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := CreateNode("common", "threaded_service_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	handler := func(input uint32) (uint32, error) {
		return input * 10, nil
	}

	// renamed from "math" — collided with service_benchmark_test.go's BenchmarkServiceCall
	// once both run against the same "common" namespace.
	_, err = NewThreadedService(ns, "threaded_math", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[uint32, uint32](ns, "threaded_math")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := uint32(1); i <= 5; i++ {
		resp, err := caller.Call(i, ctx)
		if err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		if resp != i*10 {
			t.Errorf("expected %d, got %d", i*10, resp)
		}
	}
}

func TestServiceCaller_ContextCancel(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := CreateNode("common", "cancel_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	// BUGFIX: was func(string)(string,error) / NewServiceCaller[string,string] — mad has
	// no string support anymore. uint32 exercises the same slow-handler/timeout path.
	handler := func(input uint32) (uint32, error) {
		time.Sleep(2 * time.Second)
		return input, nil
	}

	_, err = NewService(ns, "slow", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[uint32, uint32](ns, "slow")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = caller.Call(1, ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestThreadedService_Parallel(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := CreateNode("common", "threaded_parallel_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	handler := func(input uint32) (uint32, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	_, err = NewThreadedService(ns, "parallel", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[uint32, uint32](ns, "parallel")
	if err != nil {
		t.Fatal(err)
	}
	const count = 20
	errChan := make(chan error, count)

	for i := uint32(0); i < count; i++ {
		go func(val uint32) {
			resp, err := caller.Call(val, ctx)
			if err != nil {
				errChan <- err
				return
			}
			if resp != val {
				errChan <- errors.New("wrong response")
				return
			}
			errChan <- nil
		}(i)
	}

	for i := 0; i < count; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("parallel call failed: %v", err)
		}
	}
}
