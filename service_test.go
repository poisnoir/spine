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
	ns, err := JointNamespace("test_service", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	handler := func(input string) (string, error) {
		if input == "error" {
			return "", errors.New("intentional error")
		}
		return "hello " + input, nil
	}

	_, err = NewService(ns, "greeter", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[string, string](ns, "greeter")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()

	// Test success case
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := caller.Call("world", ctx)
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if resp != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", resp)
	}

	// Test error case
	resp, err = caller.Call("error", ctx)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestThreadedService(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := JointNamespace("test_threaded_service", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	handler := func(input uint32) (uint32, error) {
		return input * 10, nil
	}

	_, err = NewThreadedService(ns, "math", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[uint32, uint32](ns, "math")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()

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
	ns, err := JointNamespace("test_cancel", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	handler := func(input string) (string, error) {
		time.Sleep(2 * time.Second)
		return input, nil
	}

	_, err = NewService(ns, "slow", handler)
	if err != nil {
		t.Fatal(err)
	}

	caller, err := NewServiceCaller[string, string](ns, "slow")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = caller.Call("should_timeout", ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestThreadedService_Parallel(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := JointNamespace("test_parallel", ctx, logger)
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
	defer caller.Close()

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
