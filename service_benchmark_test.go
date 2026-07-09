package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func BenchmarkServiceCall(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	// BUGFIX: see pubsub_benchmark_test.go — names must be unique per invocation, not
	// just per function, since go test's benchmark harness re-runs this whole function
	// (including setup) on every calibration pass.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	ns, err := CreateNode("common", "bench_service_node_"+suffix, ctx, logger)
	if err != nil {
		b.Fatal(err)
	}
	handler := func(input uint32) (uint32, error) {
		return input * 2, nil
	}
	_, err = NewService(ns, "math_"+suffix, handler)
	if err != nil {
		b.Fatal(err)
	}
	caller, err := NewServiceCaller[uint32, uint32](ns, "math_"+suffix)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := caller.Call(uint32(i), context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServiceCallParallel(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	ns, err := CreateNode("common", "bench_service_parallel_node_"+suffix, ctx, logger)
	if err != nil {
		b.Fatal(err)
	}
	handler := func(input uint32) (uint32, error) {
		return input * 2, nil
	}
	_, err = NewService(ns, "math_parallel_"+suffix, handler)
	if err != nil {
		b.Fatal(err)
	}
	caller, err := NewServiceCaller[uint32, uint32](ns, "math_parallel_"+suffix)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint32(0)
		for pb.Next() {
			_, err := caller.Call(i, context.Background())
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
