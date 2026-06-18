package spine

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func BenchmarkServiceCall(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := CreateNode("bench", "test", ctx, logger)
	if err != nil {
		b.Fatal(err)
	}
	handler := func(input uint32) (uint32, error) {
		return input * 2, nil
	}
	_, err = NewService(ns, "math", handler)
	if err != nil {
		b.Fatal(err)
	}
	caller, err := NewServiceCaller[uint32, uint32](ns, "math")
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

	ns, err := CreateNode("bench_parallel", "test", ctx, logger)
	if err != nil {
		b.Fatal(err)
	}
	handler := func(input uint32) (uint32, error) {
		return input * 2, nil
	}
	_, err = NewService(ns, "math_parallel", handler)
	if err != nil {
		b.Fatal(err)
	}
	caller, err := NewServiceCaller[uint32, uint32](ns, "math_parallel")
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
