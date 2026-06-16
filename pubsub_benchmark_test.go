package spine

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func BenchmarkPubSub(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := JointNamespace("bench_pubsub", ctx, logger)
	if err != nil {
		b.Fatal(err)
	}

	topic := "bench_topic"
	pub, err := NewPublisher[uint32](ns, topic)
	if err != nil {
		b.Fatal(err)
	}

	sub, err := NewSubscriber[uint32](ns, topic)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.Publish(uint32(i))
		_, err := sub.Get()
		if err != nil {
			b.Fatal(err)
		}
	}
}
