package spine

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func BenchmarkPubSub(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	ns, err := CreateNode("bench_pubsub", "test", ctx, logger)
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

	time.Sleep(time.Millisecond * 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.Publish(uint32(i))
		_, err := sub.Get()
		if err != nil {
			b.Fatal(err)
		}
	}
}
