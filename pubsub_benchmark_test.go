package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func BenchmarkPubSub(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	// BUGFIX: see pubsub_test.go — spined only accepts "common"; node/topic names made
	// unique. Unique per-invocation, not just per-function: go test's benchmark harness
	// calls this whole function multiple times while calibrating (increasing b.N each
	// pass), re-running this setup every time. A fixed name collided with itself on the
	// second pass — NODE_ALREADY_REGISTERED, silently swallowed by CreateNode, then
	// "broken pipe" on the next write since spined had already closed that connection.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	ns, err := CreateNode("common", "bench_pubsub_node_"+suffix, ctx, logger)
	if err != nil {
		b.Fatal(err)
	}

	topic := "bench_topic_" + suffix

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
