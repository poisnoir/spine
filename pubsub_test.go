package spine

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestPubSub_Basic(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("test_pubsub", "node1", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[string](node, "updates")
	if err != nil {
		t.Fatal(err)
	}

	sub, err := NewSubscriber[string](node, "updates")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	time.Sleep(time.Millisecond * 100)

	expected := "hello subscribers"
	pub.Publish(expected)

	got, err := sub.Get()
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}

	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestPubSub_MultipleSubscribers(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("test_multi_pubsub", "node1", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[int32](node, "numbers")
	if err != nil {
		t.Fatal(err)
	}

	const subCount = 3
	subs := make([]*Subscriber[int32], subCount)
	for i := 0; i < subCount; i++ {
		s, err := NewSubscriber[int32](node, "numbers")
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
		defer s.Close()
	}

	time.Sleep(time.Millisecond * 100)

	val := int32(42)
	pub.Publish(val)

	for i, s := range subs {
		got, err := s.Get()
		if err != nil {
			t.Errorf("sub %d failed to get message: %v", i, err)
			continue
		}
		if got != val {
			t.Errorf("sub %d expected %d, got %d", i, val, got)
		}
	}
}
