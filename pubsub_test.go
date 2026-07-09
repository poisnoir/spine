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

	// BUGFIX: spined only accepts the "common" namespace today. These tests used to pass
	// ad-hoc namespace names, which only "worked" because they silently fell back to
	// local-only mode whenever spined wasn't reachable — the moment a real spined is
	// running (as it was for parts of this session), every one of these calls would
	// get silently rejected with INVALID_NAMESPACE and fail downstream in confusing ways.
	node, err := CreateNode("common", "pubsub_basic_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	// BUGFIX: was NewPublisher[string]/NewSubscriber[string] — mad has no string support
	// anymore (removed along with slices/maps), so this failed with "unsupported type:
	// string" the moment it got past the namespace fix above. uint32 exercises the same
	// pub/sub path with a type mad actually supports.
	pub, err := NewPublisher[uint32](node, "updates")
	if err != nil {
		t.Fatal(err)
	}

	sub, err := NewSubscriber[uint32](node, "updates")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	time.Sleep(time.Millisecond * 100)

	expected := uint32(777)
	pub.Publish(expected)

	got, err := sub.Get()
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}

	if got != expected {
		t.Errorf("expected %d, got %d", expected, got)
	}
}

func TestPubSub_MultipleSubscribers(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "pubsub_multi_node", ctx, logger)
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
