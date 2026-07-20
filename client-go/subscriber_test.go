package spine

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/poisnoir/spine-go/client-go/internal/mad"
)

// Regression test for a real bug in Subscriber.connect(): a failed handshake
// read used to be logged and then fall through to "success" (s.conn = conn;
// s.isConnected = true; return nil) instead of returning the error - so a
// publisher that dies mid-handshake (closes the connection before it can
// write a status byte) left the subscriber marked connected over a dead
// socket, satisfying backoff.Retry over a connection that was never
// actually usable.
//
// Simulates that exact scenario: a fake "publisher" that accepts the
// connection, reads the type-fingerprint code the subscriber sends, then
// closes without ever writing a response.
func TestSubscriber_ConnectReturnsErrorWhenHandshakeReadFails(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_handshake_regress_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	topic := "subscriber_handshake_regress_topic"
	path := "/tmp/spine/publisher/" + node.namespace + "/" + topic
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)

	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer os.Remove(path)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		conn.Read(buf) // read the type code, then close without responding
		conn.Close()
	}()

	decoder, err := mad.NewMad[uint32]()
	if err != nil {
		t.Fatal(err)
	}

	sub := &Subscriber[uint32]{
		node:         node,
		subscribedTo: topic,
		ctx:          node.ctx,
		serializer:   decoder,
	}

	if err := sub.connect(); err == nil {
		t.Error("expected connect() to return the handshake read error, got nil")
	}
	if sub.conn != nil {
		t.Error("expected conn to remain unset after a failed handshake read")
	}
}

// Historical note on how Subscriber.Get() got to its current design, kept
// because both bugs below are real and were verified live, and the second
// one is easy to reintroduce by accident in any future rewrite of run():
//
//  1. Get() used to just wait on a signal from a background goroutine that
//     continuously read the socket into a single shared lastData field - a
//     fresh decoded value overwrote the previous one before a slow-to-call
//     Get() ever saw it. 2000 rapid Publish() calls delivered only 348
//     values via Get(), out of order.
//  2. An intermediate fix made Get() do its own read directly (no
//     buffering at all, matching client-zig) - which exposed a second,
//     independent bug: reading into the *whole* MAX_PACKET_SIZE pool
//     buffer via a single conn.Read() silently discarded extra bytes,
//     since a Unix domain socket has no message framing of its own and one
//     read call could return several already-written values concatenated
//     together once the publisher got far enough ahead. This skipped
//     straight from value 1 to value 31 in the same burst.
//
// The current design (run() feeds a bounded, oldest-evicted-first queue;
// Get() just pops from it) is a deliberate, discussed tradeoff, not a bug:
// see the two tests below for what it actually guarantees.

// A burst that fits inside subscriberQueueCapacity must never lose
// anything - only a burst that actually overflows the queue should drop
// anything, and only its oldest entries (see the overflow test below).
func TestSubscriber_BurstWithinCapacityDeliversEveryValueInOrder(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_withincap_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "subscriber_withincap_test_topic")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber[uint32](node, "subscriber_withincap_test_topic")
	if err != nil {
		t.Fatal(err)
	}

	const n = subscriberQueueCapacity // exactly at capacity - must not overflow
	for i := uint32(0); i < n; i++ {
		pub.Publish(i)
	}

	// Give run() time to read, decode, and queue all n values before we
	// start draining - the point of this test is the queue's own
	// contents, not a race between producer and consumer.
	time.Sleep(300 * time.Millisecond)

	for want := uint32(0); want < n; want++ {
		got, err := sub.Get()
		if err != nil {
			t.Fatalf("Get() failed at value %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("value %d: got %d, want %d (dropped or reordered within capacity)", want, got, want)
		}
	}
}

// A burst that exceeds subscriberQueueCapacity must evict the *oldest*
// unread values to make room for newer ones, keeping exactly the newest
// subscriberQueueCapacity, still in order - not drop newest, not reorder,
// not silently keep stale data indefinitely.
func TestSubscriber_OverflowDropsOldestKeepsNewestInOrder(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_overflow_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "subscriber_overflow_test_topic")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber[uint32](node, "subscriber_overflow_test_topic")
	if err != nil {
		t.Fatal(err)
	}

	const n = 100 // > subscriberQueueCapacity (32): guarantees overflow
	for i := uint32(0); i < n; i++ {
		pub.Publish(i)
	}

	// Deliberately not draining via Get() until after every value has been
	// published and (almost certainly) already read+queued by run() - the
	// queue needs to have actually overflowed before this test means
	// anything.
	time.Sleep(300 * time.Millisecond)

	firstExpected := uint32(n - subscriberQueueCapacity)
	for want := firstExpected; want < n; want++ {
		got, err := sub.Get()
		if err != nil {
			t.Fatalf("Get() failed at value %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("value %d: got %d, want %d (oldest-eviction order broken)", want, got, want)
		}
	}
}

// Concurrent Get() callers are meant to be safe, each getting a distinct
// value in FIFO order (see Get()'s own doc comment) - the queue is shared
// mutable state now guarded by queueMu/queueCond instead of by a single
// synchronous read per call, so this is worth its own direct check rather
// than only trusting the reasoning behind that design. -race isn't
// available in this environment (no cgo/gcc); run repeated with -count in
// CI where it is, for whatever extra confidence that adds.
func TestSubscriber_ConcurrentGetCallersEachGetDistinctSequentialValues(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_concurrent_get_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "subscriber_concurrent_get_topic")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber[uint32](node, "subscriber_concurrent_get_topic")
	if err != nil {
		t.Fatal(err)
	}

	const n = subscriberQueueCapacity // stay within capacity - this test is about concurrent consumers, not overflow
	const consumers = 8
	if n%consumers != 0 {
		t.Fatalf("test setup: n (%d) must divide evenly by consumers (%d)", n, consumers)
	}

	var wg sync.WaitGroup
	var resultsMu sync.Mutex
	var results []uint32

	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n/consumers; i++ {
				v, err := sub.Get()
				if err != nil {
					t.Errorf("Get() failed: %v", err)
					return
				}
				resultsMu.Lock()
				results = append(results, v)
				resultsMu.Unlock()
			}
		}()
	}

	for i := uint32(0); i < n; i++ {
		pub.Publish(i)
	}

	wg.Wait()

	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
	seen := make(map[uint32]bool, n)
	for _, v := range results {
		if seen[v] {
			t.Fatalf("value %d was returned to more than one Get() caller", v)
		}
		seen[v] = true
	}
	for want := uint32(0); want < n; want++ {
		if !seen[want] {
			t.Fatalf("value %d was never returned to any caller", want)
		}
	}
}
