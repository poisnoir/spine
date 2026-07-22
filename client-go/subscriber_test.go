package spine

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/poisnoir/spine-go/client-go/internal/globals"
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

// Historical note on how Subscriber.Get() got to its current design:
//
//  1. Get() used to just wait on a signal from a background goroutine that
//     continuously read the socket into a single shared lastData field - a
//     fresh decoded value overwrote the previous one before a slow-to-call
//     Get() ever saw it. 2000 rapid Publish() calls delivered only 348
//     values via Get(), out of order.
//  2. A bounded ring-buffer queue (background goroutine feeds it, Get()
//     pops from it) fixed that, deliberately trading data loss for never
//     blocking the publisher on a slow subscriber - but its own producer
//     goroutine could die on a permanent reconnect failure with nothing
//     left to ever wake a blocked Get() again, hanging it forever. Fixing
//     that hang was possible, but the head-of-line-blocking tradeoff itself
//     wasn't judged worth it here, so this went back to option 3 below.
//  3. The current design: Get() reads directly, synchronously, matching
//     client-zig's Subscriber.next() - no queue, no background goroutine,
//     no possibility of the producer dying independently of whoever's
//     calling Get(). The tradeoff is the mirror image of option 2's: a
//     slow Get() caller now applies real backpressure through to the
//     publisher (see Publisher.Publish's own doc comment), instead of
//     silently losing its own stale data.
//
// Get() must still avoid the *other* independent bug bug 2's fix also
// carried forward: reading into the *whole* MAX_PACKET_SIZE pool buffer via
// a single conn.Read() silently discards extra bytes, since a Unix domain
// socket has no message framing of its own and one read call could return
// several already-written values concatenated together once the publisher
// got far enough ahead. Get() uses io.ReadFull for exactly one payload's
// worth to avoid that - see Get()'s own comment.

// A publisher restarting (or its connection otherwise dropping) must be
// transparent to Get() - it reconnects internally and keeps blocking,
// rather than surfacing the drop as an error to the caller.
func TestSubscriber_GetReconnectsAfterConnectionDrop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_reconnect_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "subscriber_reconnect_test_topic")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber[uint32](node, "subscriber_reconnect_test_topic")
	if err != nil {
		t.Fatal(err)
	}

	pub.Publish(1)
	got, err := sub.Get()
	if err != nil {
		t.Fatalf("first Get() failed: %v", err)
	}
	if got != 1 {
		t.Fatalf("first Get(): got %d, want 1", got)
	}

	// Simulate the connection dying by closing the *publisher's* accepted
	// copy of it (not the subscriber's own conn) - closing your own fd and
	// then reusing it is a use-after-close bug, not a stand-in for a real
	// remote disconnect. Closing the peer's end and leaving the
	// subscriber's own fd untouched is exactly how a real disconnect gets
	// observed: Get()'s next read on its own still-valid fd sees a genuine
	// EOF/error.
	pub.mu.Lock()
	pub.clients[0].Close()
	pub.clients = pub.clients[:0]
	pub.mu.Unlock()

	resultCh := make(chan struct {
		v   uint32
		err error
	}, 1)
	go func() {
		v, err := sub.Get()
		resultCh <- struct {
			v   uint32
			err error
		}{v, err}
	}()

	// give Get() a moment to notice the closed conn and reconnect before
	// publishing - dialing a local unix socket is fast but not instant.
	time.Sleep(200 * time.Millisecond)
	pub.Publish(2)

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("Get() after reconnect failed: %v", r.err)
		}
		if r.v != 2 {
			t.Fatalf("Get() after reconnect: got %d, want 2", r.v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get() did not return after a connection drop + republish")
	}
}

// A *permanent* reconnect failure (the producer now serves a different
// type) must surface as a real error from Get() - not be retried forever,
// and not leave Get() hanging with nothing to ever wake it (the ring-buffer
// design's bug - see the historical note above). Since Get() does its own
// read directly with no separate producer goroutine to die independently,
// there's no separate "who wakes the waiter" question here: the same call
// that hits the permanent failure is the one that returns it.
func TestSubscriber_GetReturnsErrorInsteadOfHangingAfterPermanentReconnectFailure(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "subscriber_permanent_fail_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	topic := "subscriber_permanent_fail_test_topic"
	path := "/tmp/spine/publisher/" + node.namespace + "/" + topic
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)

	listener1, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}

	acceptedCh := make(chan net.Conn, 1)
	go func() {
		conn, err := listener1.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		conn.Read(buf) // consume the type code
		conn.Write([]byte{globals.OK_STATUS_CODE})
		acceptedCh <- conn
	}()

	sub, err := NewSubscriber[uint32](node, topic)
	if err != nil {
		t.Fatal(err)
	}
	listener1.Close()

	firstConn := <-acceptedCh
	firstConn.Close() // forces Get()'s next read to fail and reconnect

	os.Remove(path)
	listener2, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener2.Close()
	defer os.Remove(path)

	go func() {
		conn, err := listener2.Accept()
		if err != nil {
			return
		}
		conn.Write([]byte{255}) // anything != OK_STATUS_CODE
		conn.Close()
	}()

	resultCh := make(chan error, 1)
	go func() {
		_, err := sub.Get()
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get() hung instead of returning an error")
	}
}
