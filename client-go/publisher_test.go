package spine

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

// Regression test for a real bug: Publish() used to be fire-and-forget -
// stash `data` into a shared field and ping a capacity-1, non-blocking-send
// channel for a background goroutine to pick up later. Calling Publish()
// faster than that goroutine could drain the signal (a real socket write
// per signal, much slower than a struct assignment) silently overwrote the
// not-yet-sent value. Verified before the fix: 2000 rapid Publish() calls
// delivered exactly 1 of them.
//
// Reads directly off the raw socket instead of through Subscriber.Get() -
// Subscriber has its own separate lastData/pushSig fields on the receiving
// side with the identical shape of bug, which would fail this test for a
// reason that has nothing to do with Publish() itself. This isolates what
// changed: does Publish() now put every value on the wire.
func TestPublisher_DeliversEveryValueUnderBurst(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "publisher_burst_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "publisher_burst_test_topic")
	if err != nil {
		t.Fatal(err)
	}

	path := "/tmp/spine/publisher/" + node.namespace + "/publisher_burst_test_topic"
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("b14")); err != nil { // mad.code(uint32)
		t.Fatal(err)
	}
	var status [1]byte
	if _, err := io.ReadFull(conn, status[:]); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	const n = 2000
	go func() {
		for i := uint32(0); i < n; i++ {
			pub.Publish(i)
		}
	}()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [4]byte
	for want := uint32(0); want < n; want++ {
		if _, err := io.ReadFull(conn, buf[:]); err != nil {
			t.Fatalf("raw read failed after %d/%d values: %v", want, n, err)
		}
		if got := binary.BigEndian.Uint32(buf[:]); got != want {
			t.Fatalf("value %d on the wire: got %d, want %d (dropped or reordered)", want, got, want)
		}
	}
}

// Regression test for a real bug: dead-subscriber cleanup went through its
// own capacity-100 notification channel with a non-blocking send. More
// than 100 simultaneous write failures in one Publish() round meant the
// rest were never queued for removal, leaking those connections until (if
// ever) a *later* Publish() call gave them another chance. Verified before
// the fix: 27/150 dead clients survived a single publish round.
func TestPublisher_PrunesMoreThan100DeadClientsInOneCall(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := CreateNode("common", "publisher_deadclient_test_node", ctx, logger)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := NewPublisher[uint32](node, "publisher_deadclient_test_topic")
	if err != nil {
		t.Fatal(err)
	}

	const n = 150 // > the old deadClient channel's capacity of 100
	path := "/tmp/spine/publisher/" + node.namespace + "/publisher_deadclient_test_topic"
	for i := 0; i < n; i++ {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("subscriber %d failed to dial: %v", i, err)
		}
		if _, err := conn.Write([]byte("b14")); err != nil {
			t.Fatalf("subscriber %d failed handshake write: %v", i, err)
		}
		var status [1]byte
		conn.Read(status[:])
		conn.Close() // dies immediately after handshaking
	}

	time.Sleep(100 * time.Millisecond)

	pub.mu.Lock()
	before := len(pub.clients)
	pub.mu.Unlock()
	if before != n {
		t.Fatalf("expected all %d subscribers to have registered before publishing, got %d", n, before)
	}

	pub.Publish(1) // synchronous - returns only once this pass has pruned everyone it can

	pub.mu.Lock()
	after := len(pub.clients)
	pub.mu.Unlock()

	if after != 0 {
		t.Errorf("expected all %d dead clients pruned in a single Publish() call, %d remained", n, after)
	}
}
