package spine

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

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
		isConnected:  false,
		pushSig:      make(chan struct{}, 1),
		serializer:   decoder,
	}

	if err := sub.connect(); err == nil {
		t.Error("expected connect() to return the handshake read error, got nil")
	}
	if sub.isConnected {
		t.Error("expected isConnected to remain false after a failed handshake read")
	}
}
