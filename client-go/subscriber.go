package spine

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/spine-go/client-go/internal/mad"
	"github.com/poisnoir/spine-go/client-go/internal/globals"
)

type Subscriber[K any] struct {
	node         *Node
	subscribedTo string

	ctx  context.Context
	conn net.Conn

	serializer *mad.Mad[K]
}

func NewSubscriber[K any](node *Node, topic string) (*Subscriber[K], error) {

	decoder, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	// Waiting for a publisher, not a service: isService = false.
	err = node.registerConsumer(topic, false)
	if err != nil {
		return nil, err
	}

	sub := &Subscriber[K]{
		node:         node,
		subscribedTo: topic,
		ctx:          node.ctx,
		serializer:   decoder,
	}

	// Blocks (unlimited backoff) until a publisher actually exists, rather
	// than handing back a Subscriber that might not be connected yet. A
	// failed connect rolls back the registration above, the same way
	// NewPublisher/NewService already do.
	bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), sub.ctx)
	if err := backoff.Retry(sub.connect, bo); err != nil {
		node.unregisterConsumer(topic, false)
		return nil, err
	}

	return sub, nil
}

// Get blocks until the next published message arrives, then decodes it. On
// a read failure (publisher died/restarted), reconnects (unlimited backoff)
// and retries the read rather than surfacing a transient disconnect as an
// error - reading has no side effects, so retrying here has none of the
// at-most-once/idempotency concerns ServiceCaller.Call has. Not safe for
// concurrent callers: Get owns the connection's read side directly, with no
// lock of its own - callers needing fan-out to multiple goroutines should
// serialize their own calls to Get.
func (s *Subscriber[K]) Get() (K, error) {
	var zero K

	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)
	buf := *bufPtr
	payloadSize := s.serializer.GetRequiredSize()

	for {
		// io.ReadFull, not conn.Read(buf): a Unix domain socket has no
		// message framing of its own, so a bare Read() into the whole pool
		// buffer can return more than one already-written value
		// concatenated together once the publisher gets ahead of this
		// call - reading exactly one payload's worth never over-reads into
		// the next message.
		if _, err := io.ReadFull(s.conn, buf[:payloadSize]); err != nil {
			s.node.logger.Warn("subscriber connection lost", "topic", s.subscribedTo, "error", err)
			s.conn.Close()

			bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), s.ctx)
			if err := backoff.Retry(s.connect, bo); err != nil {
				return zero, err // ctx cancelled, or a permanent type mismatch
			}
			continue
		}

		var data K
		if err := s.serializer.Decode(buf[:payloadSize], &data); err != nil {
			s.node.logger.Error("subscriber decode failed", "topic", s.subscribedTo, "error", err)
			continue
		}
		return data, nil
	}
}

func (s *Subscriber[K]) connect() error {

	logger := s.node.logger.With(
		s.node.Name(),
		"subscriber",
		s.subscribedTo,
		"connect",
	)

	// BUGFIX: Publisher's listener (network.go/createListener) was migrated from
	// "unixpacket" to "unix" but this dial was missed, so every Subscriber failed to
	// connect with "protocol wrong type for socket" and retried forever, silently.
	conn, err := net.Dial("unix", "/tmp/spine/publisher/"+s.node.namespace+"/"+s.subscribedTo)
	if err != nil {
		return err
	}

	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)
	buf := *bufPtr

	keyCode := s.serializer.Code()
	n := copy(buf, keyCode)

	_, err = conn.Write(buf[:n])
	if err != nil {
		logger.Error("failed to write into socket", "error", err)
		conn.Close()
		return err
	}

	_, err = conn.Read(buf)
	if err != nil {
		logger.Error("failed to read from socket", "error", err)
		conn.Close()
		return err
	}
	if buf[0] != globals.OK_STATUS_CODE {
		conn.Close()
		// BUGFIX: a type mismatch is permanent - K is fixed at compile time
		// by the caller, so retrying can never fix it. backoff.Permanent
		// stops the retry loop immediately instead of retrying forever,
		// mirroring client-zig's Subscriber.connect() ("a type mismatch is
		// permanent... only transient errors... should back off and
		// retry").
		err := fmt.Errorf("publisher data type is different")
		logger.Error("failed to validate publisher input type", "error", err)
		return backoff.Permanent(err)
	}

	s.conn = conn
	return nil
}

func (s *Subscriber[K]) SubscribedTo() string {
	return s.subscribedTo
}
