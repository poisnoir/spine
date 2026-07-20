package spine

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/spine-go/client-go/internal/mad"
	"github.com/poisnoir/spine-go/client-go/internal/globals"
)

// subscriberQueueCapacity bounds how many decoded-but-not-yet-Get()'d values
// a Subscriber holds onto. Once full, the oldest unread value is dropped to
// make room for the newest one - see Subscriber.push. A fixed constant
// rather than a NewSubscriber parameter: keeps the existing signature and
// every call site (examples, tests, benchmarks) unchanged; revisit as a
// per-Subscriber option later if a single default stops being enough.
const subscriberQueueCapacity = 32

type Subscriber[K any] struct {
	node         *Node
	subscribedTo string

	ctx context.Context

	// conn is only ever touched by run() - the single background goroutine
	// that owns the connection for this Subscriber's whole lifetime - so it
	// needs no lock of its own.
	conn net.Conn

	// Bounded ring buffer of decoded values not yet returned by Get().
	// run() is the sole producer; Get() is the (possibly concurrent)
	// consumer. queueCond signals "the queue became non-empty, or ctx was
	// cancelled" - Get() re-checks ctx.Err() after waking since either can
	// be why it woke up.
	queueMu   sync.Mutex
	queueCond *sync.Cond
	queue     []K
	head      int
	count     int

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
		queue:        make([]K, subscriberQueueCapacity),
		serializer:   decoder,
	}
	sub.queueCond = sync.NewCond(&sub.queueMu)

	// Mirrors client-zig's Node.subscribe(), which calls Subscriber.connect()
	// synchronously before returning - blocks (unlimited backoff) until a
	// publisher actually exists, rather than handing back a Subscriber that
	// might not be connected yet and hoping something notices later. This
	// also closes a previously-documented gap: since there's now a real
	// synchronous failure point, a failed connect can roll back the
	// registration above, the same way NewPublisher/NewService already do.
	bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), sub.ctx)
	if err := backoff.Retry(sub.connect, bo); err != nil {
		node.unregisterConsumer(topic, false)
		return nil, err
	}

	go sub.run()
	go func() {
		<-sub.ctx.Done()
		sub.queueMu.Lock()
		sub.queueCond.Broadcast() // wake any Get() blocked waiting for data - it'll see ctx.Err() and return
		sub.queueMu.Unlock()
	}()

	return sub, nil
}

// run reads every published value off the wire as soon as it arrives,
// independently of how fast (or slow) callers of Get() drain them, and
// keeps the most recent subscriberQueueCapacity of them - see push.
//
// This is a deliberate divergence from client-zig's Subscriber.next()
// (which has no buffering at all and instead applies backpressure straight
// through to the publisher once a subscriber falls behind): here, a
// subscriber slower than its publisher drops its own oldest unread values
// instead of ever stalling the publisher or other subscribers of the same
// topic. That tradeoff (staleness over completeness, and over ever
// blocking the sender) was chosen deliberately for this client, not an
// oversight - see subscriber_test.go for the two scenarios it's verified
// against.
func (s *Subscriber[K]) run() {
	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)
	buf := *bufPtr
	payloadSize := s.serializer.GetRequiredSize()

	for {
		if s.ctx.Err() != nil {
			return
		}

		// io.ReadFull, not conn.Read(buf): a Unix domain socket has no
		// message framing of its own, so a bare Read() into the whole pool
		// buffer can return more than one already-written value
		// concatenated together once the publisher gets ahead of this
		// loop - reading exactly one payload's worth never over-reads into
		// the next message.
		if _, err := io.ReadFull(s.conn, buf[:payloadSize]); err != nil {
			s.node.logger.Warn("subscriber connection lost", "topic", s.subscribedTo, "error", err)
			s.conn.Close()

			bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), s.ctx)
			if err := backoff.Retry(s.connect, bo); err != nil {
				return // ctx cancelled, or a permanent type mismatch - nothing more this goroutine can do
			}
			continue
		}

		var data K
		if err := s.serializer.Decode(buf[:payloadSize], &data); err != nil {
			s.node.logger.Error("subscriber decode failed", "topic", s.subscribedTo, "error", err)
			continue
		}

		s.push(data)
	}
}

// push adds v to the queue, evicting the oldest unread value first if the
// queue is already full. Only ever called from run() (the sole producer).
func (s *Subscriber[K]) push(v K) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()

	if s.count == len(s.queue) {
		var zero K
		s.queue[s.head] = zero // don't hold onto it longer than necessary
		s.head = (s.head + 1) % len(s.queue)
		s.count--
	}

	tail := (s.head + s.count) % len(s.queue)
	s.queue[tail] = v
	s.count++
	s.queueCond.Signal()
}

// Get returns the oldest value not yet returned, blocking if the queue is
// currently empty. Safe for concurrent callers: each gets the next value
// in FIFO order, none see the same value twice.
func (s *Subscriber[K]) Get() (K, error) {
	var zero K

	s.queueMu.Lock()
	defer s.queueMu.Unlock()

	for s.count == 0 {
		if err := s.ctx.Err(); err != nil {
			return zero, err
		}
		s.queueCond.Wait()
	}

	v := s.queue[s.head]
	s.queue[s.head] = zero
	s.head = (s.head + 1) % len(s.queue)
	s.count--
	return v, nil
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
