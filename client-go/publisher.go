package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/poisnoir/spine-go/client-go/internal/mad"
	"github.com/poisnoir/spine-go/client-go/internal/globals"
)

type Publisher[K any] struct {
	node   *Node
	name   string
	logger *slog.Logger

	serializer *mad.Mad[K]

	listener net.Listener
	clients  []io.ReadWriteCloser
	// Guards clients, held across a whole Publish() pass (every write plus
	// every removal), not just individual field accesses - see Publish's
	// own comment for why.
	mu sync.Mutex

	ctx context.Context
}

func NewPublisher[K any](node *Node, name string) (*Publisher[K], error) {

	serializer, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	err = node.registerPublisher(name, madTypeOf(serializer))
	if err != nil {
		return nil, err
	}

	socketPath := "/tmp/spine/publisher/" + node.namespace + "/" + name
	listener, err := createListener(socketPath)
	if err != nil {
		// spined already approved this name - undo that now, or it's left
		// believing a publisher is registered with nothing behind it.
		node.unregisterPublisher(name)
		return nil, err
	}

	// BUGFIX: no more entity-level Close()/cancel — a publisher shares its node's
	// context directly and lives as long as the node does, same as Subscriber.
	p := &Publisher[K]{
		node:   node,
		name:   name,
		logger: node.logger,

		serializer: serializer,

		listener: listener,
		clients:  make([]io.ReadWriteCloser, 0),

		ctx: node.ctx,
	}

	go runListener(listener, node.logger, p.registerSubscriber)
	go p.closeClientsOnCancel()

	return p, nil
}

// The only remaining background goroutine: Publish() itself is now fully
// synchronous (see its own comment), so the one thing still worth a
// dedicated goroutine is reacting to node shutdown promptly instead of
// only on the next Publish() call, which might never come.
func (p *Publisher[K]) closeClientsOnCancel() {
	<-p.ctx.Done()
	p.mu.Lock()
	for _, client := range p.clients {
		client.Close()
	}
	p.clients = nil
	p.mu.Unlock()
}

func (p *Publisher[K]) registerSubscriber(conn io.ReadWriteCloser) {

	var err error
	bufPtr := p.node.bufferPool.Get().(*[]byte)
	buf := *bufPtr

	defer func() {
		if err != nil {
			conn.Close()
		}
		p.node.bufferPool.Put(bufPtr)
	}()

	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	if !slices.Equal([]byte(p.serializer.Code()), buf[:n]) {
		err = fmt.Errorf("invalid data code")
		conn.Write([]byte{globals.ERROR_MISMATCH_PAYLOAD_CODE})
		return
	}

	_, err = conn.Write([]byte{globals.OK_STATUS_CODE})
	if err != nil {
		return
	}

	p.mu.Lock()
	p.clients = append(p.clients, conn)
	p.mu.Unlock()
}

// BUGFIX: Publish() used to be fire-and-forget - stash `data` into a shared
// field and ping a capacity-1, non-blocking-send channel for a background
// goroutine to pick up later. Two problems followed directly from that
// indirection, both verified live:
//   - Calling Publish() faster than that goroutine could drain the signal
//     (a real socket write per signal, much slower than a struct
//     assignment) silently overwrote the not-yet-sent value - 2000 rapid
//     Publish() calls in a burst delivered exactly 1 of them.
//   - Dead-client cleanup went through its own separate, capacity-100
//     notification channel with a non-blocking send; more than 100
//     simultaneous write failures in one round meant the rest were never
//     queued for removal, leaking those connections until (if ever) a
//     *later* Publish() call gave them another chance - verified: 27/150
//     dead clients survived a single publish round.
//
// Now mirrors client-zig's Publisher.publish() exactly: every call writes
// to and prunes the client list synchronously, in one pass, under one
// lock. No queue for a value to get lost in, no separate bounded channel
// for a removal to get lost in - a call that returns has actually
// broadcast to (or pruned) every client that existed when it was called.
// The tradeoff, also matching client-zig, is that this now blocks the
// caller for the duration of every client write - a single stuck
// subscriber head-of-line-blocks every other subscriber's delivery for
// this call, not just its own.
func (p *Publisher[K]) Publish(data K) {
	payloadSize := p.serializer.GetRequiredSize()
	if payloadSize > globals.MAX_PACKET_SIZE {
		p.logger.Error("payload size too big", "size", payloadSize)
		return
	}

	bufPtr := p.node.bufferPool.Get().(*[]byte)
	defer p.node.bufferPool.Put(bufPtr)
	buf := *bufPtr
	p.serializer.Encode(&data, buf)

	p.mu.Lock()
	defer p.mu.Unlock()

	i := 0
	for i < len(p.clients) {
		client := p.clients[i]
		if _, err := client.Write(buf[:payloadSize]); err != nil {
			client.Close()
			last := len(p.clients) - 1
			p.clients[i] = p.clients[last]
			p.clients = p.clients[:last]
			continue // check the client just swapped into position i next
		}
		i++
	}
}
