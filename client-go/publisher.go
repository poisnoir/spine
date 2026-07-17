package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/poisnoir/spine-go/internal/mad"
	"github.com/poisnoir/spine-go/internal/globals"
)

type Publisher[K any] struct {
	node   *Node
	name   string
	logger *slog.Logger

	serializer *mad.Mad[K]

	listener   net.Listener
	clients    []io.ReadWriteCloser
	clientMu   sync.RWMutex
	deadClient chan io.ReadWriteCloser

	sendSig    chan struct{}
	lastDataMu sync.RWMutex
	lastData   K

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

		listener:   listener,
		deadClient: make(chan io.ReadWriteCloser, 100),
		clients:    make([]io.ReadWriteCloser, 0),

		sendSig: make(chan struct{}, 1),

		ctx: node.ctx,
	}

	go runListener(listener, node.logger, p.registerSubscriber)
	go p.run()

	return p, nil
}

func (p *Publisher[K]) run() {

	for {
		select {
		case <-p.ctx.Done():
			p.clientMu.Lock()
			for _, client := range p.clients {
				client.Close()
			}
			p.clients = nil
			p.clientMu.Unlock()
			return
		case <-p.sendSig:
			p.lastDataMu.RLock()
			tempData := p.lastData
			p.lastDataMu.RUnlock()

			payloadSize := p.serializer.GetRequiredSize()
			if payloadSize > globals.MAX_PACKET_SIZE {
				p.logger.Error("payload size too big", "size", payloadSize)
				continue
			}
			bufPtr := p.node.bufferPool.Get().(*[]byte)
			buf := *bufPtr
			p.serializer.Encode(&tempData, buf)

			var wg sync.WaitGroup

			p.clientMu.RLock()
			snapClients := make([]io.ReadWriteCloser, len(p.clients))
			copy(snapClients, p.clients)
			p.clientMu.RUnlock()

			for _, client := range snapClients {
				wg.Add(1)
				go func(target io.ReadWriteCloser) {
					_, err := target.Write(buf[:payloadSize])
					if err != nil {
						select {
						case p.deadClient <- target:
						default:
						}
					}
					wg.Done()
				}(client)
			}

			go func(b *[]byte) {
				wg.Wait()
				p.node.bufferPool.Put(b)
			}(bufPtr)

		case deadClient := <-p.deadClient:
			p.clientMu.Lock()
			p.clients = slices.DeleteFunc(p.clients, func(c io.ReadWriteCloser) bool {
				return c == deadClient
			})
			deadClient.Close()
			p.clientMu.Unlock()
		}
	}
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

	p.clientMu.Lock()
	p.clients = append(p.clients, conn)
	p.clientMu.Unlock()
}

func (p *Publisher[K]) Publish(data K) {
	p.lastDataMu.Lock()
	p.lastData = data
	p.lastDataMu.Unlock()

	select {
	case p.sendSig <- struct{}{}:
	default:
	}
}
