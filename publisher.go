package spine

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
	"github.com/xtaci/kcp-go/v5"

	"github.com/grandcat/zeroconf"
)

type Publisher[K any] struct {
	namespace *Namespace
	name      string
	server    *zeroconf.Server
	logger    *slog.Logger

	serializer *mad.Mad[K]

	listener   *kcp.Listener
	clients    []io.ReadWriteCloser
	clientMu   sync.RWMutex
	deadClient chan io.ReadWriteCloser

	sendSig    chan struct{}
	lastDataMu sync.RWMutex
	lastData   K
}

func NewPublisher[K any](ns *Namespace, name string) (*Publisher[K], error) {

	serializer, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	listener, err := kcp.ListenWithOptions(":0", ns.encryption, 10, 3)
	if err != nil {
		return nil, err
	}

	server, err := zeroconf.Register(
		name,
		"_"+ns.Name()+globals.ZERO_CONF_NODE_TYPE,
		globals.ZERO_CONF_DOMAIN,
		listener.Addr().(*net.UDPAddr).Port,
		[]string{
			"type=" + globals.ZERO_CONF_PUBLISHER,
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	p := &Publisher[K]{
		namespace: ns,
		name:      name,
		server:    server,
		logger:    ns.logger,

		serializer: serializer,

		listener:   listener,
		deadClient: make(chan io.ReadWriteCloser, 100),
		clients:    make([]io.ReadWriteCloser, 0),

		sendSig: make(chan struct{}, 1),
	}

	go runListener(listener, ns.logger, p.registerSubscriber)
	go p.run()

	return p, nil
}

func (p *Publisher[K]) run() {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.sendSig:
			p.lastDataMu.RLock()
			tempData := p.lastData
			p.lastDataMu.RUnlock()

			payloadSize := p.serializer.GetRequiredSize(&tempData) + 1
			if payloadSize > globals.MAX_PACKET_SIZE {
				p.logger.Error("payload size too big", "size", payloadSize)
				continue
			}
			ticker.Reset(10 * time.Second)
			bufPtr := p.namespace.bufferPool.Get().(*[]byte)
			buf := *bufPtr
			buf[0] = globals.PUBLISER_PUSH
			p.serializer.Encode(&tempData, buf[1:])

			var wg sync.WaitGroup

			p.clientMu.RLock()
			snapClients := make([]io.ReadWriteCloser, len(p.clients))
			copy(snapClients, p.clients)
			p.clientMu.RUnlock()

			for _, client := range snapClients {
				wg.Add(1)
				go func(target io.ReadWriteCloser) {
					_, err := write(target, buf, payloadSize, false)
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
				p.namespace.bufferPool.Put(b)
			}(bufPtr)

		case deadClient := <-p.deadClient:
			p.clientMu.Lock()
			p.clients = slices.DeleteFunc(p.clients, func(c io.ReadWriteCloser) bool {
				return c == deadClient
			})
			deadClient.Close()
			p.clientMu.Unlock()

		case <-ticker.C:
			p.clientMu.RLock()
			snapClients := make([]io.ReadWriteCloser, len(p.clients))
			copy(snapClients, p.clients)
			p.clientMu.RUnlock()
			for _, client := range snapClients {
				go func(conn io.ReadWriteCloser) {
					err := ping(conn)
					if err != nil {
						select {
						case p.deadClient <- conn:
						default:
						}
					}
				}(client)
			}
		}
	}
}

func (p *Publisher[K]) registerSubscriber(conn io.ReadWriteCloser) {

	var err error
	bufPtr := p.namespace.bufferPool.Get().(*[]byte)
	buf := *bufPtr

	defer func() {
		if err != nil {
			conn.Close()
		}
		p.namespace.bufferPool.Put(bufPtr)
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
