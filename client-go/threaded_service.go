package spine

import (
	"fmt"
	"io"
	"net"

	"github.com/poisnoir/spine-go/client-go/internal/mad"
)

type ThreadedService[K any, V any] struct {
	node *Node
	name string

	listener net.Listener

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]

	handler func(K) (V, error)
}

// BUGFIX: dropped the unused context/cancel fields along with Close() — this
// type never read its context anywhere (each request is handled synchronously
// in its own per-connection goroutine, no ctx.Done() select), so they were
// dead weight even before entity-level Close() was removed project-wide.
func NewThreadedService[K any, V any](node *Node, name string, handler func(K) (V, error)) (*ThreadedService[K, V], error) {

	keyEnc, valueEnc, listener, err := generateService[K, V](node, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
	}

	ts := &ThreadedService[K, V]{
		node: node,
		name: name,

		listener:        listener,
		handler:         handler,
		keySerializer:   keyEnc,
		valueSerializer: valueEnc,
	}

	go runListener(listener, node.logger, ts.clientHandler) // stops when listener closes
	return ts, nil
}

func (s *ThreadedService[K, V]) clientHandler(conn io.ReadWriteCloser) {

	logger := s.node.logger.With(
		s.node.Name(),
		"service",
		s.name,
		"client handler",
	)

	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)

	// BUGFIX: dropped the nil stringSerializer arg — handleCallerRequest no longer takes one.
	handleCallerRequest(conn, s.keySerializer, s.valueSerializer, *bufPtr, s.processRequest, logger)

}

func (ts *ThreadedService[K, V]) processRequest(key K) serviceOutput[V] {
	result, err := ts.handler(key)
	return serviceOutput[V]{data: result, err: err}
}

