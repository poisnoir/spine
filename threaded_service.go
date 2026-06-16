package spine

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/grandcat/zeroconf"
	"github.com/poisnoir/mad-go"
)

type ThreadedService[K any, V any] struct {
	namespace *Namespace
	name      string
	server    *zeroconf.Server

	context  context.Context
	cancel   context.CancelFunc
	listener net.Listener

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]

	requests chan serviceRequest[K, V]
	handler  func(K) (V, error)
}

func NewThreadedService[K any, V any](namespace *Namespace, name string, handler func(K) (V, error)) (*ThreadedService[K, V], error) {

	keyEnc, valueEnc, listener, err := generateService[K, V](namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
	}

	ctx, cancel := context.WithCancel(namespace.ctx)

	ts := &ThreadedService[K, V]{
		namespace: namespace,
		name:      name,

		context: ctx,
		cancel:  cancel,

		handler:         handler,
		keySerializer:   keyEnc,
		valueSerializer: valueEnc,

		requests: make(chan serviceRequest[K, V], 100),
	}

	// todo fix me pls
	logger := namespace.logger

	go runListener(listener, logger, ts.clientHandler) // stops when listener closes
	return ts, nil
}

func (s *ThreadedService[K, V]) clientHandler(conn io.ReadWriteCloser) {

	logger := s.namespace.logger.With(
		s.namespace.Name(),
		"service",
		s.name,
		"client handler",
	)

	bufPtr := s.namespace.bufferPool.Get().(*[]byte)
	defer s.namespace.bufferPool.Put(bufPtr)

	handleCallerRequest(conn, s.keySerializer, s.valueSerializer, s.namespace.stringSerializer, *bufPtr, s.processRequest, logger)

}

func (ts *ThreadedService[K, V]) processRequest(key K) serviceOutput[V] {
	result, err := ts.handler(key)
	return serviceOutput[V]{data: result, err: err}
}

func (ts *ThreadedService[K, V]) Close() {
	ts.cancel()
}
