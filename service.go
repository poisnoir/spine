package spine

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/poisnoir/mad-go"
)

type Service[K any, V any] struct {
	node *Node
	name string

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]

	context  context.Context
	listener net.Listener
	cancel   context.CancelFunc

	handler  func(K) (V, error)
	requests chan serviceRequest[K, V]
}

func NewService[K any, V any](node *Node, name string, handler func(K) (V, error)) (*Service[K, V], error) {

	keySer, valueSer, listener, err := generateService[K, V](node, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
	}

	ctx, cancel := context.WithCancel(node.ctx)

	s := &Service[K, V]{
		node: node,
		name: name,

		keySerializer:   keySer,
		valueSerializer: valueSer,

		context:  ctx,
		cancel:   cancel,
		listener: listener,

		requests: make(chan serviceRequest[K, V], 100),
		handler:  handler,
	}

	go s.runHandler()
	go runListener(listener, node.logger, s.clientHandler) // stops when listener closes
	return s, nil
}

func (s *Service[K, V]) clientHandler(conn io.ReadWriteCloser) {

	logger := s.node.logger.With(
		s.node.Name(),
		"service",
		s.name,
		"client handler",
	)

	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)

	handleCallerRequest(
		conn,
		s.keySerializer,
		s.valueSerializer,
		s.node.stringSerializer,
		*bufPtr,
		s.processRequest,
		logger,
	)

}

func (s *Service[K, V]) processRequest(key K) serviceOutput[V] {
	// send to handler
	hr := serviceRequest[K, V]{
		ctx:    s.context,
		input:  key,
		output: make(chan serviceOutput[V], 1),
	}

	select {
	case s.requests <- hr:
	case <-s.context.Done():
		return serviceOutput[V]{err: s.context.Err()}
	}

	select {
	case out := <-hr.output:
		return out
	case <-s.context.Done():
		return serviceOutput[V]{err: s.context.Err()}
	}
}

func (s *Service[K, V]) runHandler() {

	logger := s.node.logger.With(
		s.node.Name(),
		"service",
		s.name,
		"request handler",
	)

	for {
		select {
		case request := <-s.requests:
			if err := request.ctx.Err(); err != nil {
				continue
			}

			response, err := s.handler(request.input)
			if err != nil {
				logger.Error("handler error", "error", err)
			}
			request.output <- serviceOutput[V]{data: response, err: err}
			logger.Info("handled request", "request", request.input, "response", response)
		case <-s.context.Done():
			return
		}
	}
}

func (s *Service[K, V]) Close() {
	s.listener.Close()
	s.cancel()
}

func (s *Service[K, V]) Name() string {
	return s.name
}
