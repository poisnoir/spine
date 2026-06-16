package spine

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/poisnoir/mad-go"
)

type Service[K any, V any] struct {
	namespace *Namespace
	name      string

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]

	context  context.Context
	listener net.Listener
	cancel   context.CancelFunc

	handler  func(K) (V, error)
	requests chan serviceRequest[K, V]
}

func NewService[K any, V any](namespace *Namespace, name string, handler func(K) (V, error)) (*Service[K, V], error) {

	// fix me pls
	logger := namespace.logger

	keySer, valueSer, listener, err := generateService[K, V](namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
	}

	ctx, cancel := context.WithCancel(namespace.ctx)

	s := &Service[K, V]{
		namespace: namespace,
		name:      name,

		keySerializer:   keySer,
		valueSerializer: valueSer,

		context:  ctx,
		cancel:   cancel,
		listener: listener,

		requests: make(chan serviceRequest[K, V], 100),
		handler:  handler,
	}

	go s.runHandler()
	go runListener(listener, logger, s.clientHandler) // stops when listener closes
	return s, nil
}

func (s *Service[K, V]) clientHandler(conn io.ReadWriteCloser) {

	logger := s.namespace.logger.With(
		s.namespace.Name(),
		"service",
		s.name,
		"client handler",
	)

	bufPtr := s.namespace.bufferPool.Get().(*[]byte)
	defer s.namespace.bufferPool.Put(bufPtr)

	handleCallerRequest(
		conn,
		s.keySerializer,
		s.valueSerializer,
		s.namespace.stringSerializer,
		*bufPtr,
		s.processRequest,
		logger,
	)

}

func (s *Service[K, V]) processRequest(key K) serviceOutput[V] {
	// send to handler
	hr := serviceRequest[K, V]{
		input:  key,
		output: make(chan serviceOutput[V], 1),
	}

	// todo: need some timeout shit
	s.requests <- hr
	return <-hr.output
}

func (s *Service[K, V]) runHandler() {

	logger := s.namespace.logger.With(
		s.namespace.Name(),
		"service",
		s.name,
		"request handler",
	)

	for {
		select {
		case request := <-s.requests:
			response, err := s.handler(request.input)
			if err != nil {
				logger.Error("unable to handle request", "error", err)
				// Todo: Change To return error instead of default
			}
			request.output <- serviceOutput[V]{data: response, err: err}
			s.namespace.logger.Info("handled request", "request", request.input, "response", response)
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
