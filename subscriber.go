package spine

import (
	"context"
	"fmt"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
	"github.com/xtaci/kcp-go/v5"
)

type Subscriber[K any] struct {
	namespace    *Namespace
	subscribedTo string

	conn        *kcp.UDPSession
	ctx         context.Context
	cancel      context.CancelFunc
	isConnected bool

	mutex    sync.RWMutex
	lastData K
	handler  func(K)
	pushSig  chan struct{}

	serializer *mad.Mad[K]
}

func NewSubscriber[K any](namespace *Namespace, topic string, handler func(K)) (*Subscriber[K], error) {

	decoder, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(namespace.ctx)

	sub := &Subscriber[K]{
		namespace:    namespace,
		subscribedTo: topic,

		ctx:         ctx,
		cancel:      cancel,
		isConnected: false,

		handler: handler,
		pushSig: make(chan struct{}, 1),

		serializer: decoder,
	}

	go sub.run()
	go sub.runHandler()

	return sub, nil
}

func (s *Subscriber[K]) runHandler() {
	for {

		select {
		case <-s.ctx.Done():
			return
		case <-s.pushSig:
			s.mutex.RLock()
			snap := s.lastData
			s.mutex.RUnlock()
			s.handler(snap)
		}
	}
}

func (s *Subscriber[K]) run() {

	bufPtr := s.namespace.bufferPool.Get().(*[]byte)
	buf := *bufPtr

	var data K

	for {
		if s.isConnected {
			_, err := s.conn.Read(buf)
			if err != nil {
				s.isConnected = false
				continue
			}

			if buf[0] == globals.PING_CODE {
				_, err = s.conn.Write([]byte{globals.PONG_CODE})
				if err != nil {
					s.isConnected = false
				}
				continue
			}

			s.serializer.Decode(buf[1:], &data)

			s.mutex.Lock()
			s.lastData = data
			s.mutex.Unlock()

			select {
			case s.pushSig <- struct{}{}:
			default:
			}

		} else {
			bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), s.ctx)
			_ = backoff.Retry(s.connect, bo)
		}
	}
}

func (s *Subscriber[K]) connect() error {

	logger := s.namespace.logger.With(
		s.namespace.Name(),
		"subscriber",
		s.subscribedTo,
		"connect",
	)

	// finding the service
	address, err := s.namespace.GetService(s.subscribedTo, s.ctx)
	if err != nil {
		logger.Error("unable to find the service", "error", err)
		return err // the only way to fail here is to run out of context
	}

	// establishing connection
	sess, err := kcp.DialWithOptions(address, s.namespace.encryption, 10, 3)
	if err != nil {
		logger.Error("failed to dial service", "error", err)
		return err
	}

	// getting buffer for comm
	bufPtr := s.namespace.bufferPool.Get().(*[]byte)
	defer s.namespace.bufferPool.Put(bufPtr)
	buf := *bufPtr

	// validating input/output service types
	keyCode := s.serializer.Code()
	n := copy(buf, keyCode)

	n, err = write(sess, buf, n, true)
	if err != nil {
		logger.Error("failed to validate service input type", "error", err)
		return err
	} else if n != 1 {
		err = fmt.Errorf("response is corrupted")
		logger.Error("failed to validate service input type", "error", err)
		return err
	} else if buf[0] != globals.OK_STATUS_CODE {
		err = fmt.Errorf("service data type is different")
		logger.Error("failed to validate service input type", "error", err)
		return err
	}

	s.conn = sess
	s.isConnected = true

	return nil
}

func (s *Subscriber[K]) Stop() {
	s.cancel()
}

func (s *Subscriber[K]) SubscribedTo() string {
	return s.subscribedTo
}
