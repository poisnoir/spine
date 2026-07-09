package spine

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
)

type Subscriber[K any] struct {
	node         *Node
	subscribedTo string

	conn        net.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	isConnected bool

	mutex    sync.RWMutex
	lastData K
	pushSig  chan struct{}

	serializer *mad.Mad[K]
}

func NewSubscriber[K any](node *Node, topic string) (*Subscriber[K], error) {

	decoder, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	err = node.registerToSpined(topic, globals.SUBSCRIBER_TYPE)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(node.ctx)

	sub := &Subscriber[K]{
		node:         node,
		subscribedTo: topic,

		ctx:         ctx,
		cancel:      cancel,
		isConnected: false,

		pushSig: make(chan struct{}, 1),

		serializer: decoder,
	}

	go sub.run()
	return sub, nil
}

func (s *Subscriber[K]) Get() (K, error) {
	var zero K
	select {
	case <-s.ctx.Done():
		return zero, s.ctx.Err()
	case <-s.pushSig:
		s.mutex.RLock()
		snap := s.lastData
		s.mutex.RUnlock()
		return snap, nil
	}
}

func (s *Subscriber[K]) run() {

	bufPtr := s.node.bufferPool.Get().(*[]byte)
	defer s.node.bufferPool.Put(bufPtr)
	buf := *bufPtr

	var data K

	for {
		select {
		case <-s.ctx.Done():
			if s.conn != nil {
				s.conn.Close()
			}
			return
		default:
			if !s.isConnected {
				bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), s.ctx)
				err := backoff.Retry(s.connect, bo)
				if err != nil {
					return
				}
				continue
			}

			n, err := s.conn.Read(buf)
			if err != nil {
				s.node.logger.Warn("subscriber connection lost", "topic", s.subscribedTo, "error", err)
				s.isConnected = false
				if s.conn != nil {
					s.conn.Close()
				}
				continue
			}

			err = s.serializer.Decode(buf[:n], &data)
			if err != nil {
				s.node.logger.Error("subscriber decode failed", "topic", s.subscribedTo, "error", err)
				continue
			}

			s.mutex.Lock()
			s.lastData = data
			s.mutex.Unlock()

			select {
			case s.pushSig <- struct{}{}:
			default:
			}
		}
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
		return err
	}

	_, err = conn.Read(buf)
	if err != nil {
		logger.Error("failed to read from socket", "error", err)
	} else if buf[0] != globals.OK_STATUS_CODE {
		err = fmt.Errorf("publisher data type is different.")
		logger.Error("failed to validate publisher input type", "error", err)
		return err
	}

	s.conn = conn
	s.isConnected = true

	return nil
}

func (s *Subscriber[K]) Close() error {
	s.cancel()
	return nil
}

func (s *Subscriber[K]) SubscribedTo() string {
	return s.subscribedTo
}
