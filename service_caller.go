package spine

import (
	"context"
	"fmt"
	"net"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
)

type ServiceCaller[K any, V any] struct {
	node        *Node
	serviceName string

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]

	ctx    context.Context
	cancel context.CancelFunc

	conn        net.Conn
	requests    chan serviceRequest[K, V]
	isConnected bool
}

func NewServiceCaller[K any, V any](node *Node, serviceName string) (*ServiceCaller[K, V], error) {

	keySer, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	valueSer, err := mad.NewMad[V]()
	if err != nil {
		return nil, err
	}

	err = node.registerToSpined(serviceName, globals.SERVICE_CALLER_TYPE)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(node.ctx)

	sc := &ServiceCaller[K, V]{
		node:        node,
		serviceName: serviceName,

		keySerializer:   keySer,
		valueSerializer: valueSer,

		ctx:    ctx,
		cancel: cancel,

		isConnected: false,
		requests:    make(chan serviceRequest[K, V], 100),
	}

	bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), sc.ctx)
	_ = backoff.Retry(sc.connect, bo)

	go sc.run()
	return sc, nil
}

func (sc *ServiceCaller[K, V]) run() {

	for {
		select {
		case <-sc.ctx.Done():
			if sc.conn != nil {
				sc.conn.Close()
			}
			return
		case requestData := <-sc.requests:
			if !sc.isConnected {
				bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), sc.ctx)
				_ = backoff.Retry(sc.connect, bo)
			}

			if err := requestData.ctx.Err(); err != nil {
				requestData.output <- serviceOutput[V]{err: err}
				continue
			}

			output, err := sc.send(requestData.input)
			if err != nil {
				sc.node.logger.Error("failed to send request", "error", err)
				sc.isConnected = false
			}
			requestData.output <- serviceOutput[V]{data: output, err: err}
		}
	}
}

// send is the gateway to kcp connection. It acts as multiplexer
func (sc *ServiceCaller[K, V]) send(key K) (V, error) {
	var v V

	requestSize := sc.keySerializer.GetRequiredSize()
	if requestSize > globals.MAX_PACKET_SIZE {
		return v, fmt.Errorf(globals.ERROR_PAYLOAD_SIZE)
	}

	bufPtr := sc.node.bufferPool.Get().(*[]byte)
	defer sc.node.bufferPool.Put(bufPtr)
	buf := *bufPtr

	sc.keySerializer.Encode(&key, buf)

	// _, err := write(sc.conn, buf, requestSize, true)
	_, err := sc.conn.Write(buf[:requestSize])
	if err != nil {
		return v, err
	}

	_, err = sc.conn.Read(buf)
	if err != nil {
		return v, err
	}

	if buf[0] != globals.OK_STATUS_CODE {
		// error messages can't be decoded here: mad has no string/slice support,
		// so the service side only ever sends a bare status code, not a message.
		return v, fmt.Errorf("call error: status %d", buf[0])
	}

	_ = sc.valueSerializer.Decode(buf[1:], &v)
	return v, nil
}

// Call sends key to the service and returns V from service
// context is used for establishing connection
func (sc *ServiceCaller[K, V]) Call(key K, ctx context.Context) (V, error) {

	var zero V
	data := serviceRequest[K, V]{
		ctx:    ctx,
		input:  key,
		output: make(chan serviceOutput[V], 1),
	}

	select {
	case sc.requests <- data:
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-sc.ctx.Done():
		return zero, sc.ctx.Err()
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-sc.ctx.Done():
		return zero, sc.ctx.Err()
	case output := <-data.output:
		return output.data, output.err
	}
}

func (sc *ServiceCaller[K, V]) Close() error {
	sc.cancel()
	return nil
}

func (sc *ServiceCaller[K, V]) connect() error {

	logger := sc.node.logger.With(
		sc.node.Name(),
		"service_caller",
		sc.serviceName,
		"connect",
	)

	// establishing connection
	conn, err := net.Dial("unix", "/tmp/spine/service/"+sc.node.namespace+"/"+sc.serviceName)
	if err != nil {
		logger.Error("failed to dial service", "error", err)
		return err
	}

	// getting buffer for comm
	bufPtr := sc.node.bufferPool.Get().(*[]byte)
	defer sc.node.bufferPool.Put(bufPtr)
	buf := *bufPtr

	// validating input/output service types
	keyCode := sc.keySerializer.Code()
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
		err = fmt.Errorf("service data type is different")
		logger.Error("failed to validate service input type", "error", err)
		return err
	}

	valueCode := sc.valueSerializer.Code()
	n = copy(buf, valueCode)

	_, err = conn.Write(buf[:n])
	if err != nil {
		logger.Error("failed to write into socket", "error", err)
		return err
	}
	_, err = conn.Read(buf)
	if err != nil {
		logger.Error("failed to read from socket", "error", err)
		return err
	} else if buf[0] != globals.OK_STATUS_CODE {
		err = fmt.Errorf("service data type is different")
		logger.Error("failed to validate service output type", "error", err)
		return err
	}
	sc.conn = conn
	sc.isConnected = true

	return nil
}
