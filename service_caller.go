package spine

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
	"github.com/xtaci/kcp-go/v5"
)

type ServiceCaller[K any, V any] struct {
	namespace   *Namespace
	serviceName string

	keySerializer   *mad.Mad[K]
	valueSerializer *mad.Mad[V]
	errorSerializer *mad.Mad[string]

	ctx    context.Context
	cancel context.CancelFunc

	conn        *kcp.UDPSession
	requests    chan serviceRequest[K, V]
	isConnected bool
}

func NewServiceCaller[K any, V any](namespace *Namespace, serviceName string) (*ServiceCaller[K, V], error) {

	keySer, err := mad.NewMad[K]()
	if err != nil {
		return nil, err
	}

	valueSer, err := mad.NewMad[V]()
	if err != nil {
		return nil, err
	}

	errSer, _ := mad.NewMad[string]()

	ctx, cancel := context.WithCancel(namespace.ctx)

	sc := &ServiceCaller[K, V]{
		namespace:   namespace,
		serviceName: serviceName,

		keySerializer:   keySer,
		valueSerializer: valueSer,
		errorSerializer: errSer,

		ctx:    ctx,
		cancel: cancel,

		isConnected: false,
		requests:    make(chan serviceRequest[K, V], 100),
	}

	go sc.run()

	return sc, nil
}

func (sc *ServiceCaller[K, V]) run() {

	// timer heartbeat
	// make sure connection is open and alive
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sc.ctx.Done():
			sc.conn.Close()
			return
		default:
			if sc.isConnected {
				select {
				case <-ticker.C:
					if err := ping(sc.conn); err != nil {
						sc.isConnected = false
					}
				case requestData := <-sc.requests:
					// Postpone heartbeat
					ticker.Reset(10 * time.Second)
					output, err := sc.send(requestData.input)
					if err != nil {
						sc.isConnected = false
					}
					requestData.output <- serviceOutput[V]{data: output, err: err}
				}
			} else {
				bo := backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), sc.ctx)
				_ = backoff.Retry(sc.connect, bo)
			}
		}

	}
}

// send is the gateway to kcp connection. It acts as multiplexer
func (sc *ServiceCaller[K, V]) send(key K) (V, error) {
	var v V

	requestSize := sc.keySerializer.GetRequiredSize(&key) + 1
	if requestSize > globals.MAX_PACKET_SIZE {
		return v, fmt.Errorf(globals.ERROR_PAYLOAD_SIZE)
	}

	bufPtr := sc.namespace.bufferPool.Get().(*[]byte)
	defer sc.namespace.bufferPool.Put(bufPtr)
	buf := *bufPtr
	buf[0] = globals.SERVICE_REQUEST
	sc.keySerializer.Encode(&key, buf[1:])

	_, err := write(sc.conn, buf, requestSize, true)
	if err != nil {
		return v, err
	}

	if buf[0] != globals.OK_STATUS_CODE {
		var errMsg string
		_ = sc.errorSerializer.Decode(buf[1:], &errMsg)
		return v, fmt.Errorf("call error: %s", errMsg)
	}

	_ = sc.valueSerializer.Decode(buf[1:], &v)
	return v, nil
}

// Call sends key to the service and returns V from service
// context is used for establishing connection
func (sc *ServiceCaller[K, V]) Call(key K, ctx context.Context) (V, error) {

	var zero V
	data := serviceRequest[K, V]{
		input:  key,
		output: make(chan serviceOutput[V], 1),
	}
	sc.requests <- data

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case output := <-data.output:
		return output.data, output.err
	}
}

func (sc *ServiceCaller[K, V]) Close() {
	sc.cancel()
}

func (sc *ServiceCaller[K, V]) connect() error {

	logger := sc.namespace.logger.With(
		sc.namespace.Name(),
		"service_caller",
		sc.serviceName,
		"connect",
	)

	// finding the service
	address, err := sc.namespace.GetService(sc.serviceName, sc.ctx)
	if err != nil {
		logger.Error("unable to find the service", "error", err)
		return err // the only way to fail here is to run out of context
	}

	// establishing connection
	sess, err := kcp.DialWithOptions(address, sc.namespace.encryption, 10, 3)
	if err != nil {
		logger.Error("failed to dial service", "error", err)
		return err
	}

	// getting buffer for comm
	bufPtr := sc.namespace.bufferPool.Get().(*[]byte)
	defer sc.namespace.bufferPool.Put(bufPtr)
	buf := *bufPtr

	// validating input/output service types
	keyCode := sc.keySerializer.Code()
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

	valueCode := sc.valueSerializer.Code()
	n = copy(buf, valueCode)

	n, err = write(sess, buf, n, true)
	if err != nil {
		logger.Error("failed to validate service output type", "error", err)
	} else if n != 1 {
		err = fmt.Errorf("response is corrupted")
		logger.Error("failed to validate service output type", "error", err)
		return err
	} else if buf[0] != globals.OK_STATUS_CODE {
		err = fmt.Errorf("service data type is different")
		logger.Error("failed to validate service output type", "error", err)
		return err
	}
	sc.conn = sess
	sc.isConnected = true

	return nil

}
