package spine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"

	"github.com/poisnoir/spine-go/internal/mad"
	"github.com/poisnoir/spine-go/internal/globals"
)

// bunch of same operations in service and threaded service

func generateService[K any, V any](node *Node, name string) (*mad.Mad[K], *mad.Mad[V], net.Listener, error) {
	logger := node.logger.With(
		node.Name(),
		"service",
		name,
		"new service",
	)

	keyEnc, err := mad.NewMad[K]()
	if err != nil {
		logger.Error("unable to create key encoder", "error", err)
		return nil, nil, nil, err
	}

	valueEnc, err := mad.NewMad[V]()
	if err != nil {
		logger.Error("unable to create value encoder", "error", err)
		return nil, nil, nil, err
	}

	// Register with spined first, same as Publisher (publisher.go) - a
	// genuine duplicate service name is then rejected by spined's own check
	// before any OS-level bind is attempted, rather than surfacing as a
	// generic listener-creation error.
	err = node.registerService(name, madTypeOf(keyEnc), madTypeOf(valueEnc))
	if err != nil {
		logger.Error("unable to register service with spined", "error", err)
		return nil, nil, nil, err
	}

	socketPath := "/tmp/spine/service/" + node.namespace + "/" + name
	listener, err := createListener(socketPath)
	if err != nil {
		logger.Error("unable to create listener", "error", err)
		// spined already approved this name - undo that now, or it's left
		// believing a service is registered with nothing behind it.
		node.unregisterService(name)
		return nil, nil, nil, err
	}

	return keyEnc, valueEnc, listener, nil

}

func establishConnection(conn io.ReadWriteCloser, keyCode []byte, valueCode []byte, buf []byte, logger *slog.Logger) error {
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	if !slices.Equal(keyCode, buf[:n]) {
		logger.Error("failed to establish connectsion")
		return fmt.Errorf("invalid key code")
	}
	_, err = conn.Write([]byte{globals.OK_STATUS_CODE})
	if err != nil {
		logger.Error("failed to establish connection")
		return err
	}

	n, err = conn.Read(buf)
	if err != nil {
		logger.Error("failed to establish connection")
		return err
	}

	if !slices.Equal(valueCode, buf[:n]) {
		logger.Error("failed to establish connection, invalid value code")
		return fmt.Errorf("invalid value code")
	}
	_, err = conn.Write([]byte{globals.OK_STATUS_CODE})

	return err
}

// BUGFIX: dropped the stringSerializer param. mad has no string/slice support, so
// it was always passed as nil and crashed with a nil-pointer deref the first time
// a handler returned an error (see the res.err branch below).
func handleCallerRequest[K any, V any](conn io.ReadWriteCloser, keySerializer *mad.Mad[K], valueSerializer *mad.Mad[V], buf []byte, processRequest func(K) serviceOutput[V], logger *slog.Logger) {

	defer conn.Close()
	err := establishConnection(conn, []byte(keySerializer.Code()), []byte(valueSerializer.Code()), buf, logger)
	if err != nil {
		logger.Error("failed to stablish connection", "error", err)
		return
	}

	for {
		n, err := conn.Read(buf)
		if err != nil {
			logger.Error("unable to read from connection", "error", err)
			return
		}

		var key K
		err = keySerializer.Decode(buf[:n], &key)
		if err != nil {
			logger.Error("unable to decode key", "error", err)
			conn.Write([]byte{globals.ERROR_SERIALIZER_ERROR_CODE})
			continue
		}

		res := processRequest(key)
		if res.err != nil {
			// BUGFIX: this branch logged the wrong variable (err, which was nil/stale
			// from the Decode above, instead of res.err), encoded the error message via
			// a stringSerializer that was always nil (crash), and had no continue/return,
			// so it fell through and wrote a second bogus OK response for the same request.
			logger.Error("handler failed", "error", res.err)
			_, err = conn.Write([]byte{globals.ERROR_SERVICE_ERROR_CODE})
			if err != nil {
				logger.Error("failed to write from connection", "error", err)
				return
			}
			continue
		}

		buf[0] = globals.OK_STATUS_CODE
		valueSerializer.Encode(&res.data, buf[1:])
		_, err = conn.Write(buf[:valueSerializer.GetRequiredSize()+1])
		if err != nil {
			logger.Error("failed to write from connection", "error", err)
			return
		}

	}
}

type serviceRequest[K any, V any] struct {
	ctx    context.Context
	input  K
	output chan serviceOutput[V]
}

type serviceOutput[V any] struct {
	data V
	err  error
}
