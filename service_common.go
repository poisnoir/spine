package spine

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"

	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
)

// bunch of same operations in service and threaded service

func generateService[K any, V any](namespace *Namespace, name string) (*mad.Mad[K], *mad.Mad[V], net.Listener, error) {
	logger := namespace.logger.With(
		namespace.Name(),
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

	socketPath := "/tmp/spine/service/" + name
	listener, err := createListener(socketPath)
	if err != nil {
		logger.Error("unable to create listener", "error", err)
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

func handleCallerRequest[K any, V any](conn io.ReadWriteCloser, keySerializer *mad.Mad[K], valueSerializer *mad.Mad[V], stringSerializer *mad.Mad[string], buf []byte, processRequest func(K) serviceOutput[V], logger *slog.Logger) {

	defer conn.Close()
	err := establishConnection(conn, []byte(keySerializer.Code()), []byte(valueSerializer.Code()), buf, logger)
	if err != nil {
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
			logger.Error("handler failed", "error", err)
			errMsg := res.err.Error()
			buf[0] = globals.ERROR_SERVICE_ERROR_CODE
			stringSerializer.Encode(&errMsg, buf[1:])
			_, err = conn.Write(buf[:stringSerializer.GetRequiredSize(&errMsg)+1])
			if err != nil {
				logger.Error("failed to write from connection", "error", err)
				return
			}
		}

		buf[0] = globals.OK_STATUS_CODE
		valueSerializer.Encode(&res.data, buf[1:])
		_, err = conn.Write(buf[:valueSerializer.GetRequiredSize(&res.data)+1])
		if err != nil {
			logger.Error("failed to write from connection", "error", err)
			return
		}

	}
}

type serviceRequest[K any, V any] struct {
	input  K
	output chan serviceOutput[V]
}

type serviceOutput[V any] struct {
	data V
	err  error
}
