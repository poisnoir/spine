package spine

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/poisnoir/mad-go"
)

type Namespace struct {
	name string

	ctx context.Context

	logger           *slog.Logger
	bufferPool       sync.Pool
	stringSerializer *mad.Mad[string]

	spinedConn net.Conn
}

func JointNamespace(name string, ctx context.Context, logger *slog.Logger) (*Namespace, error) {

	conn, err := net.Dial("unix", "/tmp/spine/spined")
	if err != nil {
		logger.Warn("Could not connect to spined daemon. Operating in local-only mode.")
		conn = nil
	}

	stringSer, _ := mad.NewMad[string]()
	return &Namespace{
		name: name,

		ctx: ctx,

		logger: logger,
		bufferPool: sync.Pool{New: func() any {
			b := make([]byte, 4096)
			return &b
		}},
		stringSerializer: stringSer,

		spinedConn: conn,
	}, nil
}

func (ns *Namespace) Name() string {
	return ns.name
}

func registerServiceCaller(name string, inputCode string, outputCode string) error {
	return nil
}
