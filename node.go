package spine

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/poisnoir/mad-go"
	"github.com/poisnoir/spine-go/internal/globals"
)

type Node struct {
	namespace string
	name      string

	ctx context.Context

	logger           *slog.Logger
	bufferPool       sync.Pool
	stringSerializer *mad.Mad[string]

	spinedConn net.Conn
}

func CreateNode(namespace string, name string, ctx context.Context, logger *slog.Logger) (*Node, error) {

	conn, err := net.Dial("unixpacket", "/tmp/spine/spined")
	if err != nil {
		logger.Warn("Could not connect to spined daemon. Operating in local-only mode.")
		conn = nil
	}

	stringSer, _ := mad.NewMad[string]()
	return &Node{
		namespace: namespace,
		name:      name,

		ctx: ctx,

		logger: logger,
		bufferPool: sync.Pool{New: func() any {
			b := make([]byte, globals.MAX_PACKET_SIZE)
			return &b
		}},
		stringSerializer: stringSer,

		spinedConn: conn,
	}, nil
}

func (ns *Node) Name() string {
	return ns.name
}

func registerServiceCaller(name string, inputCode string, outputCode string) error {
	return nil
}
