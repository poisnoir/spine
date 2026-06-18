package spine

import (
	"context"
	"fmt"
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

func (n *Node) registerToSpined(name string, elementType uint8) error {

	// if nodes are running locally
	if n.spinedConn == nil {
		return nil
	}

	bufPtr := n.bufferPool.Get().(*[]byte)
	defer n.bufferPool.Put(bufPtr)
	buf := *bufPtr

	buf[0] = globals.SPINED_REGISTER
	buf[1] = elementType
	n.stringSerializer.Encode(&name, buf[2:])

	_, err := n.spinedConn.Write(buf)
	if err != nil {
		return err
	}

	buf_size, err := n.spinedConn.Read(buf)
	if err != nil {
		return err
	}

	if buf[0] == globals.OK_STATUS_CODE {
		return nil
	}
	var errorMsg string
	_ = n.stringSerializer.Decode(buf[1:buf_size], &errorMsg)

	return fmt.Errorf("failed to register to spined: %s", errorMsg)
}
