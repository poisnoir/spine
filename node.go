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

type spineString struct {
	data [64]uint8
	len  uint8
}

func newSpinedString(s string) spineString {
	var ss spineString
	// copy() built-in safely copies bytes up to the size of the array
	length := copy(ss.data[:], s) //TODO: check me pls
	ss.len = uint8(length)
	return ss
}

type RegisterEntityPayload struct {
	entity_name spineString
	entity_type uint8
}

type registernodePayload struct {
	namespace_name spineString
	node_name      spineString
}

type Node struct {
	namespace string
	name      string

	ctx context.Context

	logger           *slog.Logger
	bufferPool       sync.Pool
	stringSerializer *mad.Mad[spineString]

	spinedConn net.Conn
}

func CreateNode(namespace string, name string, ctx context.Context, logger *slog.Logger) (*Node, error) {

	conn, err := net.Dial("unix", "/tmp/spine/spined")
	if err != nil {
		logger.Warn("Could not connect to spined daemon. Operating in local-only mode.")
		conn = nil
	} else {

		// TODO: this whole section has to be refactored

		ser, _ := mad.NewMad[registernodePayload]()

		var w_buf [1024]uint8

		payload := registernodePayload{
			namespace_name: newSpinedString(namespace),
			node_name:      newSpinedString(name),
		}

		_ = ser.Encode(&payload, w_buf[:])
		_, err = conn.Write(w_buf[:ser.GetRequiredSize()])

		if err != nil {
			return nil, err
		}

		_, err = conn.Read(w_buf[:])

		if err != nil {
			return nil, err
		}

	}

	stringSer, _ := mad.NewMad[spineString]()
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

func (n *Node) registerToSpined(name string, entity_type uint8) error {
	ser, _ := mad.NewMad[RegisterEntityPayload]()

	// if nodes are running locally
	if n.spinedConn == nil {
		return nil
	}

	payload := RegisterEntityPayload{
		entity_name: newSpinedString(name),
		entity_type: entity_type,
	}

	bufPtr := n.bufferPool.Get().(*[]byte)
	defer n.bufferPool.Put(bufPtr)
	buf := *bufPtr

	_ = ser.Encode(&payload, buf[:ser.GetRequiredSize()])

	_, err := n.spinedConn.Write(buf[:ser.GetRequiredSize()])
	if err != nil {
		return err
	}

	_, err = n.spinedConn.Read(buf)
	if err != nil {
		return err
	}

	if buf[0] == globals.OK_STATUS_CODE {
		return nil
	}

	return fmt.Errorf("failed to register to spined")
}
