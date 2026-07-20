package spine

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"

	"github.com/poisnoir/spine-go/client-go/internal/mad"
	"github.com/poisnoir/spine-go/client-go/internal/globals"
)

// Errors returned by CreateNode when spined rejects a node registration -
// mirrors client-zig's node.zig RegisterError.
var (
	ErrInvalidNamespace      = errors.New("spined: invalid namespace")
	ErrNodeAlreadyRegistered = errors.New("spined: node already registered")
	ErrTooManyNodes          = errors.New("spined: too many nodes")
	ErrUnexpectedNodeStatus  = errors.New("spined: unexpected status registering node")
)

// Errors returned by registerPublisher/registerService/registerConsumer
// (and their unregister counterparts) when spined rejects an entity
// registration - mirrors client-zig's node.zig EntityRegisterError.
var (
	ErrTooManyEntities         = errors.New("spined: too many entities")
	ErrInvalidEntityType       = errors.New("spined: invalid entity type")
	ErrEntityAlreadyRegistered = errors.New("spined: entity already registered")
	ErrTooManyUnknownEntities  = errors.New("spined: too many unknown entities")
	ErrUnexpectedEntityStatus  = errors.New("spined: unexpected status registering entity")
)

// BUGFIX: was [64]uint8. spined's wire protocol moved its shared string size
// (protocol/src/globals.zig's STRING_SIZE) from 64 to 32 bytes when spined
// was consolidated into the poisnoir/spine monorepo — spine-go wasn't
// updated to match, so every node registration silently decoded garbage
// (a misaligned slice of this struct) and got rejected as INVALID_NAMESPACE,
// even for the real "common" namespace. Reproduced live: a real spined
// closes the connection right after this decodes wrong, and the next write
// (entity registration) hits the already-closed socket as "broken pipe".
type spineString struct {
	data [32]uint8
	len  uint8
}

func newSpinedString(s string) spineString {
	var ss spineString
	// copy() built-in safely copies bytes up to the size of the array
	length := copy(ss.data[:], s) //TODO: check me pls
	ss.len = uint8(length)
	return ss
}

// The inverse of newSpinedString - decodes a spineString read off the wire
// (e.g. inside a GetInfoResponse) back into a normal Go string. Used by
// namespace.go's GetInfo to build its public, idiomatic Info tree.
func (ss spineString) String() string {
	return string(ss.data[:ss.len])
}

// Mirrors protocol/src/payloads.zig's MadType - field names matter for wire
// compatibility here (mad sorts fields alphabetically by name on both
// sides, with no field names transmitted, so Go's sort order has to land on
// the same [string, uint32] layout Zig's does; keeping identical names is
// just for clarity, not strictly required).
type MadType struct {
	code         spineString
	requiredSize uint32
}

func madTypeOf[T any](ser *mad.Mad[T]) MadType {
	return MadType{
		code:         newSpinedString(string(ser.Code())),
		requiredSize: uint32(ser.GetRequiredSize()),
	}
}

// Mirrors protocol/src/payloads.zig's RegisterPublisherPayload/
// RegisterServicePayload/RegisterConsumerPayload: spined now reads a
// one-byte operation code first (globals.REGISTER_PUBLISHER_CODE/
// REGISTER_SERVICE_CODE/REGISTER_CONSUMER_CODE) telling it which of these
// three shapes follows, rather than one fixed shape with an entity_type
// field inside it. Consumers (subscriber/service_caller) don't carry a
// MadType at all - they're matched against a producer's, not their own.
type RegisterPublisherPayload struct {
	name     spineString
	out_type MadType
}

type RegisterServicePayload struct {
	name     spineString
	in_type  MadType
	out_type MadType
}

// is_service mirrors entity.zig's Consumer.producer_type: false means
// waiting for a publisher (a subscriber), true means waiting for a service
// (a service_caller).
type RegisterConsumerPayload struct {
	name       spineString
	is_service bool
}

type registernodePayload struct {
	namespace_name spineString
	node_name      spineString
}

// Mirrors protocol/src/payloads.zig's UnregisterPayload/
// UnregisterConsumerPayload - sent with UNREGISTER_PUBLISHER_CODE/
// UNREGISTER_SERVICE_CODE/UNREGISTER_CONSUMER_CODE to roll back a
// registration on the same connection.
type UnregisterPayload struct {
	name spineString
}

type UnregisterConsumerPayload struct {
	name       spineString
	is_service bool
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

		w_buf[0] = globals.REGISTER_NODE_CODE
		_ = ser.Encode(&payload, w_buf[1:])
		_, err = conn.Write(w_buf[:1+ser.GetRequiredSize()])

		if err != nil {
			return nil, err
		}

		_, err = conn.Read(w_buf[:])

		if err != nil {
			return nil, err
		}

		switch w_buf[0] {
		case globals.OK_STATUS_CODE:
		case globals.INVALID_NAMESPACE:
			return nil, ErrInvalidNamespace
		case globals.NODE_ALREADY_REGISTERED:
			return nil, ErrNodeAlreadyRegistered
		case globals.TOO_MANY_NODES:
			return nil, ErrTooManyNodes
		default:
			return nil, ErrUnexpectedNodeStatus
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

// A free function, not a method: Go methods can't introduce their own type
// parameters, only free functions can. Writes the one-byte operation code
// (telling spined which payload shape follows) and then the encoded
// payload itself; the status-handling is identical across all three
// registration kinds, only the outgoing payload shape differs.
func sendRegistration[T any](n *Node, opCode uint8, payload T) error {
	// if nodes are running locally
	if n.spinedConn == nil {
		return nil
	}

	ser, _ := mad.NewMad[T]()

	bufPtr := n.bufferPool.Get().(*[]byte)
	defer n.bufferPool.Put(bufPtr)
	buf := *bufPtr

	buf[0] = opCode
	ser.Encode(&payload, buf[1:])

	_, err := n.spinedConn.Write(buf[:1+ser.GetRequiredSize()])
	if err != nil {
		return err
	}

	_, err = n.spinedConn.Read(buf)
	if err != nil {
		return err
	}

	switch buf[0] {
	case globals.OK_STATUS_CODE:
		return nil
	case globals.TOO_MANY_ENTITIES:
		return ErrTooManyEntities
	case globals.INVALID_ENTITY_TYPE:
		return ErrInvalidEntityType
	case globals.ENTITY_ALREADY_REGISTERED:
		return ErrEntityAlreadyRegistered
	case globals.TOO_MANY_UNKNOWN_ENTITIES:
		return ErrTooManyUnknownEntities
	default:
		return ErrUnexpectedEntityStatus
	}
}

func (n *Node) registerPublisher(name string, outType MadType) error {
	return sendRegistration(n, globals.REGISTER_PUBLISHER_CODE, RegisterPublisherPayload{
		name:     newSpinedString(name),
		out_type: outType,
	})
}

func (n *Node) registerService(name string, inType MadType, outType MadType) error {
	return sendRegistration(n, globals.REGISTER_SERVICE_CODE, RegisterServicePayload{
		name:     newSpinedString(name),
		in_type:  inType,
		out_type: outType,
	})
}

func (n *Node) registerConsumer(name string, isService bool) error {
	return sendRegistration(n, globals.REGISTER_CONSUMER_CODE, RegisterConsumerPayload{
		name:       newSpinedString(name),
		is_service: isService,
	})
}

// unregisterPublisher/unregisterService/unregisterConsumer roll back a
// registration made moments ago on this same connection - callers use these
// to undo registerPublisher/registerService/registerConsumer when a later
// step (binding the real listener, connecting to a producer) fails, so
// spined doesn't keep believing an entity exists with nothing behind it.
// Mirrors client-zig's node.zig unregisterPublisher/unregisterService/
// unregisterConsumer.
func (n *Node) unregisterPublisher(name string) error {
	return sendRegistration(n, globals.UNREGISTER_PUBLISHER_CODE, UnregisterPayload{
		name: newSpinedString(name),
	})
}

func (n *Node) unregisterService(name string) error {
	return sendRegistration(n, globals.UNREGISTER_SERVICE_CODE, UnregisterPayload{
		name: newSpinedString(name),
	})
}

func (n *Node) unregisterConsumer(name string, isService bool) error {
	return sendRegistration(n, globals.UNREGISTER_CONSUMER_CODE, UnregisterConsumerPayload{
		name:       newSpinedString(name),
		is_service: isService,
	})
}
