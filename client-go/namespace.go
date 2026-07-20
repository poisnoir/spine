package spine

import (
	"errors"
	"io"
	"net"

	"github.com/poisnoir/spine-go/client-go/internal/globals"
	"github.com/poisnoir/spine-go/client-go/internal/mad"
)

// Errors returned by AddNamespace when spined rejects the request - mirrors
// client-zig's node.zig NamespaceError, and node.go's ErrEntityAlreadyRegistered
// et al. above.
var (
	ErrNamespaceAlreadyRegistered = errors.New("spined: namespace already exists")
	ErrTooManyNamespaces          = errors.New("spined: too many namespaces")
	ErrUnexpectedNamespaceStatus  = errors.New("spined: unexpected status adding namespace")
)

// Mirrors protocol/src/payloads.zig's CreateNamespacePayload.
type createNamespacePayload struct {
	name spineString
}

// Wire-format mirrors of protocol/src/payloads.zig's NodeInfo/NamespaceInfo/
// GetInfoResponse - field names/casing kept identical to the Zig side on
// purpose (see node.go's RegisterPublisherPayload etc. for why): mad sorts
// struct fields alphabetically by name to decide wire order, and that sort
// runs independently on each side, so matching names is what keeps the two
// sides landing on the same order without either side ever transmitting a
// field name. These stay unexported - GetInfo below decodes into them, then
// copies the result into the public Info/NamespaceInfo/NodeInfo types.
type nodeInfoWire struct {
	name spineString
}

type namespaceInfoWire struct {
	name     spineString
	node_num uint32
	nodes    [globals.MAX_NODES]nodeInfoWire
}

type getInfoResponseWire struct {
	namespace_num uint32
	namespaces    [globals.MAX_NAMESPACES]namespaceInfoWire
}

// NodeInfo/NamespaceInfo/Info are GetInfo's public result tree - ordinary,
// idiomatic Go types decoupled from the wire-format structs above, the same
// way squid's own printInfo (squid/src/main.zig) walks a decoded
// GetInfoResponse rather than exposing its raw fixed-size-array shape to
// callers directly.
type NodeInfo struct {
	Name string
}

type NamespaceInfo struct {
	Name  string
	Nodes []NodeInfo
}

type Info struct {
	Namespaces []NamespaceInfo
}

// AddNamespace creates a namespace on spined - the same operation squid's
// own `squid add namespace <name>` performs (squid/src/main.zig's
// addNamespace), exposed here as a library call. A free function, not a
// Node method: namespace creation isn't scoped to any particular node - it
// doesn't touch a node's own registration connection or identity at all,
// the same reason squid itself has no Node concept to hang this off of (see
// spined/readme.md - ADD_NAMESPACE_CODE is its own one-shot connection,
// unrelated to a node's REGISTER_NODE_CODE session). Works even if no Node
// has ever been created, as long as spined itself is reachable.
func AddNamespace(name string) error {
	conn, err := net.Dial("unix", "/tmp/spine/spined")
	if err != nil {
		return err
	}
	defer conn.Close()

	ser, err := mad.NewMad[createNamespacePayload]()
	if err != nil {
		return err
	}

	var buf [256]byte
	buf[0] = globals.ADD_NAMESPACE_CODE
	payload := createNamespacePayload{name: newSpinedString(name)}
	if err := ser.Encode(&payload, buf[1:]); err != nil {
		return err
	}

	if _, err := conn.Write(buf[:1+ser.GetRequiredSize()]); err != nil {
		return err
	}

	var status [1]byte
	if _, err := io.ReadFull(conn, status[:]); err != nil {
		return err
	}

	switch status[0] {
	case globals.OK_STATUS_CODE:
		return nil
	case globals.NAMESPACE_ALREADY_REGISTERED:
		return ErrNamespaceAlreadyRegistered
	case globals.TOO_MANY_NAMESPACES:
		return ErrTooManyNamespaces
	default:
		return ErrUnexpectedNamespaceStatus
	}
}

// GetInfo fetches every namespace/node spined currently knows about - the
// same operation squid's own `squid info` performs, exposed here as a
// library call. Same rationale as AddNamespace above for being a free
// function rather than a Node method.
//
// GetInfoResponse is ~270KB fully populated (see spined.zig's
// handle_get_info), comfortably larger than a single Unix-socket Read is
// guaranteed to return in one call, so this uses io.ReadFull rather than a
// bare conn.Read - unlike AddNamespace's one-byte status reply, the wire
// response here is too big to risk a short read going unnoticed.
func GetInfo() (*Info, error) {
	conn, err := net.Dial("unix", "/tmp/spine/spined")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{globals.GET_INFO_CODE}); err != nil {
		return nil, err
	}

	ser, err := mad.NewMad[getInfoResponseWire]()
	if err != nil {
		return nil, err
	}

	raw := make([]byte, ser.GetRequiredSize())
	if _, err := io.ReadFull(conn, raw); err != nil {
		return nil, err
	}

	var wire getInfoResponseWire
	if err := ser.Decode(raw, &wire); err != nil {
		return nil, err
	}

	info := &Info{Namespaces: make([]NamespaceInfo, 0, wire.namespace_num)}
	for i := uint32(0); i < wire.namespace_num; i++ {
		ns := &wire.namespaces[i]
		nsInfo := NamespaceInfo{Name: ns.name.String(), Nodes: make([]NodeInfo, 0, ns.node_num)}
		for j := uint32(0); j < ns.node_num; j++ {
			nsInfo.Nodes = append(nsInfo.Nodes, NodeInfo{Name: ns.nodes[j].name.String()})
		}
		info.Namespaces = append(info.Namespaces, nsInfo)
	}
	return info, nil
}
