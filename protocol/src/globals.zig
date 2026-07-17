// Shared spine wire protocol: the single Unix socket both spine nodes and
// squid dial to reach spined, the fixed string size used for every name on
// the wire, and every command/status/type code and array bound both sides
// must agree on numerically. mad encodes everything as fixed-size fields
// with no framing or length prefix, so a mismatch here is a silent wire
// break, not a compile error — squid, spined, and every spine client
// library all import this one copy instead of keeping their own in sync by
// hand.
pub const SPINED_PATH = "/tmp/spine/spined";
pub const PUBLISHER_SOCKET_DIR = "/tmp/spine/publisher/";
pub const SERVICE_SOCKET_DIR = "/tmp/spine/service/";

pub const STRING_SIZE: u32 = 32;

// Top-level command byte, sent as the very first byte on every SPINED_PATH
// connection, telling spined what the rest of the connection is:
//   - REGISTER_NODE_CODE: a spine node registering itself (RegisterNodePayload
//     follows, then any number of entity registration/unregistration
//     messages on the same connection - see namespace.zig's op codes below).
//   - ADD_NAMESPACE_CODE / GET_INFO_CODE: a one-shot squid control command.

pub const ADD_NAMESPACE_CODE: u8 = 0;
pub const REMOVE_NAMESPACE_CODE: u8 = 2;
pub const GET_INFO_CODE: u8 = 4;
pub const REGISTER_NODE_CODE: u8 = 6;

// squid control-command responses.
pub const OK_STATUS: u8 = 0;
pub const TOO_MANY_NAMESPACES: u8 = 240;
pub const NAMESPACE_ALREADY_REGISTERED: u8 = 241;
pub const INVALID_COMMAND: u8 = 246;
pub const ERROR_MISMATCH_PAYLOAD_CODE: u8 = 254;

pub fn statusMessage(status: u8) []const u8 {
    return switch (status) {
        OK_STATUS => "ok",
        TOO_MANY_NAMESPACES => "spined already has the maximum number of namespaces",
        NAMESPACE_ALREADY_REGISTERED => "a namespace with that name already exists",
        INVALID_COMMAND => "spined did not recognize that command",
        else => "unknown error",
    };
}

// GetInfoResponse array bounds. Fixed at compile time since mad has no
// dynamic-length encoding — these size the live storage in spined too, so
// nothing here can silently truncate a namespace/node/lookup list.
pub const MAX_NAMESPACES: u32 = 32;
pub const MAX_NODES: u32 = 256;
pub const MAX_ENTITIES: u32 = 256;
pub const MAX_SUBSCRIBERS_PER_PUBLISHER: usize = 32;

// Node/entity-registration protocol: spine nodes speak this to spined over
// SPINED_PATH (RegisterNodePayload, then any number of entity registration
// messages). Shared between spined and every spine client library so they
// can't drift the way spine-zig's own STRING_SIZE=64 copy once did.
pub const NODE_ALREADY_REGISTERED: u8 = 249;
pub const TOO_MANY_NODES: u8 = 250;
pub const INVALID_NAMESPACE: u8 = 255;

pub const TOO_MANY_UNKNOWN_ENTITIES: u8 = 248;
pub const TOO_MANY_ENTITIES: u8 = 251;
pub const INVALID_ENTITY_TYPE: u8 = 252;
pub const ENTITY_ALREADY_REGISTERED: u8 = 253;
pub const ERROR_SERIALIZER_ERROR_CODE: u8 = 251;
pub const ERROR_SERVICE_ERROR_CODE: u8 = 252;

// Entity-registration operation codes: the first byte of each registration
// message, telling spined which payload shape (RegisterPublisherPayload/
// RegisterServicePayload/RegisterConsumerPayload) follows - mirrors how
// squid's ADD_NAMESPACE_CODE/GET_INFO_CODE/etc. already work, rather than
// spined reading one fixed shape and branching on a field inside it.
//
// Three codes, not four: entity.zig's domain model already unifies
// subscriber/service_caller as one Consumer (name + which kind of producer
// it's waiting for), so the wire protocol mirrors that directly instead of
// carrying a 4th, mostly-empty payload shape for it.
pub const REGISTER_PUBLISHER_CODE: u8 = 0;
pub const REGISTER_SERVICE_CODE: u8 = 1;
pub const REGISTER_CONSUMER_CODE: u8 = 2;

// UNREGISTER_CONSUMER_CODE covers both subscriber and service_caller.
// Idempotent - unregistering a name that was never registered (or already
// removed) is OK_STATUS, not an error, since the caller's intent ("this
// shouldn't exist") is already satisfied either way.
pub const UNREGISTER_PUBLISHER_CODE: u8 = 3;
pub const UNREGISTER_SERVICE_CODE: u8 = 4;
pub const UNREGISTER_CONSUMER_CODE: u8 = 5;

pub const MAX_PACKET_SIZE: usize = 4096;

pub const REGISTER_ENTITY: u8 = 80;
pub const BAD_NAME: u8 = 254;

pub const MAX_FAKE_ENTITIES: u32 = 256;
pub const FAKE_NODE = "SPINED_FAKE_NODE";
