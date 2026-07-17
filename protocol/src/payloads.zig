const globals = @import("globals.zig");
const mad = @import("mad.zig");
const string = mad.string;
const MadType = mad.MadType;

// squid <-> spined control-plane payloads. Field layout must match exactly
// on both ends (mad encodes struct fields sorted alphabetically by name),
// which is guaranteed now that both binaries import this one copy instead
// of keeping their own in sync by hand.
pub const CreateNamespacePayload = struct {
    name: string,
};

pub const NodeInfo = struct {
    name: string,
};

// No lookups here anymore - that was tied to the old lookup-manager
// mechanism (namespace.zig used to track "consumers with no matching
// producer yet" as a separate list); now that consumers/producers are
// tracked directly as entities, squid info has nothing separate to show
// for pending lookups.
pub const NamespaceInfo = struct {
    name: string,
    node_num: u32,
    nodes: [globals.MAX_NODES]NodeInfo,
};

pub const GetInfoResponse = struct {
    namespace_num: u32,
    namespaces: [globals.MAX_NAMESPACES]NamespaceInfo,
};

// spine node <-> spined registration payloads. A node sends
// RegisterNodePayload once on connecting to SPINED_PATH, then any number of
// entity registration messages on that same connection - each prefixed with
// one of REGISTER_PUBLISHER_CODE/REGISTER_SERVICE_CODE/REGISTER_CONSUMER_CODE
// (protocol/src/globals.zig) telling spined which of the three payloads
// below to decode next. Shared between spined and every spine client
// library (spine-zig, ...) so a client can't silently drift from what
pub const RegisterNodePayload = struct {
    namespace_name: string,
    node_name: string,
};

pub const RegisterPublisherPayload = struct {
    name: string,
    out_type: MadType = MadType{},
};

pub const RegisterServicePayload = struct {
    name: string,
    in_type: MadType = MadType{},
    out_type: MadType = MadType{},
};

// A consumer just names which producer it's waiting for and what kind of
// producer that is (mirrors entity.zig's Consumer.producer_type) - a
// subscriber is_service=false (wants a publisher), a service_caller
// is_service=true (wants a service). No MadType here: consumers don't
// publish a schema of their own, they're matched against a producer's.
pub const RegisterConsumerPayload = struct {
    name: string,
    is_service: bool,
};

// Rolls back a registration on the same connection (UNREGISTER_PUBLISHER_CODE/
// UNREGISTER_SERVICE_CODE/UNREGISTER_CONSUMER_CODE - protocol/src/globals.zig).
// UnregisterPayload covers publisher and service alike: the op-code already
// says which one, so the name is all spined needs to find and remove it.
pub const UnregisterPayload = struct {
    name: string,
};

// Consumer unregistration needs the same is_service disambiguator as
// RegisterConsumerPayload: a node could have both a subscriber and a
// service_caller registered under the same name, so the name alone isn't
// enough to know which entity to remove.
pub const UnregisterConsumerPayload = struct {
    name: string,
    is_service: bool,
};
