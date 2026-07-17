package globals

const MAX_PACKET_SIZE int = 4096

// Status Codes
const OK_STATUS_CODE uint8 = 0

// Top-level command byte, sent as the very first byte on every connection to
// spined's single socket ("/tmp/spine/spined") - mirrors
// protocol/src/globals.zig's REGISTER_NODE_CODE. spined and squid share that
// one socket now (ADD_NAMESPACE_CODE/GET_INFO_CODE are squid's own control
// commands on it); this tells spined the rest of the connection is a spine
// node registering itself (RegisterNodePayload follows).
const REGISTER_NODE_CODE uint8 = 6

// Entity-registration operation codes: the first byte of each registration
// message, telling spined which of RegisterPublisherPayload/
// RegisterServicePayload/RegisterConsumerPayload follows. Mirrors
// protocol/src/globals.zig's REGISTER_PUBLISHER_CODE/REGISTER_SERVICE_CODE/
// REGISTER_CONSUMER_CODE - three, not four, since a subscriber and a
// service_caller are both just a Consumer (name + which kind of producer
// it's waiting for) in spined's domain model now.
const REGISTER_PUBLISHER_CODE uint8 = 0
const REGISTER_SERVICE_CODE uint8 = 1
const REGISTER_CONSUMER_CODE uint8 = 2

// Rolls back a registration on the same connection - mirrors
// protocol/src/globals.zig's UNREGISTER_PUBLISHER_CODE/UNREGISTER_SERVICE_CODE/
// UNREGISTER_CONSUMER_CODE. Sent when a later step (binding the real
// listener, connecting to a producer) fails after spined already approved
// the registration, so spined doesn't keep believing an entity exists with
// nothing behind it. Idempotent on spined's side - unregistering a name
// that was never registered is not an error.
const UNREGISTER_PUBLISHER_CODE uint8 = 3
const UNREGISTER_SERVICE_CODE uint8 = 4
const UNREGISTER_CONSUMER_CODE uint8 = 5

const ERROR_SERIALIZER_ERROR_CODE uint8 = 251
const ERROR_SERVICE_ERROR_CODE uint8 = 252
const ERROR_MISMATCH_PAYLOAD_CODE uint8 = 254

// spined registration/entity status codes - mirrors protocol/src/globals.zig
// exactly, since these travel over the wire between spined and every client
// library, this one and client-zig alike.
const TOO_MANY_UNKNOWN_ENTITIES uint8 = 248
const NODE_ALREADY_REGISTERED uint8 = 249
const TOO_MANY_NODES uint8 = 250
const TOO_MANY_ENTITIES uint8 = 251
const INVALID_ENTITY_TYPE uint8 = 252
const ENTITY_ALREADY_REGISTERED uint8 = 253
const INVALID_NAMESPACE uint8 = 255

const ERROR_SERVICE_HANDLER = "service handler has an error"
const ERROR_CORRUPT_PAYLOAD = "CORRUPT_PAYLOAD"
const ERROR_PAYLOAD_SIZE = "failed to encode key. key is too big. max key size is 4kb"
