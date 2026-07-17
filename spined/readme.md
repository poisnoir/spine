# Spined

Spined is the registry daemon for spine. It runs one per machine and lets the
spine nodes on that machine register themselves and the entities they expose
(publishers, subscribers, services, service callers) under a shared
namespace, so that nodes can find each other by name.

Nodes talk to spined over a single Unix domain socket at `/tmp/spine/spined`.
`squid` talks to that same socket for its own control commands (add
namespace, get info) — there is only one socket, dispatched by a leading
command byte (see Protocol below). Spined itself does not proxy or relay any
service/pub-sub traffic — that still happens directly between nodes over
their own Unix sockets (see [client-zig](../client-zig/readme.md) /
[client-go](../client-go/README.md)). Spined is purely a local name registry
today; there is no cross-machine transport yet.

## Building

```sh
zig build              # produces zig-out/bin/spined (run from the repo root)
zig build run-spined   # build and run
zig build test         # run the unit tests (see below)
```

## Protocol

All messages are encoded with `mad` (`protocol/src/mad.zig`), a small
reflection-free binary codec shared with `client-go` (`internal/mad`). Every
field is fixed-size — there is no string/slice/map support on either side, so
every payload has a size that's known at compile time. Struct fields are
encoded in alphabetical-by-name order, not declaration order.

Every connection to `/tmp/spine/spined` starts with one command byte
(`protocol/src/globals.zig`) telling spined what the rest of the connection
is:

| Code | Meaning |
| --- | --- |
| `0` (`ADD_NAMESPACE_CODE`) | squid: create a namespace (`CreateNamespacePayload`) |
| `2` (`REMOVE_NAMESPACE_CODE`) | reserved, not yet wired to a squid command |
| `4` (`GET_INFO_CODE`) | squid: dump every namespace/node (`GetInfoResponse`) |
| `6` (`REGISTER_NODE_CODE`) | a spine node registering itself |

A spine node registering itself speaks a longer, stateful exchange on that
one connection:

1. **Register the node** — send `REGISTER_NODE_CODE`, then a
   `RegisterNodePayload { namespace_name, node_name }`, and read back one
   status byte. Only the `"common"` namespace exists today (see
   Limitations).
2. **Register/unregister entities** — after a successful node registration,
   the connection stays open and the node sends any number of entity
   messages, each prefixed with its own one-byte op-code and answered with
   one status byte:

   | Op-code | Value | Payload |
   | --- | --- | --- |
   | `REGISTER_PUBLISHER_CODE` | `0` | `RegisterPublisherPayload { name, out_type }` |
   | `REGISTER_SERVICE_CODE` | `1` | `RegisterServicePayload { name, in_type, out_type }` |
   | `REGISTER_CONSUMER_CODE` | `2` | `RegisterConsumerPayload { name, is_service }` |
   | `UNREGISTER_PUBLISHER_CODE` | `3` | `UnregisterPayload { name }` |
   | `UNREGISTER_SERVICE_CODE` | `4` | `UnregisterPayload { name }` |
   | `UNREGISTER_CONSUMER_CODE` | `5` | `UnregisterConsumerPayload { name, is_service }` |

   Only three register codes, not four: a subscriber and a service caller are
   both just a `Consumer` (a name plus which kind of producer it's waiting
   for) in spined's domain model, so `is_service` is what distinguishes them
   on the wire rather than a separate payload shape. Registering a
   publisher/service whose name is already registered by another producer in
   the namespace is rejected with `ENTITY_ALREADY_REGISTERED` — consumers are
   exempt, any number of them may share a producer's name. Unregistering is
   idempotent: removing a name that was never registered (or already
   removed) still returns `OK_STATUS`, not an error — a client library uses
   this to roll back a registration if a later local step (binding the real
   socket, connecting to a producer) fails, so spined never ends up tracking
   an entity with nothing behind it.
3. **Disconnect** — when the connection closes, spined removes the node and
   every entity it registered from its namespace, scoped by that
   connection's own node id (one node can never remove another's entity).

Strings on the wire (`namespace_name`, `node_name`, entity `name`) are a
fixed `{ data: [32]u8, len: u8 }` struct (`STRING_SIZE`, `protocol/src/mad.zig`'s
`string` type), not a length-prefixed dynamic string.

### Status codes (`protocol/src/globals.zig`)

| Value | Meaning |
| --- | --- |
| `0` | OK |
| `240` | (squid) too many namespaces |
| `241` | (squid) a namespace with that name already exists |
| `246` | (squid) unrecognized command |
| `248` | Too many unknown entities |
| `249` | A node with that name is already registered in the namespace |
| `250` | Namespace already has `MAX_NODES` nodes |
| `251` | Namespace already has `MAX_ENTITIES` entities |
| `252` | Unknown entity op-code |
| `253` | A producer (publisher/service) with that name is already registered |
| `255` | Unknown namespace |

### Limits

Everything is a fixed-size array, sized at compile time in
`protocol/src/globals.zig` — there is no config file or CLI flag to change
these yet:

- `MAX_NAMESPACES = 32`
- `MAX_NODES = 256` per namespace
- `MAX_ENTITIES = 256` per namespace
- `MAX_SUBSCRIBERS_PER_PUBLISHER = 32`
- Names (`STRING_SIZE`) are capped at 32 bytes

## Flags

None. `spined` takes no command-line arguments — the socket path
(`/tmp/spine/spined`) and every limit above are compile-time constants.

## Limitations / known gaps

- **Local machine only**: spined only listens on a Unix domain socket. There
  is no TCP/network listener, so it cannot coordinate nodes across machines
  yet despite that being the eventual goal.
- **Only the `"common"` namespace is pre-registered**: `squid add namespace`
  can create more, but a node can only register itself into a namespace that
  already exists (`INVALID_NAMESPACE` otherwise).
- **No notification on late registration**: if a consumer registers before
  the producer it's waiting for exists, spined doesn't notify anyone when
  that producer later registers — client libraries handle this themselves by
  retrying the direct socket dial with backoff, not by asking spined.
- **UART peer bridging is being extracted, not wired in yet**: `spine-uart`
  (`../spine-uart/`) is a standalone process stub for driving a UART device
  in a separate, supervised subprocess (so a flaky device driver can't take
  spined down), but spined doesn't spawn or talk to it yet.

## Unit tests

`zig build test` runs `protocol/src/mad.zig`'s codec unit tests (encode/decode
roundtrips, wire-code generation for every supported type, alphabetical
field-sorting) and `namespace.zig`'s tests for the producer/consumer
registration and removal logic (`hasProducer`, `removeProducer`,
`removeConsumer`) — pure in-memory `Namespace` tests, no socket involved.
`main.zig`'s `test { _ = @import("spined.zig"); }` chain (and `spined.zig`'s
own `_ = @import("namespace.zig")`) forces those into the test build; without
it, `zig build test` would silently stop compiling/running them without
anything failing loudly. Real socket-level behavior (a node actually
registering, a duplicate producer actually being rejected before any bind is
attempted) is covered separately by `../integration-tests/`, which spawns a
real `spined` binary.
