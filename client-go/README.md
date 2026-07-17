# client-go

client-go is the Go client library for spine, exported as the `spine` package — the same
node/pub-sub/RPC model as [client-zig](../client-zig/readme.md), wire-compatible with it, built on top
of its own reflection-based `mad` codec (`internal/mad`) that mirrors `protocol/src/mad.zig`'s field
layout exactly (fixed-size fields, alphabetical-by-name on the wire).

## Key philosophy: local-first, registry-assisted

A node always talks to other nodes directly over Unix domain sockets on the same machine — there is no
broker or relay in the data path, on the Pub/Sub or the RPC side. `spined`, the registry daemon, is
optional and purely local: if it's reachable at `/tmp/spine/spined`, a node registers itself and its
entities there so `squid info` and other nodes' duplicate-name checks can see them; if it isn't
reachable, `CreateNode` falls back to **local-only mode** silently — pub/sub and RPC between processes
on the same machine still work (those socket paths are a fixed convention, independent of spined), the
node just isn't discoverable and duplicate-entity checks don't happen. spined itself only listens on a
Unix socket today — see [spined's own limitations](../spined/readme.md#limitations--known-gaps) — so
none of this reaches across machines yet despite that being the eventual goal.

## Features

- **No IDL, no codegen**: define topics and services using native Go types and generics; `mad` reflects
  over the type at startup instead of requiring `.proto`/`.msg` files.
- **Compile-time type safety**: Go 1.18+ generics catch a `Publisher[K]`/`Subscriber[K]` or
  `Service[K, V]`/`ServiceCaller[K, V]` mismatch before it ever reaches the wire.
- **Resilient connectivity**: `Subscriber`/`ServiceCaller` reconnect with exponential backoff
  (`github.com/cenkalti/backoff/v4`) if the producer they're waiting for isn't up yet or drops.
- **Typed spined errors**: `CreateNode`/`NewPublisher`/`NewService`/`NewSubscriber`/`NewServiceCaller`
  return `errors.Is`-comparable sentinels (`ErrEntityAlreadyRegistered`, `ErrInvalidNamespace`, ...) when
  spined rejects a registration, not just an opaque error string.

`K`/`V` must be a fixed-size type `mad` supports — integers, floats, bools, arrays, and structs of
those. **No strings, slices, or maps** on either side of the wire — this is a deliberate, shared
limitation with client-zig/spined, not something client-go is missing on its own (see
`internal/mad/mad.go`'s type switch).

## Getting started

### 1. Create a node

All communication is scoped to a namespace. Only `"common"` exists by default (see
[spined's limitations](../spined/readme.md#limitations--known-gaps)).

```go
ctx := context.Background()
logger := slog.Default()

node, err := spine.CreateNode("common", "my_node", ctx, logger)
```

### 2. Pub/Sub

```go
pub, err := spine.NewPublisher[uint32](node, "temperature")
pub.Publish(21)

sub, err := spine.NewSubscriber[uint32](node, "temperature")
value, err := sub.Get() // blocks until the next published value
```

### 3. Services (RPC)

```go
handler := func(input uint32) (uint32, error) {
    return input * 2, nil
}
service, err := spine.NewService(node, "time_two", handler)

caller, err := spine.NewServiceCaller[uint32, uint32](node, "time_two")
result, err := caller.Call(21, ctx) // 42
```

`temperature` and `time_two` aren't special — they're just the same topic/service names
[client-zig's readme](../client-zig/readme.md) uses for its own demo, so either side is a drop-in
replacement for the other when testing cross-language compatibility. See `example/` for complete,
runnable versions of each of the above.

### Threaded vs. sequential services

`NewService` processes requests sequentially (one at a time, in registration order) — best for handlers
that touch shared state. `NewThreadedService` (`threaded_service.go`) processes requests in parallel —
best for stateless, CPU- or I/O-bound handlers. Both speak the identical wire protocol
(`service_common.go`), so a `client-zig` `ServiceCaller` can't tell which one it's talking to.

## Installation

```bash
go get github.com/poisnoir/spine-go/client-go
```

Build/test from this directory (it's a separate Go module from the rest of the monorepo):

```bash
go build ./...
go test ./...
go test -bench=. -benchmem ./...
```

## Benchmarks

Local Unix domain sockets, local-only mode (no spined running), AMD Ryzen 7 8845HS — same machine
[client-zig's benchmarks](../client-zig/readme.md#benchmarks) were run on, for reference. Single-machine
numbers with real run-to-run variance; re-run `go test -bench=. -benchmem` on your own hardware rather
than trusting these as a guarantee.

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `BenchmarkPubSub` | ~7,900 | 221 | 8 |
| `BenchmarkServiceCall` | ~14,300 | 417 | 12 |
| `BenchmarkServiceCallParallel` | ~13,800 | 416 | 12 |
| `BenchmarkThreadedServiceCall` | ~12,800 | 271 | 9 |
| `BenchmarkThreadedServiceCallParallel` | ~11,700 | 270 | 9 |

client-zig's equivalent benchmarks run noticeably faster (single-digit-μs pub/sub and service calls vs.
~8–14μs here) — not yet root-caused; a likely factor is `client-go`'s per-request `bufferPool`/reflection
path in `internal/mad` vs. `client-zig`'s comptime-generated encode/decode.

## Known gaps / design notes

- **No entity-level `Close()`**: a `Publisher`/`Subscriber`/`Service`/`ServiceCaller` lives as long as
  its node's `context.Context` does; cancel the node's context to stop everything at once rather than
  closing entities individually.
- **No OS-level duplicate-socket protection**: `createListener` (`network.go`) unconditionally removes
  whatever's at the target socket path before binding, rather than probing first the way
  `client-zig`'s `protocol.network.bind()` does. A genuine duplicate producer name is still caught by
  spined's own registration check (`ErrEntityAlreadyRegistered`) when spined is reachable, but there's
  no OS-level backstop the way client-zig has for the local-only-mode / spined-unreachable case.
- **`Subscriber`/`ServiceCaller` don't roll back their spined registration on connect failure**: they
  connect on a background goroutine (`go sub.run()`), not synchronously, so `NewSubscriber`/
  `NewServiceCaller` return as soon as registration succeeds — there's no synchronous failure point to
  hook an unregister call into the way `NewPublisher`/`NewService` do. `Publisher`/`Service` *do* roll
  back their registration if the local listener bind fails after spined already approved the name.

## Unit tests

`go test ./...` covers pub/sub (basic roundtrip, multiple subscribers) and service/caller (standard and
threaded, sequential calls, handler errors) against a real local socket, but always in local-only mode —
there's no equivalent of `../integration-tests/`'s spawn-a-real-spined harness here yet. `internal/mad`
has its own codec roundtrip tests, mirroring `protocol/src/mad.zig`'s.
