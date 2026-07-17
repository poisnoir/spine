# client-zig

client-zig is the Zig client library for spine, exported as the `spine` module — the same
node/pub-sub/RPC model as [client-go](../client-go/README.md), wire-compatible with it,
built directly on top of the shared `protocol` module's `mad.zig` codec. It's newer and smaller in scope than
`client-go`: node registration, Publisher/Subscriber, and Service/ServiceCaller are implemented; there is no
`Node.deinit()`-driven cleanup story for entities yet (see Limitations), and no threaded-vs-sequential
service distinction (one `Service` implementation is wire-compatible with both of client-go's).

## Building

Run from the repo root (`spine/`), not this directory — this is a component of the `spine` monorepo,
built by its top-level `build.zig`.

```sh
zig build                          # produces zig-out/bin/spine (demo binary) and zig-out/bin/spine_bench
zig build run-client-zig -- <mode>  # publish | subscribe | service | call | call-loop (default: publish)
zig build test                     # run the unit tests (see below)
zig build bench                    # run the benchmarks (see below); add -Doptimize=ReleaseFast for real numbers
```

`zig build run -- publish`/`-- subscribe` talk to a `uint32` topic called `temperature` in the
`common` namespace — the same topic/type/namespace client-go's `example/publisher` and
`example/subscriber` use, so either side can be swapped for the other. `-- service`/`-- call` do the
same for a `uint32 -> uint32` service called `time_two`, matching client-go's `example/service` and
`example/service_caller`. `-- call-loop` calls repeatedly once a second instead of once — useful for
exercising reconnect live: start it against a running `-- service`, kill and restart the service, and
watch calls fail once then resume on their own.

## Usage

```zig
const spine = @import("spine");

var node = try spine.Node.init("common", "my-node", io, allocator);
defer node.deinit();

// Pub/Sub
const pub = try node.publish(u32, "temperature");
try pub.publish(21);

const sub = try node.subscribe(u32, "temperature");
const value = try sub.next(); // blocks until the next published value

// RPC
fn timesTwo(input: u32) anyerror!u32 {
    return input * 2;
}
_ = try node.newService(u32, u32, "time_two", timesTwo);

const caller = try node.newServiceCaller(u32, u32, "time_two");
const result = try caller.call(21); // 42
```

`K`/`V` can be any fixed-size type `mad` supports — integers, floats, bools, arrays, and structs of
those (alphabetically-by-field-name on the wire, same as client-go). No strings, slices, or maps, on
either side of the wire — this is a deliberate, shared limitation with client-go/spined, not something
client-zig is missing on its own.

If `spined` isn't reachable, `Node.init` falls back to **local-only mode** silently: the node still
works for pub/sub and RPC between processes on the same machine (those socket paths are a fixed
convention, independent of spined), it just isn't discoverable and duplicate namespace/entity checks
don't happen.

When `spined` is reachable, `publish()`/`newService()`/`subscribe()`/`newServiceCaller()` register the
entity with spined *before* claiming the local resource (binding a socket, connecting to a producer) —
a genuine duplicate producer name is rejected by spined's own check
(`EntityRegisterError.EntityAlreadyRegistered`) without ever touching a socket. If the local step fails
anyway for some unrelated reason, the registration is automatically rolled back (an `errdefer` sends an
unregister call), so spined never ends up tracking an entity with nothing behind it.

## Cross-language compatibility

Every entity type has been tested against the real client-go implementation, in both directions, as
separate processes (not just in-process unit tests):

- Zig `Publisher` ↔ Go `Subscriber`, and Go `Publisher` ↔ Zig `Subscriber`.
- Zig `Service` ↔ Go `ServiceCaller`, and Go `Service` ↔ Zig `ServiceCaller`.

One wire-compatibility detail worth knowing if you're debugging across languages: on a mad
type-fingerprint mismatch, client-go's `Service` sends **no response byte at all** — it just closes the
connection (see `service_common.go`'s `establishConnection`). client-zig's `Service` mirrors that exact
behavior rather than "improving" it, since a real client-go `ServiceCaller` only knows how to interpret
that specific shape of failure.

## Benchmarks

`zig build bench -Doptimize=ReleaseFast` (20,000 iterations each; AMD Ryzen 7 8845HS, local Unix domain
sockets, local-only mode / no spined running). These are single-machine numbers with real run-to-run
variance from background system load — typical vs. occasional-spike range shown below rather than one
cherry-picked run:

| Benchmark | Typical | Occasionally (under load) |
| --- | --- | --- |
| `BenchmarkPubSub` | ~1.8–2.0 μs/op | up to ~4 μs/op |
| `BenchmarkServiceCall` | ~7.0–7.5 μs/op | up to ~15 μs/op |
| `BenchmarkServiceCallParallel` (8 workers, one shared connection) | ~9.0–9.6 μs/op | up to ~17 μs/op |

## Known gaps / design notes

- **No `Close()`/`deinit()` on entities** (`Publisher`, `Subscriber`, `Service`, `ServiceCaller`) —
  deliberate for now, matching client-go (which dropped the same methods for the same reason): an entity
  is meant to live as long as its node does, the same way a long-running HTTP server doesn't get told to
  "stop" individual handlers. Real cleanup, at least for `Subscriber`/`ServiceCaller`, is coming back
  later, designed rather than bolted on.
- Because of the above, a process that creates a `Publisher`/`Service` can't return normally from
  `main()` — their accept loops run forever on `io`'s own thread pool, and `std.Io.Threaded.deinit()`
  (called by the zig runtime's own `start.zig` wrapper as part of a normal return) blocks joining them.
  Long-running demos handle this the same way an external `kill` already would: `main.zig` and
  `bench.zig` both call `std.process.exit(0)` explicitly instead of returning.
- `ServiceCaller.connect()`/`Subscriber.connect()` retry with exponential backoff (100ms → 5s cap) on
  transient failures (publisher/service not up yet), but return immediately — no retry — on a mad
  type-fingerprint mismatch, since that's permanent: `K`/`V` are fixed at compile time by the caller, so
  retrying can never fix it.
- `Publisher` caps concurrent subscribers at `globals.MAX_SUBSCRIBERS_PER_PUBLISHER` (32) — a fixed
  array, matching spined's own fixed-size-arrays style, rather than client-go's unbounded slice.
- **Reconnect-on-drop** is handled, but the two sides differ slightly since one has an idempotency
  concern and the other doesn't:
  - `Subscriber.next()` treats a broken connection as transient: it reconnects (same unlimited
    backoff as the initial connect) and keeps retrying the read internally, so a call to `next()`
    never surfaces a transient disconnect as an error — it just blocks a bit longer, the same way a
    real caller would experience a publisher restart. Reading has no side effects, so retrying inside
    the same call is free.
  - `ServiceCaller.call()` reconnects first if the last attempt marked the connection dead, but a
    failure *during* the current request still returns that request's own error rather than silently
    retrying it — a request may have already taken effect server-side by the time the failure is
    observed, so silently resending isn't safe. The *next* call reconnects and proceeds normally.
    Verified live: killing and restarting a real `spine service` process out from under a
    long-running `spine call-loop` caller, the caller logs the failure once and resumes on its
    own once the service comes back.

## Unit tests

`zig build test` covers pub/sub (basic roundtrip, multiple subscribers, ordering, struct payloads, a
mismatched-type rejection, dead-subscriber cleanup, reconnect-after-drop, producer already-running /
crash-recovery) and service/caller (basic call, sequential calls, handler errors, key/value type
mismatches, reconnect-after-drop) — 17 tests total (16 entity tests plus one `refAllDecls` compile-check
in `root.zig`), split by which entity's contract they verify: `publisher.zig` (broadcast semantics,
producer lifecycle), `subscriber.zig` (handshake rejection, reconnect, dialing a crashed producer),
`service.zig` (handler dispatch), and `service_caller.zig` (handshake rejection, reconnect). They share
one `std.Io.Threaded`/arena across all of them, defined once in `test_support.zig`, rather than one per
test: a `Publisher`/`Service`'s accept loop runs forever on a background thread (see Known gaps above),
so tearing down a per-test `Threaded` would mean joining a thread that's permanently blocked in
`accept()`.

One test-methodology note worth knowing if you're adding more of these: to simulate a connection dying,
never close a `net.Stream` you're about to reuse yourself — this Io backend correctly treats reusing an
fd after your own `close()` as a programmer bug (a hard panic, not a soft error), since that's not what
a real remote disconnect looks like. The pubsub reconnect test instead closes the *publisher's* accepted
copy of the connection (`publisher.clients[0]`) and clears it from `clients_num`, leaving the
subscriber's own fd genuinely valid so its next read sees a real EOF. `Service` doesn't expose its
accepted per-caller connections the way `Publisher` does, so the caller-reconnect test instead sets
`caller.is_connected = false` directly to exercise the same branch without touching a socket at all —
the real socket-level failure path is covered by live process testing instead (see Known gaps above).
