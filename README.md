# spine

Monorepo for the spine control plane and its client libraries: `spined` (the
per-machine registry daemon), `spine-uart` (a supervised peer-bridge process
for UART devices), and two wire-compatible node client libraries —
`client-zig` and `client-go`. The CLI that used to talk to `spined` (`squid`)
has been folded into the client libraries themselves (`spine.addNamespace`/
`spine.getInfo` in client-zig).

```
spine/
  protocol/           shared wire-protocol module: mad codec, string type,
                      every command/status/entity-type code, and every
                      payload struct (spined's single control socket, node/
                      entity registration and unregistration, GetInfoResponse).
  spined/             the daemon
  spine-uart/         standalone UART peer-bridge process (stub, see its own readme)
  client-zig/         the Zig node client library, exported as the `spine` module
  client-go/          the Go node client library, wire-compatible with client-zig
  integration-tests/  spawns a real spined binary and drives it with
                      client-zig, over the actual Unix sockets
```

- [spined/readme.md](spined/readme.md) — the registry daemon and wire protocol
- [client-zig/readme.md](client-zig/readme.md) — the Zig `spine` module
- [client-go/README.md](client-go/README.md) — the Go `spine` module

## Building

The Zig components (`spined`, `spine-uart`, `client-zig`, and the
test suites) are built together from the repo root:

```sh
zig build                    # produces zig-out/bin/{spined,spine,spine_bench,spine_uart}
zig build test               # protocol's mad.zig tests + spined's namespace tests +
                              # client-zig's pubsub/service tests + compile-checks spine-uart
zig build test-integration   # spawns a real spined binary and drives it with
                              # client-zig, over the actual Unix sockets (needs `zig build` first)
zig build run-spined
zig build run-client-zig -- publish   # or subscribe | service | call | call-loop
zig build bench                       # client-zig's pub/sub + service benchmarks
```

`client-go` is a separate Go module (its own `go.mod`) — see
[client-go/README.md](client-go/README.md) for building and testing it.
