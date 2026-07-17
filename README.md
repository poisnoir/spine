# spine

Monorepo for the spine control plane and its client libraries: `spined` (the
per-machine registry daemon), `squid` (the CLI that talks to it), `spine-uart`
(a supervised peer-bridge process for UART devices), and two wire-compatible
node client libraries — `client-zig` and `client-go`.

```
spine/
  protocol/           shared wire-protocol module: mad codec, string type,
                      every command/status/entity-type code, and every
                      payload struct (spined's single control socket, node/
                      entity registration and unregistration, GetInfoResponse).
  spined/             the daemon
  squid/              the CLI
  spine-uart/         standalone UART peer-bridge process (stub, see its own readme)
  client-zig/         the Zig node client library, exported as the `spine` module
  client-go/          the Go node client library, wire-compatible with client-zig
  integration-tests/  spawns real spined/squid binaries and drives them with
                      client-zig, over the actual Unix sockets
```

- [spined/readme.md](spined/readme.md) — the registry daemon and wire protocol
- [squid/README.md](squid/README.md) — the CLI: commands and flags
- [client-zig/readme.md](client-zig/readme.md) — the Zig `spine` module
- [client-go/README.md](client-go/README.md) — the Go `spine` module

## Building

The Zig components (`spined`, `squid`, `spine-uart`, `client-zig`, and the
test suites) are built together from the repo root:

```sh
zig build                    # produces zig-out/bin/{spined,squid,spine,spine_bench,spine_uart}
zig build test               # protocol's mad.zig tests + spined's namespace tests +
                              # client-zig's pubsub/service tests + compile-checks squid/spine-uart
zig build test-integration   # spawns real spined/squid binaries and drives them with
                              # client-zig, over the actual Unix sockets (needs `zig build` first)
zig build run-spined
zig build run-squid -- info
zig build run-client-zig -- publish   # or subscribe | service | call | call-loop
zig build bench                       # client-zig's pub/sub + service benchmarks
```

`client-go` is a separate Go module (its own `go.mod`) — see
[client-go/README.md](client-go/README.md) for building and testing it.
