# TCP Peer Protocol

Design document for how two `spined` instances talk to each other over a TCP
peer link. No code yet — this is the design to build against. Compare with
[uart.md](uart.md) for the UART peer link, which is a meaningfully different
shape because TCP doesn't have UART's two big constraints: it's reliable
(the OS guarantees in-order, uncorrupted delivery or a clean disconnect —
no CRC/resync machinery needed), and it isn't a single shared wire (no
per-link bandwidth ceiling forcing on-demand-only forwarding).

Because bandwidth isn't scarce the way it is on a serial link, this design
doesn't bother with anything demand-driven — it mirrors *all* real producers
a shared namespace has, unconditionally, rather than only what's currently
wanted locally.

## Core idea (as given, refined below)

On connect, two peers exchange which namespaces they have in common, then
exchange their real producers within those namespaces. Wherever both sides
already have an equivalent producer, nothing new gets created. For
everything else, the receiving side materializes it locally as a fake
producer, all grouped under one synthetic node named `fake_tcp_node_<peer
name>` per peer link.

## Concepts

- **Shared namespace**: a namespace name that exists, locally registered,
  on *both* sides of the link. No separate allow-list/config — if both
  sides already have a namespace called `rime`, it's shared automatically;
  if only one side has it, there's nothing on the other end to sync it
  against, so it's skipped. (Flagging this as the simplest default — happy
  to add an explicit include/exclude list at `squid add peer` time later if
  "share everything with the same name" turns out to be too blunt.)
- **Similar producer**: same namespace, same name, same `ProducerType`
  (`.publisher` or `.service`). Only real producers count for this
  comparison and for what gets announced in the first place — a producer
  that's itself a fake mirrored in from a *different* peer is never
  re-announced onward. Otherwise namespace `common` shared across peers A–B
  and B–C could daisy-chain B's mirror of A's producer back out to C as if
  it were B's own, creating stale copies-of-copies and, in a ring topology,
  actual forwarding loops. If C wants A's producer, C should peer with A
  directly (or with whoever actually owns it).
- **Fake node**: exactly the existing `Node`/`EntitySlot` bookkeeping
  already in `namespace.zig`, just populated from a peer link instead of a
  real connecting process. One per peer link, named `fake_tcp_node_<peer
  name>`, holding every producer mirrored in from that peer as ordinary
  `EntitySlot`s with `node_id` set to the fake node's id. This is
  deliberately not a special code path — the existing per-node cleanup
  (`cleanNode`) already removes every entity belonging to a given
  `node_id`, so tearing down a whole peer's worth of mirrored producers on
  disconnect is just calling that same function with the fake node's id.
  No new cleanup logic needed.

## Phase 0 — Connect and handshake

On TCP connection (accepted the same way a local node or squid connection
is), both sides exchange:

- protocol version (mismatch → log, leave the link idle, same as UART)
- a self-identifying peer name (used to name the fake node on the *other*
  side — needs to be stable across reconnects and valid wherever a node
  name is validated today, so a reconnect after a drop can cleanly reuse the
  same fake node name once the stale one is torn down)
- the list of locally registered namespace names

Each side computes the intersection with its own namespace list — that
intersection is this link's set of shared namespaces going forward.

## Phase 1 — Producer exchange

For each shared namespace, each side sends its full list of **real** local
producers: name, `ProducerType`, and MAD type signature (the publisher's
`outType`, or the service's `inType`/`outType` pair). This happens as a
batch right after Phase 0, and then incrementally forever after (see Phase
4) — real producers keep appearing and disappearing locally the whole time
the link is up, not just at connect.

## Phase 2 — Matching

For every producer the peer announced, check locally (real producers *and*
anything already mirrored in from an earlier/other peer) for one with the
same name + `ProducerType` in that namespace:

- **Match, compatible signature** → nothing to do, the namespace is already
  served locally; don't create a duplicate.
- **Match, incompatible signature** (same name/kind, different MAD type) →
  this is a schema conflict, not a duplicate. Skip creating it, but log it
  distinctly from the ordinary dedup case — silently treating a genuine
  conflict the same as "already have it" would hide a real bug (two
  differently-versioned nodes claiming the same topic name).
- **No match** → goes into Phase 3.

## Phase 3 — Fake node and producer creation

If this is the first producer from this peer that needs mirroring, first
register the fake node (`fake_tcp_node_<peer name>`) the same way a real
node registers — reusing the existing node bookkeeping means it shows up in
`squid info` like any other node, which is useful for debugging ("why do I
have a `temperature` publisher I didn't start?").

Every producer that didn't find a match in Phase 2 gets created as an
`EntitySlot` with `node_id` set to that fake node's id, mirroring the peer's
`Producer` variant (`.publisher`/`.service`) with the same name and type
signature. A small per-link integer id gets assigned to each one at
announce time (included in the `PRODUCER_ANNOUNCE` frame) — used on the wire
by `DATA`/`SERVICE_CALL`/`SERVICE_RESPONSE` frames so they don't have to
repeat the full name on every message, same reasoning as UART's `ENTITY_ID`,
just less bandwidth-critical here.

## Phase 4 — Ongoing sync

Namespaces aren't static — nodes connect and disconnect the whole time a
peer link is up. Rather than a one-shot snapshot at connect, Phase 1's
announce is really a standing subscription:

- A new real local producer registers in a shared namespace → broadcast
  `PRODUCER_ANNOUNCE` to every peer link sharing that namespace, and each
  peer runs it through Phase 2/3 same as the initial batch.
- A real local producer disappears (its owning node disconnects) →
  broadcast `PRODUCER_WITHDRAW` (namespace, name, kind). Each peer that had
  mirrored it removes just that one `EntitySlot` from its fake node — not
  the whole fake node, unless that was the last producer it had from this
  peer.

## Data plane

- **Publishers**: the real side forwards every publish as a `DATA` frame
  tagged with the per-link producer id. No ack — same fire-and-forget
  semantics as local pub/sub and as the UART design; a dropped/delayed
  frame just means the fake subscriber-facing side serves the next value
  instead of an intermediate one.
- **Services**: the side with the real service dispatches incoming
  `SERVICE_CALL` frames locally and returns `SERVICE_RESPONSE` tagged with
  a matching sequence number. The calling side still applies a timeout —
  TCP disconnecting is a clean signal, but a peer whose process is wedged
  (not actually disconnected) isn't, so a timeout matters here for the same
  reason it does over UART. On timeout, surface the failure to whatever
  local caller made the request; don't silently retry a non-idempotent call.

No `SYNC`/`CRC`/byte-rescanning is needed anywhere in this protocol, unlike
UART — a `[FRAME_TYPE][LENGTH][PAYLOAD]` framing is sufficient because a
corrupted TCP stream is not a state the OS will hand you; you get a clean
error/EOF instead, same guarantee the existing local-node Unix socket
protocol already relies on.

## Teardown and reconnect

- **Peer link drops**: call `cleanNode` for that peer's fake node id, across
  every shared namespace it had touched. Every mirrored producer disappears
  in one shot, same as if a real node had crashed.
- **Reconnect**: must fully complete teardown of the previous fake node
  before re-running the handshake, so re-registering `fake_tcp_node_<peer
  name>` doesn't collide with a stale entry under
  `NODE_ALREADY_REGISTERED`.

## Open questions for the next pass

- **One multiplexed connection vs. one-per-producer**: this doc assumes a
  single TCP connection per peer carries the handshake and all
  producer/data traffic multiplexed together (simplest — one socket, one
  accept, one thing to clean up on disconnect). `peer.zig`'s existing
  comment notes TCP "can be duplicated per node," which suggests dedicated
  per-producer connections were also being considered, presumably to avoid
  one large/slow topic's data queuing behind a service call's response
  on the same socket. Worth deciding explicitly rather than defaulting.
- **Namespace-sharing default**: is "same name on both sides" really enough,
  or does the private/public split (`spine-nodes` talking to a possibly
  less-trusted peer) argue for an explicit allow-list at `squid add peer`
  time instead of full automatic sharing?
- **Heartbeat**: less urgent than UART since a real TCP disconnect is
  already unambiguous, but still worth having to catch a peer that's
  TCP-alive but internally wedged. Same open question as UART on interval
  tuning.
