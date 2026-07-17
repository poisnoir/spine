# UART Peer Protocol

Design document for how two `spined` instances talk to each other over a UART
(serial) link. This is a peer-to-peer bridge protocol, separate from the
existing `spined` &lt;-&gt; local-node and `spined` &lt;-&gt; `squid` protocols. No code
yet — this is the design to build against.

## Why UART is different

Every other transport `spined` currently speaks (Unix socket to local nodes,
Unix socket to squid) is connection-oriented: the OS hands you a clean stream
per logical conversation, and a closed connection is an unambiguous, instant
signal. A `peer` link over UART breaks both of those assumptions:

- **One wire, no boundaries.** There is exactly one physical stream for the
  whole link. Every entity `spined` wants to expose to (or pull from) its
  peer has to be multiplexed onto that single byte stream — the receiving
  side needs the frame itself to say which entity it belongs to, because
  there's no separate "connection" per entity the way there is for local
  nodes.
- **No reliable delivery, no clean disconnect.** A serial line can drop or
  flip bits (motor/servo EMI is a real concern on this robot), and a device
  can be power-cycled or unplugged without ever signaling "connection
  closed." The protocol has to detect and recover from corruption and
  silence itself, rather than relying on the transport to tell it something
  went wrong.

Everything below follows from those two constraints.

## Goals

- Let a real, local entity (publisher or service) in one `spined`'s
  namespace transparently satisfy a pending lookup in another `spined`'s
  namespace, across a single serial link.
- Stay self-describing enough to detect corruption and resynchronize without
  a human intervening.
- Keep steady-state traffic small — baud rate is fixed and low relative to a
  local Unix socket, so per-frame overhead matters.

## Non-goals (v1)

- **Bus topology.** V1 assumes a UART peer link is strictly point-to-point:
  one cable, exactly two `spined` instances. RS-485-style multi-drop buses
  with more than two devices sharing the line are out of scope — that needs
  real device addressing on top of what's described here, not just an
  entity id. Flagged as future work.
- **Encryption/authentication.** The trust model is the same as a local
  node: if you're physically wired to the port, you're trusted. No auth
  handshake beyond the protocol-version check below.
- **Prioritization/QoS between multiple forwarded entities.** V1 uses a
  single FIFO for outgoing frames. Noted as a future tuning knob if a large
  pub/sub topic ever starves a service call sharing the same link.

## Glossary

- **Local entity** — a real publisher/subscriber/service/service_caller
  registered by an actual node process, same as today.
- **Fake entity** — an entity that exists in a namespace only because a peer
  link is forwarding it from the other side. Looks identical to local
  subscribers/callers; the forwarding is invisible to them.
- **Peer-local entity id** — a small integer (1 byte) identifying "which
  forwarded thing" a frame is about, scoped to one peer link. Assigned
  during the handshake described below — unrelated to, and never the same
  number as, the internal index `spined` uses for that entity inside its own
  namespace's entity array.

## Link layer: frame format

Every frame on the wire has this shape:

| Field       | Size    | Meaning                                            |
|-------------|---------|-----------------------------------------------------|
| `SYNC`      | 2 bytes | Fixed magic value (`0xAA 0x55`), marks frame start  |
| `FRAME_TYPE`| 1 byte  | See frame types below                               |
| `ENTITY_ID` | 1 byte  | Peer-local entity id this frame concerns (0 = n/a, control frames) |
| `SEQ`       | 1 byte  | Rolling sequence number, wraps at 256               |
| `LENGTH`    | 2 bytes | Length of `PAYLOAD` in bytes                        |
| `PAYLOAD`   | `LENGTH` bytes | MAD-encoded struct, meaning depends on `FRAME_TYPE` |
| `CRC16`     | 2 bytes | CRC over `FRAME_TYPE` through `PAYLOAD` (not `SYNC`)|

`SYNC` is not byte-stuffed/escaped. If it happens to appear inside a
payload, the resulting false-positive frame start will fail its CRC check
almost certainly, and gets discarded by the resync logic below — accepting
that tradeoff keeps framing simple and fixed-overhead instead of needing
variable-length escaping.

`LENGTH` is redundant once a given `ENTITY_ID`'s type is known (MAD types are
fixed-size), but keeping it explicit makes every frame self-describing on
its own, which matters for the resync path — you don't want frame parsing to
depend on having successfully tracked prior handshake state to know how many
bytes to read next.

### Frame types

| Name              | Direction        | `ENTITY_ID` | Payload                                   |
|-------------------|------------------|-------------|--------------------------------------------|
| `HELLO`           | either, on link-up | 0         | protocol version, sender identity          |
| `HELLO_ACK`       | reply to `HELLO`  | 0          | protocol version (must match)               |
| `HEARTBEAT`       | either, periodic  | 0          | (empty)                                     |
| `LOOKUP_ANNOUNCE` | either            | 0          | entity name, kind (publisher/service needed)|
| `LOOKUP_WITHDRAW` | either            | 0          | entity name, kind — lookup now satisfied locally |
| `ENTITY_OFFER`    | either            | newly allocated | entity name, kind, MAD type code+size    |
| `ENTITY_ACCEPT`   | reply to offer    | offer's id | (empty) — schema matched, fake entity created |
| `ENTITY_REJECT`   | reply to offer    | offer's id | reason (schema mismatch, etc.)              |
| `ENTITY_WITHDRAW` | either            | existing id| the underlying local entity disappeared     |
| `DATA`            | offering side → other | existing id | MAD-encoded pub/sub payload             |
| `SERVICE_CALL`    | calling side → other  | existing id | MAD-encoded request payload              |
| `SERVICE_RESPONSE`| reply to a call   | same id, `SEQ` matches call | MAD-encoded response payload  |

## Protocol lifecycle

### Phase 0 — Link establishment

`squid add peer type uart path <path> speed <baud>` already opens the
physical port (existing behavior). Once open, `spined` sends `HELLO`
(protocol version + its own identity) and waits for `HELLO_ACK`. A version
mismatch leaves the link administratively up but functionally idle — logged,
not retried on a tight loop, since a version mismatch won't fix itself
without a software update on one side.

### Phase 1 — Lookup exchange

Immediately after the handshake, each side sends its *entire current* lookup
list (everything `LookupManager` already tracks per namespace) as a batch of
`LOOKUP_ANNOUNCE` frames. From then on, every lookup added or resolved
locally is announced incrementally:

- A new local subscriber/service_caller with nothing to satisfy it locally
  → `LOOKUP_ANNOUNCE` goes out on every peer link.
- That lookup later gets satisfied by a real local publisher/service
  registering → `LOOKUP_WITHDRAW` goes out (no longer need to import it from
  a peer, and any in-flight fake entity built for it upstream should tear
  down — see Phase 4).

### Phase 2 — Matching and offer

When a side receives `LOOKUP_ANNOUNCE` for name `X` of kind "publisher
needed," it checks whether it has a real, local (non-fake) publisher named
`X`. If yes: allocate a fresh peer-local entity id and send `ENTITY_OFFER`
(name, kind, MAD type code + size).

The receiving side validates the offered type code against what its own
lookup expects. This guards against two entities sharing a name but having
incompatible schemas — an existing footgun even for purely local entities,
worth being strict about here since a typo or version skew is easier to
introduce across a repo boundary than within one process.

- Match → create a fake local entity bound to (this peer link, this entity
  id), respond `ENTITY_ACCEPT`.
- Mismatch → respond `ENTITY_REJECT` with a reason, log it, and don't
  re-offer instantly — back off before retrying the same name, since a
  schema mismatch won't resolve itself on the next tick and a slow link
  shouldn't be spent re-litigating it.

### Phase 3 — Data forwarding

Once accepted:

- **Pub/sub**: the offering side forwards every publish of `X` as a `DATA`
  frame tagged with that entity id. No ack — this mirrors local pub/sub
  semantics exactly (latest-wins/fire-and-forget), so a dropped `DATA` frame
  just means the fake subscriber sees the next one instead, same as a real
  subscriber missing an intermediate value locally.
- **Services**: mirrored in the opposite role — the side with the real
  service accepts an offer for a *lookup of kind "service needed,"* and
  incoming `SERVICE_CALL` frames get dispatched to the real local service.
  Each call carries a `SEQ`; the caller side waits for a matching
  `SERVICE_RESPONSE` with a timeout (the link can stall with no clean
  "disconnect" signal to react to). On timeout, the local fake
  service_caller surfaces a timeout error to whatever local node made the
  call — it does not silently retry the same request, same reasoning as the
  existing client-side rule that only idempotent reads (subscriber
  reconnects) retry transparently, not writes.

### Phase 4 — Teardown

If the underlying real entity disappears (its owning node disconnects),
the offering side sends `ENTITY_WITHDRAW`. The receiving side removes its
fake entity — which, if it still has local subscribers/callers waiting on
it, should feed back into that namespace's own `LookupManager` as a fresh
`LOOKUP_ANNOUNCE`-worthy lookup, so the whole thing can recover automatically
if the real entity comes back (or a different peer link can offer it) — same
as if the local publisher had simply crashed and needed to be waited on again.

> Note: today, `cleanNode` removing a publisher's entity does not appear to
> re-add a lookup for any subscribers still waiting on it — that's a
> pre-existing gap worth fixing regardless of UART, and this teardown path
> depends on it being fixed for full recovery.

## Error handling and resync

Every frame's `CRC16` is checked before it's accepted. On mismatch: discard
one byte, scan forward for the next `SYNC` pattern, and try framing again
from there — repeat until a frame both matches `SYNC` and passes its CRC.
This is the only recovery mechanism needed for corruption; there is no
separate "resync command," because the framing is fully self-describing.
Each resync event is worth logging (a rising rate of them is a good signal
of a flaky cable/connector before it fails outright).

## Liveness

Since UART has no connection state, `spined` sends `HEARTBEAT` on an
interval. Missing `N` consecutive heartbeats marks the peer link "down":
every fake entity sourced from that link is withdrawn (feeding back into
`LookupManager` per Phase 4), and `spined` goes back to periodically
resending `HELLO`, in case the other side rebooted mid-stream rather than
the link itself failing.

## Bandwidth considerations

Names and type codes only travel during the one-time `LOOKUP_ANNOUNCE` /
`ENTITY_OFFER` exchange for a given entity, never on steady-state `DATA`
frames — those carry only `ENTITY_ID` + the MAD payload. This matters
disproportionately more here than on a local Unix socket, since baud rate is
fixed and comparatively low (set once at `squid add peer` time), and a
repeated full name on every frame would be pure waste on a link that's
already the bottleneck.

## Open questions for the next pass

- Multi-entity prioritization: does a large pub/sub topic need to yield to a
  waiting service call on the same link, or is FIFO good enough given how
  small this project's MAD payloads tend to be (e.g. `[6]f64` joint state)?
- Should `squid` be able to explicitly curate which entities a given UART
  peer is allowed to forward (an allow/deny list at peer-add time), given
  it's the one place bandwidth is explicitly known (`speed <baud>`) — or is
  automatic lookup-driven forwarding (only ever sending what's actually
  wanted) sufficient self-limiting behavior on its own?
- Heartbeat interval and missed-heartbeat threshold — needs to be picked
  against real baud rates/frame sizes once this gets built, not guessed here.
