# squid

squid is the CLI for [spined](../spined/readme.md) — the daemon's command line.

## Commands

```sh
squid add namespace <name>
squid info
```

### `squid add namespace <name>`

Creates a new namespace on spined. Only the `"common"` namespace exists by default (see spined's own
[Limitations](../spined/readme.md#limitations--known-gaps)).

```sh
$ squid add namespace robots
namespace "robots" created
```

Fails (exit code 1) if the namespace already exists, or if spined already has the maximum number of
namespaces.

### `squid info`

Takes no arguments. Prints every namespace spined currently knows about, and the nodes registered in
each.

```sh
$ squid info
namespaces:
  common
    nodes:
      - arm-node
  robots
    nodes: (none)
```

## Exit codes

`0` on success. `1` on any failure — an unrecognized command prints usage and exits `1`; a rejected
command (namespace already exists, spined at a capacity limit) prints spined's status message and
exits `1`.
