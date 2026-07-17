const std = @import("std");
const net = std.Io.net;
const protocol = @import("protocol");
const mad = protocol.mad;
const globals = @import("globals.zig");
const string = protocol.mad.string;

// squid control-plane payloads and the GetInfoResponse tree live in the
// shared protocol module now, so squid and spined can't drift apart on
// field layout the way two hand-kept copies could.
const CreateNamespacePayload = protocol.payloads.CreateNamespacePayload;
const GetInfoResponse = protocol.payloads.GetInfoResponse;

fn usage() void {
    std.debug.print(
        \\usage:
        \\  squid add namespace <name>
        \\  squid info
        \\
    , .{});
}

// Connects to spined, sends one command byte + its encoded payload, and
// returns the single status byte spined answers with. squid is a one-shot
// CLI: one command per connection, then we're done.
fn sendCommand(io: std.Io, comptime PayloadType: type, cmd: u8, payload: PayloadType) !u8 {
    const addr = try net.UnixAddress.init(protocol.globals.SPINED_PATH);
    const stream = try addr.connect(io);
    defer stream.close(io);

    var r_buf: [globals.BUF_SIZE]u8 = undefined;
    var w_buf: [globals.BUF_SIZE]u8 = undefined;
    var r = stream.reader(io, &r_buf);
    var w = stream.writer(io, &w_buf);

    try w.interface.writeInt(u8, cmd, .big);

    var encoded: [globals.BUF_SIZE]u8 = undefined;
    const n = mad.encode(PayloadType, payload, &encoded);
    try w.interface.writeAll(encoded[0..n]);
    try w.interface.flush();

    return try r.interface.takeByte();
}

fn addNamespace(io: std.Io, name: []const u8) !void {
    const payload = CreateNamespacePayload{ .name = try string.fromConst(name) };
    const status = try sendCommand(io, CreateNamespacePayload, protocol.globals.ADD_NAMESPACE_CODE, payload);

    if (status != protocol.globals.OK_STATUS) {
        std.debug.print("failed to create namespace \"{s}\": {s}\n", .{ name, protocol.globals.statusMessage(status) });
        std.process.exit(1);
    }
    std.debug.print("namespace \"{s}\" created\n", .{name});
}

// GET_INFO has no request payload and a response far bigger than a status
// byte, so it doesn't go through sendCommand: it connects itself, streams
// the command byte, then reads the fixed-size GetInfoResponse straight off
// the wire into a heap-allocated destination (too big to put on the stack).
fn getInfo(io: std.Io, allocator: std.mem.Allocator) !void {
    const addr = try net.UnixAddress.init(protocol.globals.SPINED_PATH);
    const stream = try addr.connect(io);
    defer stream.close(io);

    var r_buf: [globals.BUF_SIZE]u8 = undefined;
    var w_buf: [globals.BUF_SIZE]u8 = undefined;
    var r = stream.reader(io, &r_buf);
    var w = stream.writer(io, &w_buf);

    try w.interface.writeInt(u8, protocol.globals.GET_INFO_CODE, .big);
    try w.interface.flush();

    const size = mad.getRequiredSize(GetInfoResponse);
    const raw = try allocator.alloc(u8, size);
    try r.interface.readSliceAll(raw);

    const info = try allocator.create(GetInfoResponse);
    _ = mad.decode(GetInfoResponse, info, raw);

    printInfo(info);
}

fn printInfo(info: *const GetInfoResponse) void {
    std.debug.print("namespaces:\n", .{});
    for (0..info.namespace_num) |i| {
        const ns = &info.namespaces[i];
        std.debug.print("  {s}\n", .{ns.name.data[0..ns.name.len]});

        if (ns.node_num == 0) {
            std.debug.print("    nodes: (none)\n", .{});
        } else {
            std.debug.print("    nodes:\n", .{});
            for (0..ns.node_num) |j| {
                const node = &ns.nodes[j];
                std.debug.print("      - {s}\n", .{node.name.data[0..node.name.len]});
            }
        }
    }
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // args[0] is the program name.
    const tokens = args[1..];

    if (tokens.len == 1 and std.mem.eql(u8, tokens[0], globals.VERB_INFO)) {
        return getInfo(io, init.arena.allocator());
    }

    if (tokens.len >= 2 and std.mem.eql(u8, tokens[0], globals.VERB_ADD)) {
        if (tokens.len == 3 and std.mem.eql(u8, tokens[1], globals.NOUN_NAMESPACE)) {
            return addNamespace(io, tokens[2]);
        }
    }

    usage();
    std.process.exit(1);
}
