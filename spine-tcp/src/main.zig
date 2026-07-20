const std = @import("std");
const net = std.Io.net;

fn usage() void {
    std.debug.print(
        \\usage:
        \\  spine_tcp connect <ip-addr>
        \\  spine_tcp listen <port>
        \\
    , .{});
}

// TODO: once a mode is validated below, actually dial/listen, register as a
// spine peer bridge, and start relaying entity traffic to/from the remote
// side - mirrors spine-uart's role (../spine-uart/)
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // args[0] is the program name.
    const tokens = args[1..];

    if (tokens.len == 2 and std.mem.eql(u8, tokens[0], "connect")) {
        return doConnect(tokens[1]);
    }

    if (tokens.len == 2 and std.mem.eql(u8, tokens[0], "listen")) {
        return doListen(tokens[1]);
    }

    usage();
    std.process.exit(1);
}

// connect takes one argument: the remote address, written as `ip:port` (or
// `[ip6]:port`, per net.IpAddress.parseLiteral) - unlike listen, a port on
// its own isn't enough here since we need to know which host to dial.
fn doConnect(ip_addr: []const u8) void {
    const address = net.IpAddress.parseLiteral(ip_addr) catch |err| {
        std.debug.print("invalid ip address \"{s}\": {any}\n", .{ ip_addr, err });
        std.process.exit(1);
    };

    std.debug.print("spine_tcp: connect -> {f} (not yet implemented)\n", .{address});
}

// listen takes one argument: the local port to listen on, across every
// interface - there's no host to name on this side, unlike connect.
fn doListen(port_str: []const u8) void {
    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        std.debug.print("invalid port \"{s}\": must be an integer between 0 and 65535\n", .{port_str});
        std.process.exit(1);
    };

    std.debug.print("spine_tcp: listen on port {d} (not yet implemented)\n", .{port});
}
