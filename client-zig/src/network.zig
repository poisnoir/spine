const std = @import("std");
const net = std.Io.net;

pub fn createServer(addr: []const u8, io: std.Io) !net.Server {
    const serviceAddr: net.UnixAddress = try net.UnixAddress.init(addr);
    return try serviceAddr.listen(io, .{});
}

pub fn handleListener(
    server: *net.Server,
    io: std.Io,
    handler: fn (stream: net.Stream) void,
) !void {
    defer server.deinit(io);
    while (true) {
        const stream = try server.accept(io);
        handler(stream);
    }
}
