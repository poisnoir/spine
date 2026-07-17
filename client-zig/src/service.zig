const std = @import("std");
const net = std.Io.net;
const protocol = @import("protocol");
const mad = protocol.mad;
const print = std.debug.print;

// Generic RPC service: binds a listener at the service's fixed convention
// socket path and, for each connected caller, runs the key/value
// type-fingerprint handshake (service_common.go's establishConnection) then
// loops calling `handler` for every request. Each accepted connection gets
// its own io.concurrent task, so multiple callers are served independently —
// wire-compatible with both spine-go's Service and ThreadedService, which
// share the identical protocol in service_common.go and only differ
// internally in how Go schedules handler execution.
pub fn Service(comptime K: type, comptime V: type) type {
    comptime {
        if (mad.getRequiredSize(K) > protocol.globals.MAX_PACKET_SIZE) {
            @compileError("key type too big for globals.MAX_PACKET_SIZE");
        }
        if (mad.getRequiredSize(V) + 1 > protocol.globals.MAX_PACKET_SIZE) {
            @compileError("value type too big for globals.MAX_PACKET_SIZE");
        }
    }

    return struct {
        io: std.Io,
        name: []const u8,
        listener: net.Server,
        handler: *const fn (K) anyerror!V,

        const Self = @This();

        pub fn listen(self: *Self, io: std.Io, namespace: []const u8, name: []const u8, handler: *const fn (K) anyerror!V) !void {
            var path_buf: [256]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.SERVICE_SOCKET_DIR, namespace, name });

            const srv = try protocol.network.bind(io, path);

            self.* = .{
                .io = io,
                .name = name,
                .listener = srv,
                .handler = handler,
            };

            print("spine: service '{s}' listening\n", .{name});

            _ = try io.concurrent(acceptLoop, .{self});
        }

        fn acceptLoop(self: *Self) void {
            while (true) {
                const conn = self.listener.accept(self.io) catch |err| {
                    print("spine: service accept failed for '{s}': {any}\n", .{ self.name, err });
                    continue;
                };
                _ = self.io.concurrent(handleClient, .{ self, conn }) catch |err| {
                    print("spine: service '{s}' could not spawn client handler: {any}\n", .{ self.name, err });
                    conn.close(self.io);
                };
            }
        }

        // Reads the caller's key- then value-type fingerprints and rejects
        // the connection if either doesn't match. Mirrors spine-go's
        // establishConnection exactly, including the fact that a mismatch
        // gets *no* response byte at all — just a closed connection. A real
        // spine-go ServiceCaller only knows how to interpret that exact
        // shape of failure (an immediate read error, not a status byte), so
        // matching it here isn't optional if a Go caller needs to see the
        // same behavior talking to a Zig service.
        fn handshake(self: *Self, r: *net.Stream.Reader, w: *net.Stream.Writer) !bool {
            var key_code_buf: [64]u8 = undefined;
            var key_vecs: [1][]u8 = .{&key_code_buf};
            const kn = try r.interface.readVec(&key_vecs);
            if (!std.mem.eql(u8, key_code_buf[0..kn], mad.code(K))) return false;

            try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
            try w.interface.flush();

            var value_code_buf: [64]u8 = undefined;
            var value_vecs: [1][]u8 = .{&value_code_buf};
            const vn = try r.interface.readVec(&value_vecs);
            if (!std.mem.eql(u8, value_code_buf[0..vn], mad.code(V))) return false;

            try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
            try w.interface.flush();

            print("spine: caller connected to service '{s}'\n", .{self.name});
            return true;
        }

        fn handleClient(self: *Self, conn: net.Stream) void {
            defer conn.close(self.io);

            var r_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
            var w_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
            var r = conn.reader(self.io, &r_buf);
            var w = conn.writer(self.io, &w_buf);

            const matched = self.handshake(&r, &w) catch |err| {
                print("spine: service '{s}' handshake failed: {any}\n", .{ self.name, err });
                return;
            };
            if (!matched) return;

            const key_size = mad.getRequiredSize(K);
            const value_size = mad.getRequiredSize(V);

            while (true) {
                var req_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
                var vecs: [1][]u8 = .{&req_buf};
                const n = r.interface.readVec(&vecs) catch return;

                // BUGFIX-adjacent: mad.decode has no bounds checking of its
                // own (it slices `input[0..byte_len]` directly, which panics
                // on a too-short buffer rather than returning an error), so
                // this guard has to happen before calling it, unlike
                // spine-go where mad-go's Decode returns "buffer too small".
                if (n < key_size) {
                    w.interface.writeInt(u8, protocol.globals.ERROR_SERIALIZER_ERROR_CODE, .big) catch return;
                    w.interface.flush() catch return;
                    continue;
                }

                var key: K = undefined;
                _ = mad.decode(K, &key, req_buf[0..key_size]);

                const value = self.handler(key) catch |err| {
                    print("spine: service '{s}' handler error: {any}\n", .{ self.name, err });
                    w.interface.writeInt(u8, protocol.globals.ERROR_SERVICE_ERROR_CODE, .big) catch return;
                    w.interface.flush() catch return;
                    continue;
                };

                var resp_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
                resp_buf[0] = protocol.globals.OK_STATUS;
                _ = mad.encode(V, value, resp_buf[1 .. 1 + value_size]);

                w.interface.writeAll(resp_buf[0 .. 1 + value_size]) catch return;
                w.interface.flush() catch return;
            }
        }
    };
}

const testing = std.testing;
const Node = @import("node.zig").Node;
const ServiceCallError = @import("service_caller.zig").ServiceCallError;
const test_support = @import("test_support.zig");
const testIo = test_support.testIo;
const testAllocator = test_support.testAllocator;

// Service-centric behavior: handler dispatch and how handler outcomes reach
// the caller. ServiceCaller-centric behavior (handshake rejection,
// reconnect) lives in service_caller.zig instead.

fn doubleHandler(input: u32) anyerror!u32 {
    return input * 2;
}

fn errorOnZeroHandler(input: u32) anyerror!u32 {
    if (input == 0) return error.ZeroNotAllowed;
    return input;
}

test "service: basic call" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_basic_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_basic", doubleHandler);
    const caller = try node.newServiceCaller(u32, u32, "service_test_basic");

    try testing.expectEqual(@as(u32, 42), try caller.call(21));
}

test "service: multiple sequential calls over the same caller" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_multi_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_multi", doubleHandler);
    const caller = try node.newServiceCaller(u32, u32, "service_test_multi");

    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try testing.expectEqual(i * 2, try caller.call(i));
    }
}

test "service: handler error surfaces as CallFailed" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_error_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_error", errorOnZeroHandler);
    const caller = try node.newServiceCaller(u32, u32, "service_test_error");

    try testing.expectEqual(@as(u32, 5), try caller.call(5));
    try testing.expectError(ServiceCallError.CallFailed, caller.call(0));
}
