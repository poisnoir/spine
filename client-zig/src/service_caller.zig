const std = @import("std");
const protocol = @import("protocol");
const print = std.debug.print;
const net = std.Io.net;
const mad = protocol.mad;
const Backoff = @import("backoff.zig").Backoff;

pub const ServiceCallError = error{
    PayloadTypeMismatch,
    CallFailed,
};

// Generic RPC caller: connects to a Service's fixed convention socket path,
// runs the same key/value type-fingerprint handshake, then sends requests
// and decodes responses one at a time. call() is mutex-guarded so concurrent
// callers on the same ServiceCaller can't interleave requests/responses on
// the single underlying connection — mirrors what spine-go's internal
// request channel + single run() goroutine achieves, just via a lock instead
// of a queue.
pub fn ServiceCaller(comptime K: type, comptime V: type) type {
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
        namespace: []const u8,
        name: []const u8,
        conn: net.Stream,
        is_connected: bool = false,

        r_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined,
        w_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined,
        reader: net.Stream.Reader = undefined,
        writer: net.Stream.Writer = undefined,
        lock: std.Io.Mutex = .init,

        const Self = @This();

        // Same backoff-retry reasoning as Subscriber.connect: a type
        // mismatch is permanent (K/V are fixed at compile time), so it
        // returns immediately instead of retrying forever.
        pub fn connect(self: *Self, io: std.Io, namespace: []const u8, name: []const u8) !void {
            var backoff: Backoff = .{};

            while (true) {
                self.dial(io, namespace, name) catch |err| {
                    if (err == ServiceCallError.PayloadTypeMismatch) return err;

                    print("spine: failed to connect to service '{s}' ({any}), retrying in {d}ms\n", .{ name, err, backoff.ms });
                    try backoff.sleep(io);
                    continue;
                };
                return;
            }
        }

        // BUGFIX: this used to be a blanket `self.* = .{...}` literal that
        // included `.lock = .init`. That's fine for the very first connect
        // (self is freshly allocated, uninitialized memory), but dial() is
        // also called to reconnect from *inside* call() when is_connected is
        // false — and call() has already locked self.lock at that point.
        // Resetting it mid-call replaced the held lock with a fresh
        // *unlocked* one, so call()'s deferred unlock later found nothing
        // locked and Io.Mutex correctly panicked rather than silently
        // ignoring it. lock is a one-time thing, set by Node.newServiceCaller
        // before the first connect() — dial() must never touch it.
        fn dial(self: *Self, io: std.Io, namespace: []const u8, name: []const u8) !void {
            var path_buf: [256]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.SERVICE_SOCKET_DIR, namespace, name });
            const conn = try protocol.network.dial(io, path);

            self.io = io;
            self.namespace = namespace;
            self.name = name;
            self.conn = conn;
            self.is_connected = false;
            self.reader = conn.reader(io, &self.r_buf);
            self.writer = conn.writer(io, &self.w_buf);

            // BUGFIX-adjacent: a real spine-go service replies with *nothing
            // and closes the connection* on a type mismatch (see Service's
            // handshake comment) rather than a status byte — so a read that
            // errors out right after either handshake write means the same
            // thing here as an explicit non-OK status.
            try self.writer.interface.writeAll(mad.code(K));
            try self.writer.interface.flush();
            const key_status = self.reader.interface.takeInt(u8, .big) catch {
                conn.close(io);
                return ServiceCallError.PayloadTypeMismatch;
            };
            if (key_status != protocol.globals.OK_STATUS) {
                conn.close(io);
                return ServiceCallError.PayloadTypeMismatch;
            }

            try self.writer.interface.writeAll(mad.code(V));
            try self.writer.interface.flush();
            const value_status = self.reader.interface.takeInt(u8, .big) catch {
                conn.close(io);
                return ServiceCallError.PayloadTypeMismatch;
            };
            if (value_status != protocol.globals.OK_STATUS) {
                conn.close(io);
                return ServiceCallError.PayloadTypeMismatch;
            }

            self.is_connected = true;
            print("spine: connected to service '{s}'\n", .{name});
        }

        fn markDisconnected(self: *Self) void {
            self.is_connected = false;
            self.conn.close(self.io);
        }

        // BUGFIX: a mid-connection drop used to just surface as an error
        // forever after — nothing ever reconnected. Mirrors spine-go's
        // ServiceCaller.run(): if the last attempt marked the connection
        // dead, reconnect (unlimited backoff, same as the initial connect)
        // before trying this request. A failure *during* this request also
        // marks it dead so the *next* call reconnects first — this call
        // still returns its own error rather than silently retrying the same
        // request, since the request may have already taken effect
        // server-side by the time the failure is observed (unlike
        // Subscriber.next(), reading has no such idempotency concern).
        pub fn call(self: *Self, key: K) !V {
            try self.lock.lock(self.io);
            defer self.lock.unlock(self.io);

            if (!self.is_connected) {
                try self.connect(self.io, self.namespace, self.name);
            }

            const key_size = mad.getRequiredSize(K);
            var req_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
            _ = mad.encode(K, key, req_buf[0..key_size]);

            self.writer.interface.writeAll(req_buf[0..key_size]) catch |err| {
                self.markDisconnected();
                return err;
            };
            self.writer.interface.flush() catch |err| {
                self.markDisconnected();
                return err;
            };

            const value_size = mad.getRequiredSize(V);
            var resp_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
            var vecs: [1][]u8 = .{&resp_buf};
            const n = self.reader.interface.readVec(&vecs) catch |err| {
                self.markDisconnected();
                return err;
            };

            if (n == 0 or n < 1 + value_size) {
                self.markDisconnected();
                return ServiceCallError.CallFailed;
            }

            const status = resp_buf[0];
            if (status != protocol.globals.OK_STATUS) {
                print("spine: service '{s}' call failed with status {d}\n", .{ self.name, status });
                return ServiceCallError.CallFailed;
            }

            var value: V = undefined;
            _ = mad.decode(V, &value, resp_buf[1 .. 1 + value_size]);
            return value;
        }
    };
}

const testing = std.testing;
const Node = @import("node.zig").Node;
const test_support = @import("test_support.zig");
const testIo = test_support.testIo;
const testAllocator = test_support.testAllocator;

// ServiceCaller-centric behavior: handshake rejection and reconnect-on-drop.
// Handler dispatch behavior lives in service.zig instead.

fn doubleHandler(input: u32) anyerror!u32 {
    return input * 2;
}

test "service: mismatched key type is rejected, not retried forever" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_keymismatch_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_keymismatch", doubleHandler);

    const result = node.newServiceCaller(f32, u32, "service_test_keymismatch");
    try testing.expectError(ServiceCallError.PayloadTypeMismatch, result);
}

test "service: mismatched value type is rejected" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_valmismatch_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_valmismatch", doubleHandler);

    const result = node.newServiceCaller(u32, f32, "service_test_valmismatch");
    try testing.expectError(ServiceCallError.PayloadTypeMismatch, result);
}

test "service: caller reconnects when marked disconnected" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "service_test_reconnect_node", io, allocator);
    defer node.deinit();

    _ = try node.newService(u32, u32, "service_test_reconnect", doubleHandler);
    const caller = try node.newServiceCaller(u32, u32, "service_test_reconnect");

    try testing.expectEqual(@as(u32, 2), try caller.call(1));

    // Simulate having detected a dead connection the way markDisconnected()
    // would after a real write/read failure, without actually touching the
    // fd: Service doesn't expose its accepted per-caller connections (unlike
    // Publisher's `clients`), so there's no equivalent of the pubsub
    // reconnect test's "close the peer's copy, not our own" trick available
    // here. This still exercises the exact branch that matters — call()
    // reconnecting before attempting the request when is_connected is false
    // — see readme.md's live cross-process verification for the real
    // socket-level failure path (killing and restarting an actual service).
    caller.is_connected = false;

    try testing.expectEqual(@as(u32, 6), try caller.call(3));
}
