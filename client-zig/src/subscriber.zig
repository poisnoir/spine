const std = @import("std");
const protocol = @import("protocol");
const net = std.Io.net;
const mad = protocol.mad;
const Backoff = @import("backoff.zig").Backoff;
// TODO: replace print with logging
const print = std.debug.print;

pub const SubscribeError = error{
    PayloadTypeMismatch,
};

// Generic subscriber: connects directly to the publisher's unix socket
// (independent of spined — the socket path is a fixed convention), performs
// mad's type-fingerprint handshake, then decodes one K per next() call.
pub fn Subscriber(comptime K: type) type {
    return struct {
        io: std.Io,
        conn: net.Stream,
        namespace: []const u8,
        topic: []const u8,

        r_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined,
        reader: net.Stream.Reader = undefined,

        const Self = @This();

        // Retries the dial+handshake with exponential backoff — the publisher may not have started its
        // listener yet, or may be mid-restart, and a tight retry loop would
        // otherwise spin a core at 100% doing nothing but failing connects.
        pub fn connect(self: *Self, io: std.Io, namespace: []const u8, topic: []const u8) !void {
            var backoff: Backoff = .{};

            while (true) {
                self.dial(io, namespace, topic) catch |err| {
                    // BUGFIX: a type mismatch is permanent — K is fixed at
                    // compile time by the caller, so retrying can never fix
                    // it. Only transient errors (publisher not up yet, etc.)
                    // should back off and retry.
                    if (err == SubscribeError.PayloadTypeMismatch) return err;

                    print("spine: failed to connect to publisher for topic '{s}' ({any}), retrying in {d}ms\n", .{ topic, err, backoff.ms });
                    try backoff.sleep(io);
                    continue;
                };
                return;
            }
        }

        pub fn dial(self: *Self, io: std.Io, namespace: []const u8, topic: []const u8) !void {
            var path_buf: [256]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.PUBLISHER_SOCKET_DIR, namespace, topic });

            const conn = try protocol.network.dial(io, path);
            self.* = .{
                .io = io,
                .conn = conn,
                .namespace = namespace,
                .topic = topic,
                .r_buf = undefined,
                .reader = undefined,
            };
            self.reader = conn.reader(io, &self.r_buf);

            var w_buf: [64]u8 = undefined;
            var w = conn.writer(io, &w_buf);
            try w.interface.writeAll(mad.code(K));
            try w.interface.flush();

            const status = try self.reader.interface.takeInt(u8, .big);
            if (status != protocol.globals.OK_STATUS) {
                conn.close(io);
                return SubscribeError.PayloadTypeMismatch;
            }

            print("spine: subscribed to topic '{s}'\n", .{topic});
        }

        // Blocks until the next published message arrives, then decodes it.
        //
        // BUGFIX: a read failure (publisher died/restarted) used to surface
        // straight to the caller as an error, unlike spine-go's Subscriber,
        // which reconnects transparently in its background goroutine and
        // just keeps Get() blocked until new data shows up. next() now does
        // the synchronous equivalent: on a read failure, reconnect (with the
        // same unlimited backoff as the initial connect) and retry the read,
        // rather than surfacing a transient disconnect as an error. Reading
        // has no side effects, so retrying here has none of the
        // at-most-once/idempotency concerns ServiceCaller.call() has.
        pub fn next(self: *Self) !K {
            const size = mad.getRequiredSize(K);
            while (true) {
                const msg = self.reader.interface.take(size) catch |err| {
                    print("spine: subscriber connection to topic '{s}' lost ({any}), reconnecting\n", .{ self.topic, err });
                    self.conn.close(self.io);
                    try self.connect(self.io, self.namespace, self.topic);
                    continue;
                };
                var out: K = undefined;
                _ = mad.decode(K, &out, msg);
                return out;
            }
        }
    };
}

const testing = std.testing;
const Node = @import("node.zig").Node;
const test_support = @import("test_support.zig");
const testIo = test_support.testIo;
const testAllocator = test_support.testAllocator;

// Subscriber-centric behavior: handshake rejection, reconnect-on-drop, and
// dialing a producer that crashed before ever connecting. Broadcast
// semantics and producer-lifecycle tests live in publisher.zig instead.

test "pubsub: mismatched payload type is rejected, not retried forever" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_mismatch_node", io, allocator);
    defer node.deinit();

    _ = try node.publish(u32, "pubsub_test_mismatch");

    const result = node.subscribe(f32, "pubsub_test_mismatch");
    try testing.expectError(SubscribeError.PayloadTypeMismatch, result);
}

test "pubsub: subscriber reconnects after connection drop" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_reconnect_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_reconnect");
    const subscriber = try node.subscribe(u32, "pubsub_test_reconnect");

    try publisher.publish(1);
    try testing.expectEqual(@as(u32, 1), try subscriber.next());

    // Simulate the connection dying by closing the *publisher's* accepted
    // copy of it (and forgetting it from clients so publish() never reuses
    // that same fd) — not the subscriber's own fd. Closing your own fd and
    // then reusing it is a real use-after-close bug (this Io backend panics
    // on it, correctly), not a stand-in for a remote disconnect. Closing the
    // *peer's* end and leaving the subscriber's own fd untouched is exactly
    // how a real disconnect gets observed: next()'s next read on its own
    // still-valid fd sees a genuine EOF/error, no unsafe fd reuse involved.
    publisher.clients[0].close(io);
    publisher.clients_num = 0;

    var future = io.async(Subscriber(u32).next, .{subscriber});

    // give next() a moment to notice the closed conn and reconnect before
    // publishing — dialing a local unix socket is fast but not instant.
    try io.sleep(std.Io.Duration.fromMilliseconds(200), .awake);
    try publisher.publish(2);

    try testing.expectEqual(@as(u32, 2), try future.await(io));
}

test "subscribe: dialing a crashed producer's stale socket clears it instead of failing forever" {
    const io = testIo();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.PUBLISHER_SOCKET_DIR, "common", "dial_crashed_test_topic" });

    std.Io.Dir.deleteFileAbsolute(io, path) catch {};
    {
        const addr = try net.UnixAddress.init(path);
        var srv = try addr.listen(io, .{});
        srv.deinit(io);
    }

    // dial() is the single-attempt primitive Subscriber.connect()'s retry
    // loop calls forever - tested directly here rather than through
    // connect(), which would just retry indefinitely against a socket
    // nothing ever answers on and hang this test.
    var sub: Subscriber(u32) = undefined;
    if (sub.dial(io, "common", "dial_crashed_test_topic")) |_| {
        return error.TestUnexpectedResult; // nothing is listening, this must fail
    } else |_| {}

    // The stale file must be gone now - a second attempt should fail
    // cleanly (FileNotFound) instead of re-discovering the same dead file.
    if (sub.dial(io, "common", "dial_crashed_test_topic")) |_| {
        return error.TestUnexpectedResult;
    } else |err| {
        try testing.expectEqual(error.FileNotFound, err);
    }
}
