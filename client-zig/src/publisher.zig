const std = @import("std");
const protocol = @import("protocol");
const mad = protocol.mad;
const net = std.Io.net;
// TODO: replace print with logging
const print = std.debug.print;

// Generic publisher: binds a listener at the publisher's fixed convention
// socket path (independent of spined — subscribers dial in directly) and
// broadcasts each publish() call to every subscriber that has completed the
// mad type-fingerprint handshake.
pub fn Publisher(comptime K: type) type {
    comptime {
        if (mad.getRequiredSize(K) > protocol.globals.MAX_PACKET_SIZE) {
            @compileError("payload type too big for globals.MAX_PACKET_SIZE");
        }
    }

    return struct {
        io: std.Io,
        topic: []const u8,
        listener: net.Server,

        clients: [protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER]net.Stream = undefined,
        clients_num: usize = 0,
        lock: std.Io.Mutex = .init,

        const Self = @This();

        pub fn listen(self: *Self, io: std.Io, namespace: []const u8, topic: []const u8) !void {
            var path_buf: [256]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.PUBLISHER_SOCKET_DIR, namespace, topic });

            const srv = try protocol.network.bind(io, path); // TODO: Don't like this at all

            self.* = .{
                .io = io,
                .topic = topic,
                .listener = srv,
                .clients = undefined,
                .clients_num = 0,
                .lock = .init,
            };

            print("spine: publisher listening on topic '{s}'\n", .{topic});

            _ = try io.concurrent(acceptLoop, .{self});
        }

        fn acceptLoop(self: *Self) void {
            while (true) {
                const conn = self.listener.accept(self.io) catch |err| {
                    print("spine: publisher accept failed for topic '{s}': {any}\n", .{ self.topic, err });
                    continue;
                };
                self.handshake(conn) catch |err| {
                    print("spine: subscriber handshake failed for topic '{s}': {any}\n", .{ self.topic, err });
                };
            }
        }

        // Reads the subscriber's mad type-fingerprint and rejects it if it
        // doesn't match K, mirroring spine-go's Publisher.registerSubscriber.
        fn handshake(self: *Self, conn: net.Stream) !void {
            var w_buf: [64]u8 = undefined;
            var r_buf: [64]u8 = undefined;
            var w = conn.writer(self.io, &w_buf);
            var r = conn.reader(self.io, &r_buf);

            const expected = mad.code(K);

            // BUGFIX: was `r.interface.take(expected.len)` — a fixed-length
            // read for however many bytes *our own* K's code happens to be.
            // A genuinely mismatched subscriber sends a *different* number of
            // bytes (a different K has a different code length) and then
            // just waits for the status reply, so take() blocked forever
            // waiting for bytes that would never come — a real deadlock, not
            // just a slow path. readVec does a single opportunistic read (like
            // Go's raw conn.Read) and hands back however many bytes actually
            // arrived, so a length mismatch is just a mismatch, not a hang.
            var code_buf: [64]u8 = undefined;
            var vecs: [1][]u8 = .{&code_buf};
            const n = r.interface.readVec(&vecs) catch {
                conn.close(self.io);
                return;
            };
            const msg = code_buf[0..n];

            if (!std.mem.eql(u8, msg, expected)) {
                w.interface.writeInt(u8, protocol.globals.ERROR_MISMATCH_PAYLOAD_CODE, .big) catch {};
                w.interface.flush() catch {};
                conn.close(self.io);
                return;
            }

            // BUGFIX: register the client *before* acking OK_STATUS, not
            // after. The old order let a subscriber see OK_STATUS — and so
            // return successfully from connect() — before it was actually in
            // `clients`, opening a window where a publish() racing right
            // after subscribe() returned could be missed entirely.
            try self.lock.lock(self.io);
            if (self.clients_num >= protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER) {
                self.lock.unlock(self.io);
                conn.close(self.io);
                return;
            }
            self.clients[self.clients_num] = conn;
            self.clients_num += 1;
            const total = self.clients_num;
            self.lock.unlock(self.io);

            try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
            try w.interface.flush();

            print("spine: subscriber joined topic '{s}' ({d} total)\n", .{ self.topic, total });
        }

        // Broadcasts data to every currently-connected subscriber. Dead
        // connections (write failures) are dropped via swap-removal, the
        // same pattern spined's own cleanNode uses for its fixed-size arrays.
        pub fn publish(self: *Self, data: K) !void {
            const size = mad.getRequiredSize(K);
            var buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
            _ = mad.encode(K, data, buf[0..size]);

            try self.lock.lock(self.io);
            defer self.lock.unlock(self.io);

            var i: usize = 0;
            while (i < self.clients_num) {
                var w_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
                var w = self.clients[i].writer(self.io, &w_buf);

                const failed = blk: {
                    w.interface.writeAll(buf[0..size]) catch break :blk true;
                    w.interface.flush() catch break :blk true;
                    break :blk false;
                };

                if (failed) {
                    self.clients[i].close(self.io);
                    self.clients_num -= 1;
                    self.clients[i] = self.clients[self.clients_num];
                } else {
                    i += 1;
                }
            }
        }
    };
}

const testing = std.testing;
const Node = @import("node.zig").Node;
const test_support = @import("test_support.zig");
const testIo = test_support.testIo;
const testAllocator = test_support.testAllocator;

// Broadcast semantics and producer-lifecycle behavior — the Publisher-
// centric half of the pub/sub tests. Subscriber-centric behavior (handshake
// rejection, reconnect, dialing a crashed producer) lives in subscriber.zig
// instead.

test "pubsub: basic roundtrip" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_basic_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_basic");
    const subscriber = try node.subscribe(u32, "pubsub_test_basic");

    try publisher.publish(777);
    try testing.expectEqual(@as(u32, 777), try subscriber.next());
}

test "pubsub: multiple subscribers all receive the same value" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_multi_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(i32, "pubsub_test_multi");

    const sub1 = try node.subscribe(i32, "pubsub_test_multi");
    const sub2 = try node.subscribe(i32, "pubsub_test_multi");
    const sub3 = try node.subscribe(i32, "pubsub_test_multi");

    try publisher.publish(42);

    try testing.expectEqual(@as(i32, 42), try sub1.next());
    try testing.expectEqual(@as(i32, 42), try sub2.next());
    try testing.expectEqual(@as(i32, 42), try sub3.next());
}

test "pubsub: multiple values arrive in order" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_order_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_order");
    const subscriber = try node.subscribe(u32, "pubsub_test_order");

    // BUGFIX: was 50 - Subscriber's queue (subscriber.zig) is now a bounded,
    // oldest-evicted-first ring buffer (queue_capacity = 32) fed by a
    // background task, not an unbounded pull straight off the wire, so a
    // burst bigger than capacity is no longer guaranteed to arrive loss-free
    // - see subscriber.zig's own "burst within queue capacity" and
    // "overflow drops oldest" tests for that contract specifically. This
    // one just needs to stay a simple in-order sanity check, safely under
    // capacity.
    const n = 20;
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try publisher.publish(i);
    }

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i, try subscriber.next());
    }
}

const TestReading = struct {
    x: f32,
    y: f32,
};

test "pubsub: struct payload" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_struct_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(TestReading, "pubsub_test_struct");
    const subscriber = try node.subscribe(TestReading, "pubsub_test_struct");

    try publisher.publish(.{ .x = 1.5, .y = -2.25 });
    const got = try subscriber.next();

    try testing.expectEqual(@as(f32, 1.5), got.x);
    try testing.expectEqual(@as(f32, -2.25), got.y);
}

test "pubsub: dead subscriber is dropped from the client list" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_dead_client_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_dead_client");

    // BUGFIX: this used to create a real Subscriber and close its own conn
    // directly - inert back when nothing automatically read from a
    // Subscriber unless a test explicitly called next(). Now every
    // subscribe() spawns a background run() task that reads continuously
    // and reconnects on its own if dropped, so a real Subscriber can't
    // cleanly simulate "a client that's gone for good" anymore - closing
    // its own fd would race run()'s in-flight read (a hard panic on this
    // Io backend), and even routed through the publisher's side instead,
    // the Subscriber would just reconnect and show back up in
    // publisher.clients, racing this test's own assertion. Connecting a
    // raw socket and completing the handshake by hand isolates this test
    // back to just Publisher's own dead-client detection, with no
    // self-healing Subscriber in the way.
    {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.PUBLISHER_SOCKET_DIR, "common", "pubsub_test_dead_client" });
        const addr = try net.UnixAddress.init(path);
        const conn = try addr.connect(io);

        var w_buf: [64]u8 = undefined;
        var w = conn.writer(io, &w_buf);
        try w.interface.writeAll(mad.code(u32));
        try w.interface.flush();

        var r_buf: [1]u8 = undefined;
        var r = conn.reader(io, &r_buf);
        _ = try r.interface.takeByte();

        conn.close(io); // dies immediately after handshaking - never touched again
    }

    // Give the publisher's accept loop a moment to actually register the
    // now-dead connection above before checking clients_num.
    try io.sleep(std.Io.Duration.fromMilliseconds(100), .awake);
    try testing.expectEqual(@as(usize, 1), publisher.clients_num);

    const survivor = try node.subscribe(u32, "pubsub_test_dead_client");

    // first publish() after the dead client closed is what actually notices
    // the write failure and swap-removes it.
    try publisher.publish(1);
    _ = try survivor.next();

    try testing.expectEqual(@as(usize, 1), publisher.clients_num);
}

// Two producer-lifecycle scenarios, verified live before being written up
// here as regression tests:
//   1. Registering a name that's genuinely already running must fail, not
//      silently steal the running producer's socket.
//   2. Re-registering a name whose previous producer crashed (stale socket
//      file, nothing listening) must self-heal and succeed.
// (The third scenario from this trio — a consumer dialing a crashed
// producer's stale socket — is Subscriber-centric and lives in
// subscriber.zig.)

test "publish: registering an already-running topic fails without disturbing it" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "dup_producer_test_node", io, allocator);
    defer node.deinit();

    const first = try node.publish(u32, "dup_producer_test_topic");

    try testing.expectError(
        protocol.network.BindError.AlreadyBound,
        node.publish(u32, "dup_producer_test_topic"),
    );

    // The original publisher must still be the one actually bound - prove
    // it by publishing through it and reading the value back.
    const subscriber = try node.subscribe(u32, "dup_producer_test_topic");
    try first.publish(999);
    try testing.expectEqual(@as(u32, 999), try subscriber.next());
}

test "publish: re-registering after the previous producer crashed self-heals" {
    const io = testIo();
    const allocator = testAllocator();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ protocol.globals.PUBLISHER_SOCKET_DIR, "common", "crashed_producer_test_topic" });

    // Simulate a crash: bind directly (bypassing Node/Publisher entirely,
    // so nothing here goes through network.bind's own cleanup), then tear
    // down without unlinking - exactly what a killed process leaves behind
    // (verified against a real SIGKILL earlier in this project).
    std.Io.Dir.deleteFileAbsolute(io, path) catch {};
    {
        const addr = try net.UnixAddress.init(path);
        var srv = try addr.listen(io, .{});
        srv.deinit(io);
    }

    var node = try Node.init("common", "crashed_producer_test_node", io, allocator);
    defer node.deinit();

    // Must detect nothing is listening, clear the stale file, and bind
    // fresh - not fail with AlreadyBound against a dead file.
    const publisher = try node.publish(u32, "crashed_producer_test_topic");

    const subscriber = try node.subscribe(u32, "crashed_producer_test_topic");
    try publisher.publish(42);
    try testing.expectEqual(@as(u32, 42), try subscriber.next());
}
