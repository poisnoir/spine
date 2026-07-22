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
        // Per-client "latest value wins" mailbox: publish() overwrites
        // pending and flips event; clientWriter (spawned once per accepted
        // connection, not once per publish() call) wakes on event and sends
        // whatever's currently there. If clientWriter falls behind a fast
        // publisher, it just skips straight to the newest value next time it
        // wakes - it never sees every value, only ever the latest.
        //
        // This replaces an earlier design where publish() wrote to every
        // client synchronously, one at a time, under one lock: a client that
        // never drained its socket blocked that write forever, which froze
        // delivery to every *other* subscriber too (the loop couldn't reach
        // them) and every other task's publish() call on this topic (the
        // lock was held the whole time) - verified live (see subscriber.zig's
        // "a slow subscriber blocks the publisher and every other
        // subscriber" test) before this existed. A write-deadline fix (kill
        // a client that doesn't drain within N ms) closed that but added a
        // background task + timeout wait to *every* publish() call, even
        // when nothing was wrong (measured ~5x slower for the common single-
        // healthy-subscriber case). This design gives publish() itself no
        // blocking I/O and no per-call task spawn at all: it only ever does
        // a short mutex-guarded value copy and an event flip per client, so
        // a slow client can never delay publish() or any other client's
        // delivery, without paying a tax when everything's healthy.
        const ClientSlot = struct {
            conn: net.Stream,
            // Guards pending/has_pending specifically - publish() (the sole
            // writer, itself already serialized by Self.lock below) and this
            // slot's own clientWriter (the sole reader) are the only two
            // tasks that ever touch these, so this is a short, essentially
            // uncontended critical section, never held across the actual
            // socket write.
            pending_lock: std.Io.Mutex = .init,
            pending: K = undefined,
            has_pending: bool = false,
            event: std.Io.Event = .unset,
            // Set by clientWriter right before it exits (write failed) -
            // publish() checks this to reclaim the slot. Acquire/release
            // paired: by the time publish() observes `dead == true`,
            // clientWriter is guaranteed to have already stopped touching
            // this slot (its last write to any field happens-before this
            // store), so reclaiming/reinitializing the slot's memory is safe.
            dead: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        };

        io: std.Io,
        topic: []const u8,
        listener: net.Server,

        // Slots never relocate once claimed: clientWriter holds &clients[i]
        // for its whole lifetime (same "fixed address, never move" reasoning
        // as Subscriber's own heap allocation - see node.zig's subscribe()).
        // occupied[i] tracks whether clients[i] is a live connection, not
        // clients_num/swap-removal like the old design - a slot's position
        // is stable from the moment it's claimed until it's reclaimed.
        clients: [protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER]ClientSlot = undefined,
        occupied: [protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER]bool =
            [_]bool{false} ** protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER,
        // Maintained alongside occupied purely for the "N total" log line and
        // test/external visibility - not used for indexing.
        clients_num: usize = 0,
        // Guards occupied[]/clients_num/claiming a slot in handshake() and
        // reclaiming one in publish() - never held across a socket write or
        // an event wait, only short array/counter bookkeeping.
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
                .occupied = [_]bool{false} ** protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER,
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
            var slot_index: ?usize = null;
            for (0..protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER) |idx| {
                if (!self.occupied[idx]) {
                    slot_index = idx;
                    break;
                }
            }
            const idx = slot_index orelse {
                self.lock.unlock(self.io);
                conn.close(self.io);
                return;
            };
            self.clients[idx] = .{ .conn = conn };
            self.occupied[idx] = true;
            self.clients_num += 1;
            const total = self.clients_num;
            self.lock.unlock(self.io);

            _ = try self.io.concurrent(clientWriter, .{ self, &self.clients[idx] });

            try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
            try w.interface.flush();

            print("spine: subscriber joined topic '{s}' ({d} total)\n", .{ self.topic, total });
        }

        // The sole reader of this slot's pending/has_pending, and the sole
        // writer of its conn - one dedicated task per connected client,
        // spawned once at accept time, living until the write it's doing
        // fails (client actually gone) - not once per publish() call.
        fn clientWriter(self: *Self, slot: *ClientSlot) void {
            while (true) {
                slot.event.wait(self.io) catch return;
                slot.event.reset();

                slot.pending_lock.lock(self.io) catch return;
                const value = slot.pending;
                const has = slot.has_pending;
                slot.has_pending = false;
                slot.pending_lock.unlock(self.io);

                // Spurious wake, or another clientWriter iteration already
                // consumed this signal - shouldn't normally happen given
                // Event's own "no pending wait" precondition on reset(), but
                // cheap to guard rather than assume.
                if (!has) continue;

                const size = mad.getRequiredSize(K);
                var buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
                _ = mad.encode(K, value, buf[0..size]);

                var w_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined;
                var w = slot.conn.writer(self.io, &w_buf);
                const failed = blk: {
                    w.interface.writeAll(buf[0..size]) catch break :blk true;
                    w.interface.flush() catch break :blk true;
                    break :blk false;
                };

                if (failed) {
                    slot.conn.close(self.io);
                    slot.dead.store(true, .release);
                    return;
                }
            }
        }

        // Hands the newest value to every currently-connected subscriber's
        // clientWriter task - never blocks on any of them, and never blocks
        // on any other task's concurrent publish() call for longer than a
        // handful of short, uncontended lock acquisitions. Actual delivery
        // (and any blocking that entails) happens entirely in each client's
        // own clientWriter, outside this call.
        pub fn publish(self: *Self, data: K) !void {
            try self.lock.lock(self.io);
            defer self.lock.unlock(self.io);

            for (0..protocol.globals.MAX_SUBSCRIBERS_PER_PUBLISHER) |i| {
                if (!self.occupied[i]) continue;
                const slot = &self.clients[i];

                if (slot.dead.load(.acquire)) {
                    self.occupied[i] = false;
                    self.clients_num -= 1;
                    continue;
                }

                slot.pending_lock.lock(self.io) catch continue;
                slot.pending = data;
                slot.has_pending = true;
                slot.pending_lock.unlock(self.io);
                slot.event.set(self.io);
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

    // BUGFIX: was a tight publish-50-then-drain-50 burst, asserting every
    // value arrives. Under the "latest value wins" mailbox (see Publisher's
    // own doc comment), that's no longer true by design - a burst with
    // nobody draining overwrites clientWriter's pending value long before it
    // gets a chance to send most of them (see the coalescing test below for
    // that contract directly). Alternating publish()+next() instead gives
    // each value a natural synchronization point: next() only returns once
    // that exact value has actually been delivered, so the following
    // publish() can never race ahead of clientWriter and overwrite it first.
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        try publisher.publish(i);
        try testing.expectEqual(i, try subscriber.next());
    }
}

// The flip side of the ordering test above: a burst with nobody draining is
// exactly the scenario "latest value wins" is for - clientWriter should
// skip straight to the newest pending value once it's finally scheduled,
// not work through a backlog.
test "pubsub: a burst with no reader delivers only the latest value" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_coalesce_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_coalesce");
    const subscriber = try node.subscribe(u32, "pubsub_test_coalesce");

    var i: u32 = 0;
    while (i < 500) : (i += 1) {
        try publisher.publish(i);
    }

    // Give clientWriter a chance to actually run and send whatever it finds
    // pending - it may have already sent an earlier value or two before this
    // burst finished (there's no guarantee it was fully idle the whole time,
    // just that it can never fall further behind than "the latest"), so this
    // only asserts against the one thing the design actually promises: the
    // very last value published must be the one this eventually reads,
    // whatever else happened before it.
    try io.sleep(std.Io.Duration.fromMilliseconds(100), .awake);
    try publisher.publish(999);

    // publish() returns as soon as it hands 999 to clientWriter's mailbox,
    // not once clientWriter has actually sent it - next() only ever drains
    // what's *already* arrived (see its own doc comment on bufferedLen()),
    // so calling it immediately here would race clientWriter's still-in-
    // flight send and could see only the pre-999 backlog. A short wait lets
    // that one send actually land before draining.
    try io.sleep(std.Io.Duration.fromMilliseconds(100), .awake);
    try testing.expectEqual(@as(u32, 999), try subscriber.next());
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

    {
        // this subscriber is only kept alive long enough to connect, then its
        // connection is closed — simulating a subscriber process that died.
        const doomed = try node.subscribe(u32, "pubsub_test_dead_client");
        doomed.conn.close(io);
    }

    const survivor = try node.subscribe(u32, "pubsub_test_dead_client");

    // Unlike the old synchronous design, a slot's death is now detected and
    // reclaimed in two separate steps, not within one publish() call: the
    // first publish() after the dead client closed wakes its clientWriter
    // task, which attempts the write, fails, and marks the slot dead in the
    // background - not necessarily before this call itself returns. A
    // *second* publish() is what actually reclaims the slot, once it
    // observes dead == true.
    try publisher.publish(1);
    _ = try survivor.next();

    try io.sleep(std.Io.Duration.fromMilliseconds(100), .awake); // let clientWriter notice and mark dead
    try publisher.publish(2);
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
