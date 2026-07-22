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
        // If more than one payload's worth is already sitting in the
        // reader's own buffer (a burst arrived faster than this was called -
        // see Publisher's coalescing mailbox, which bounds how much gets
        // *sent* but not how much can land in this socket's kernel buffer
        // before this side catches up), keeps consuming and discards
        // everything but the newest, so a caller of next() always gets the
        // latest published value, never a stale backlog - verified live:
        // publishing 500 values in a tight burst then one final sentinel
        // value used to hand back some arbitrary mid-burst value instead of
        // the sentinel, since this used to just return the *oldest* unread
        // byte off the wire like any plain FIFO socket read.
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
            var latest = try self.readOne(size);

            // bufferedLen() inspects the reader's own in-memory buffer, left
            // over from take()'s last underlying refill - no new syscall, so
            // this can never block or race against a producer that stops
            // sending mid-drain: it only ever consumes what's already here.
            while (self.reader.interface.bufferedLen() >= size) {
                latest = try self.readOne(size);
            }
            return latest;
        }

        fn readOne(self: *Self, size: usize) !K {
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
const Publisher = @import("publisher.zig").Publisher;
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

    // Simulate the connection dying by shutting down (not closing) the
    // *publisher's* accepted copy of it — not the subscriber's own fd.
    //
    // BUGFIX: this used to `.close()` clients[0].conn directly, on the same
    // reasoning as the old synchronous design ("closing your own fd and
    // reusing it is a use-after-close bug, so close the *peer's* side
    // instead"). That reasoning no longer holds here: clients[0].conn is now
    // also the *sole property* of that slot's own long-lived clientWriter
    // task, which the test has no part in - closing it out from under that
    // task, then letting clientWriter's own later write attempt touch the
    // same already-closed fd, reproduced live as a hard panic ("programmer
    // bug caused syscall error: BADF"), the exact use-after-close class this
    // whole pattern exists to avoid, just from the *other* direction.
    // shutdown() severs the connection (future reads/writes fail with a
    // normal, catchable ECONNRESET/EPIPE) without invalidating the fd
    // itself, so clientWriter remains the only thing that ever calls
    // .close() on it, whenever it naturally discovers the failure.
    //
    // This also used to directly poke occupied[0]/clients_num to "forget"
    // the slot immediately - unsafe for the same underlying reason: slot 0's
    // clientWriter is still alive and blocked in slot.event.wait() at this
    // point (it's event-driven, with no way to notice a dead connection on
    // its own until it next tries to write to it), so freeing the slot let
    // the reconnecting subscriber's new connection claim that same index in
    // handshake(), re-initializing its Event out from under the still-
    // waiting old task - a second, independent hard panic ("reset called
    // before pending wait returned"). Doing nothing else here is correct and
    // sufficient: the reconnecting subscriber claims a *different*,
    // actually-free slot, and slot 0 gets discovered dead (and only then
    // reclaimed) the ordinary way, the next time publish() below tries to
    // write to it - same two-step detect-then-reclaim path as the "dead
    // subscriber is dropped" test in publisher.zig.
    try publisher.clients[0].conn.shutdown(io, .both);

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

// SCRATCH demo, not a real assertion-bearing test: proves the
// head-of-line-blocking property discussed for the old (current) design
// live, rather than just asserting it. Publisher.publish() writes to
// clients sequentially under one lock (publisher.zig) - a subscriber that
// never drains its socket eventually fills the kernel's send buffer for
// that connection, and publish()'s write to it blocks. Since the write
// loop can't skip ahead, that blocks every *subsequent* client in the list
// too, and since the lock is held for the whole call, every other task's
// publish() on this topic blocks as well.
//
// fast_sub is subscribed first (so it's earlier in clients[] than
// slow_sub) and drains continuously; slow_sub never calls next() at all.
// Both a background publisher task and fast_sub's reader run unbounded -
// nothing here is awaited, so a permanent stall (the expected outcome)
// can't hang the test itself; it's observed via atomics.
test "SCRATCH: a slow subscriber blocks the publisher and every other subscriber" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "slow_sub_demo_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "slow_sub_demo_topic");
    const fast_sub = try node.subscribe(u32, "slow_sub_demo_topic");
    _ = try node.subscribe(u32, "slow_sub_demo_topic"); // slow_sub: never drained, on purpose

    const FastReader = struct {
        var received: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
        fn run(sub: *Subscriber(u32)) void {
            while (true) {
                _ = sub.next() catch return;
                _ = received.fetchAdd(1, .monotonic);
            }
        }
    };
    var fast_future = io.async(FastReader.run, .{fast_sub});
    _ = &fast_future;

    const PublishLoop = struct {
        var completed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
        fn run(p: *Publisher(u32)) void {
            var i: u32 = 0;
            while (true) : (i += 1) {
                p.publish(i) catch return;
                _ = completed.fetchAdd(1, .monotonic);
            }
        }
    };
    var publish_future = io.async(PublishLoop.run, .{publisher});
    _ = &publish_future;

    try io.sleep(std.Io.Duration.fromSeconds(3), .awake);
    const completed_3s = PublishLoop.completed.load(.monotonic);
    const received_3s = FastReader.received.load(.monotonic);
    print("SCRATCH: after 3s - publish() calls completed: {d}, fast_sub received: {d}\n", .{ completed_3s, received_3s });

    try io.sleep(std.Io.Duration.fromSeconds(2), .awake);
    const completed_5s = PublishLoop.completed.load(.monotonic);
    const received_5s = FastReader.received.load(.monotonic);
    print("SCRATCH: after 5s - publish() calls completed: {d} (+{d}), fast_sub received: {d} (+{d})\n", .{
        completed_5s,           completed_5s -| completed_3s,
        received_5s, received_5s -| received_3s,
    });
    print("SCRATCH: if both deltas are 0, the publisher (and fast_sub with it) is permanently stalled on slow_sub's full socket buffer\n", .{});
}
