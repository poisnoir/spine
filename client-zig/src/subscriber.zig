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
// mad's type-fingerprint handshake, then keeps a bounded, most-recent-wins
// queue of decoded values fed by a background task, with next() popping
// from it.
//
// This is a deliberate divergence from a strict pull model (which this type
// used to be, and which client-go's Subscriber still was until it hit the
// same tradeoff from the other direction): reading is decoupled from
// draining, and a caller of next() slower than the publisher just misses
// its own oldest unread values instead of ever stalling the publisher or
// other subscribers of the same topic - see the queue-overflow tests below
// for the exact contract.
pub fn Subscriber(comptime K: type) type {
    return struct {
        io: std.Io,
        conn: net.Stream,
        namespace: []const u8,
        topic: []const u8,

        r_buf: [protocol.globals.MAX_PACKET_SIZE]u8 = undefined,
        reader: net.Stream.Reader = undefined,

        // Bounded ring buffer of decoded values not yet returned by next().
        // run() is the sole producer; next() is the (possibly concurrent)
        // consumer. queue_cond signals "the queue became non-empty" -
        // next() re-checks count after waking since Condition.wait can
        // return spuriously.
        queue: [queue_capacity]K = undefined,
        head: usize = 0,
        count: usize = 0,
        queue_lock: std.Io.Mutex = .init,
        queue_cond: std.Io.Condition = .init,

        const Self = @This();
        const queue_capacity = 32;

        // Retries the dial+handshake with exponential backoff — the publisher may not have started its
        // listener yet, or may be mid-restart, and a tight retry loop would
        // otherwise spin a core at 100% doing nothing but failing connects.
        // Called once for the initial connection (from Node.subscribe, via
        // start() below) and again by run() internally on every reconnect.
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

            // BUGFIX-adjacent: this used to be a blanket `self.* = .{...}`
            // literal, which is fine for the very first connect (self is
            // freshly allocated, uninitialized memory) but dial() is also
            // called to reconnect from *inside* run() - and a concurrent
            // next() call could be actively waiting on queue_cond (which
            // internally holds a pointer to queue_lock) at that exact
            // moment. Resetting queue_lock/queue_cond/queue/head/count out
            // from under a waiter would corrupt them, the same class of bug
            // already fixed in service_caller.zig's dial() for its own
            // .lock field. Assign only the connection-related fields.
            self.io = io;
            self.conn = conn;
            self.namespace = namespace;
            self.topic = topic;
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

        // Spawns the background task that keeps the queue fed. Node.subscribe()
        // calls this once, after connect() has already succeeded - not
        // connect() itself, since connect() is also called (without spawning
        // another task) whenever run() needs to reconnect.
        pub fn start(self: *Self) !void {
            _ = try self.io.concurrent(Self.run, .{self});
        }

        fn run(self: *Self) void {
            const size = mad.getRequiredSize(K);
            while (true) {
                const msg = self.reader.interface.take(size) catch |err| {
                    print("spine: subscriber connection to topic '{s}' lost ({any}), reconnecting\n", .{ self.topic, err });
                    self.conn.close(self.io);
                    self.connect(self.io, self.namespace, self.topic) catch |cerr| {
                        print("spine: subscriber for topic '{s}' giving up: {any}\n", .{ self.topic, cerr });
                        return; // permanent type mismatch - nothing more this task can do
                    };
                    continue;
                };
                var out: K = undefined;
                _ = mad.decode(K, &out, msg);
                self.push(out) catch |err| {
                    print("spine: subscriber for topic '{s}' failed to queue a value: {any}\n", .{ self.topic, err });
                    return;
                };
            }
        }

        // Adds v to the queue, evicting the oldest unread value first if the
        // queue is already full. Only ever called from run() (the sole
        // producer).
        fn push(self: *Self, v: K) !void {
            try self.queue_lock.lock(self.io);
            defer self.queue_lock.unlock(self.io);

            if (self.count == queue_capacity) {
                self.head = (self.head + 1) % queue_capacity;
                self.count -= 1;
            }

            const tail = (self.head + self.count) % queue_capacity;
            self.queue[tail] = v;
            self.count += 1;
            self.queue_cond.signal(self.io);
        }

        // Returns the oldest value not yet returned, blocking if the queue
        // is currently empty. Safe for concurrent callers: each gets the
        // next value in FIFO order, none see the same value twice.
        pub fn next(self: *Self) !K {
            try self.queue_lock.lock(self.io);
            defer self.queue_lock.unlock(self.io);

            while (self.count == 0) {
                try self.queue_cond.wait(self.io, &self.queue_lock);
            }

            const v = self.queue[self.head];
            self.head = (self.head + 1) % queue_capacity;
            self.count -= 1;
            return v;
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
    // how a real disconnect gets observed: run()'s next read on its own
    // still-valid fd sees a genuine EOF/error, no unsafe fd reuse involved.
    publisher.clients[0].close(io);
    publisher.clients_num = 0;

    var future = io.async(Subscriber(u32).next, .{subscriber});

    // give run() a moment to notice the closed conn and reconnect before
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

// A burst that fits inside the queue's capacity must never lose anything -
// only a burst that actually overflows the queue should drop anything, and
// only its oldest entries (see the overflow test below).
test "pubsub: burst within queue capacity delivers every value in order" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_withincap_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_withincap");
    const subscriber = try node.subscribe(u32, "pubsub_test_withincap");

    const n = Subscriber(u32).queue_capacity; // exactly at capacity - must not overflow
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try publisher.publish(i);
    }

    // Give run() time to read, decode, and queue all n values before we
    // start draining - this test is about the queue's own contents, not a
    // race between producer and consumer.
    try io.sleep(std.Io.Duration.fromMilliseconds(300), .awake);

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i, try subscriber.next());
    }
}

// A burst that exceeds the queue's capacity must evict the *oldest* unread
// values to make room for newer ones, keeping exactly the newest
// queue_capacity, still in order - not drop newest, not reorder, not
// silently keep stale data indefinitely.
test "pubsub: overflow drops oldest values, keeps newest in order" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_overflow_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_overflow");
    const subscriber = try node.subscribe(u32, "pubsub_test_overflow");

    const n: u32 = 100; // > queue_capacity (32): guarantees overflow
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try publisher.publish(i);
    }

    // Deliberately not draining via next() until after every value has
    // been published and (almost certainly) already read+queued by run() -
    // the queue needs to have actually overflowed before this test means
    // anything.
    try io.sleep(std.Io.Duration.fromMilliseconds(300), .awake);

    const first_expected = n - Subscriber(u32).queue_capacity;
    i = first_expected;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i, try subscriber.next());
    }
}

// Plain-u32-returning wrapper so the Future array below has a simple,
// concrete element type instead of fighting next()'s own inferred error set.
fn nextOrPanic(sub: *Subscriber(u32)) u32 {
    return sub.next() catch |err| std.debug.panic("next() failed: {any}", .{err});
}

// Concurrent next() callers are meant to be safe, each getting a distinct
// value in FIFO order (see next()'s own doc comment) - the queue is shared
// mutable state now guarded by queue_lock/queue_cond instead of a single
// synchronous read per call, so this is worth its own direct check rather
// than only trusting the reasoning behind that design. One caller per
// value (rather than a few callers each doing several next() calls) for
// maximal concurrent contention on the queue.
test "pubsub: concurrent next() callers each get distinct sequential values" {
    const io = testIo();
    const allocator = testAllocator();

    var node = try Node.init("common", "pubsub_test_concurrent_next_node", io, allocator);
    defer node.deinit();

    const publisher = try node.publish(u32, "pubsub_test_concurrent_next");
    const subscriber = try node.subscribe(u32, "pubsub_test_concurrent_next");

    const n = Subscriber(u32).queue_capacity; // one concurrent caller per value

    var futures: [n]std.Io.Future(u32) = undefined;
    for (&futures) |*f| {
        f.* = io.async(nextOrPanic, .{subscriber});
    }

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try publisher.publish(i);
    }

    var seen = [_]bool{false} ** n;
    for (&futures) |*f| {
        const v = f.await(io);
        try testing.expect(v < n);
        try testing.expect(!seen[v]); // no value delivered to more than one caller
        seen[v] = true;
    }
    for (seen) |was_seen| {
        try testing.expect(was_seen); // every value delivered to exactly one caller
    }
}
