// Unlike client-lib's own tests (which run with spined unreachable on
// purpose, to test node-to-node local-only pub/sub), these tests spawn a
// real `spined` binary and drive it with the real `spine` client-lib and
// the real `squid` binary, over the actual (hardcoded) Unix socket paths.
// This is the only place that exercises spined and its clients together as
// separate processes - it's what would have caught the Io.Mutex init
// deadlock earlier, which only ever showed up when a real client or squid
// talked to a freshly started spined.
//
// Run with `zig build test-integration`. Requires zig-out/bin/{spined,squid}
// to already be built (the build step depends on the install step for
// this), and must be run from the repository root, since the binary paths
// below are relative.

const std = @import("std");
const testing = std.testing;
const print = std.debug.print;
const spine = @import("spine");
const protocol = @import("protocol");
const globals = protocol.globals;

const spined_bin = "zig-out/bin/spined";
const squid_bin = "zig-out/bin/squid";

var test_threaded: std.Io.Threaded = undefined;
var test_arena: std.heap.ArenaAllocator = undefined;
var test_io_ready = false;

fn ensureTestIoReady() void {
    if (test_io_ready) return;
    test_threaded = .init(std.heap.page_allocator, .{
        .async_limit = .limited(64),
        .concurrent_limit = .limited(64),
    });
    test_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    test_io_ready = true;

    // Same fix as client-lib's own ensureTestIoReady: these tests' pub/sub
    // and service topics ("integration_test_topic", ...) are fixed names,
    // so a previous run's socket files are still there on every subsequent
    // run - bindProducerSocket's connect probe finds them (nothing
    // listening) and self-heals, but that path prints a noisy "unexpected
    // errno: 111" trace per occurrence. Clearing both directories once, up
    // front, means a normal run never has to touch that path at all.
    const io = test_threaded.io();
    std.Io.Dir.cwd().deleteTree(io, "/tmp/spine/publisher") catch {};
    std.Io.Dir.cwd().deleteTree(io, "/tmp/spine/service") catch {};
}

fn testIo() std.Io {
    ensureTestIoReady();
    return test_threaded.io();
}

fn testAllocator() std.mem.Allocator {
    ensureTestIoReady();
    return test_arena.allocator();
}

// Starts a real spined process against the real, hardcoded socket path,
// clearing away any stale SPINED_PATH socket file a previous crashed test
// run left behind (spined itself now does the same probe-then-delete dance
// on startup, but clearing it here too means a leftover from a hard-killed
// test run can't make the *next* test's spawn racily contend with spined's
// own cleanup). Every test that calls this must `defer child.kill(io)` so
// the socket is freed before the next test tries to bind it - tests in one
// binary run sequentially in the same process, not in isolated sandboxes.
fn spawnSpined(io: std.Io) !std.process.Child {
    std.Io.Dir.deleteFileAbsolute(io, globals.SPINED_PATH) catch {};

    return std.process.spawn(io, .{
        .argv = &.{spined_bin},
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
}

// SPINED_PATH is a real file, created the instant Spined.init()'s bind()
// call succeeds - polling for it to exist is a clean, noise-free way to
// know spined is up and accepting connections (both node registrations and
// squid commands go over this one socket now).
fn waitForSpinedReady(io: std.Io) !void {
    var attempt: usize = 0;
    while (attempt < 100) : (attempt += 1) {
        if (std.Io.Dir.accessAbsolute(io, globals.SPINED_PATH, .{})) |_| {
            return;
        } else |_| {}
        try io.sleep(std.Io.Duration.fromMilliseconds(20), .awake);
    }
    return error.SocketNeverBecameReady;
}

// Node.init() only ever tries to connect once and silently falls back to
// local-only mode on failure (by design - see node.zig). Retrying here,
// rather than just calling Node.init() once after a fixed sleep, means this
// test fails loudly (SpinedNeverBecameReady) instead of silently passing in
// degraded local-only mode if spined is slow to start.
fn waitForRegisteredNode(io: std.Io, allocator: std.mem.Allocator, namespace: []const u8, name: []const u8) !spine.Node {
    var attempt: usize = 0;
    while (attempt < 100) : (attempt += 1) {
        var node = try spine.Node.init(namespace, name, io, allocator);
        if (node.spined_conn != null) return node;
        node.deinit();
        try io.sleep(std.Io.Duration.fromMilliseconds(20), .awake);
    }
    return error.SpinedNeverBecameReady;
}

fn doubleHandler(input: u32) anyerror!u32 {
    return input * 2;
}

test "spined + spine client-lib: pub/sub between two independently-registered real nodes" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    var pub_node = try waitForRegisteredNode(io, allocator, "common", "integration_test_publisher");
    defer pub_node.deinit();
    var sub_node = try waitForRegisteredNode(io, allocator, "common", "integration_test_subscriber");
    defer sub_node.deinit();

    // Prove this actually went through spined, not the local-only fallback.
    try testing.expect(pub_node.spined_conn != null);
    try testing.expect(sub_node.spined_conn != null);

    const publisher = try pub_node.publish(u32, "integration_test_topic");
    const subscriber = try sub_node.subscribe(u32, "integration_test_topic");

    try publisher.publish(4242);
    try testing.expectEqual(@as(u32, 4242), try subscriber.next());
}

test "spined + spine client-lib: service call between two independently-registered real nodes" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    var service_node = try waitForRegisteredNode(io, allocator, "common", "integration_test_service_node");
    defer service_node.deinit();
    var caller_node = try waitForRegisteredNode(io, allocator, "common", "integration_test_caller_node");
    defer caller_node.deinit();

    _ = try service_node.newService(u32, u32, "integration_test_double", doubleHandler);
    const caller = try caller_node.newServiceCaller(u32, u32, "integration_test_double");

    try testing.expectEqual(@as(u32, 84), try caller.call(42));
}

// publish()/newService() now register with spined *before* attempting to
// bind (see node.zig's comment) - so when spined is actually reachable, a
// duplicate producer name must be rejected by spined's own check
// (EntityAlreadyRegistered) without ever reaching the bind()-level check
// (ProducerAlreadyExists). Client-lib's own unit tests run in local-only
// mode, where spined is unreachable and registration always no-ops, so they
// can't tell these two rejection paths apart - only a real spined can.
test "spined + spine client-lib: duplicate producer is rejected by spined before any bind is attempted" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    var first_node = try waitForRegisteredNode(io, allocator, "common", "dup_producer_node_1");
    defer first_node.deinit();
    var second_node = try waitForRegisteredNode(io, allocator, "common", "dup_producer_node_2");
    defer second_node.deinit();

    const publisher = try first_node.publish(u32, "dup_producer_topic");

    try testing.expectError(
        error.EntityAlreadyRegistered,
        second_node.publish(u32, "dup_producer_topic"),
    );

    // The rejected attempt must not have disturbed the real publisher -
    // prove it's still the only one bound and still fully functional.
    const subscriber = try first_node.subscribe(u32, "dup_producer_topic");
    try publisher.publish(123);
    try testing.expectEqual(@as(u32, 123), try subscriber.next());
}

// publish() registers with spined *before* binding the real socket (see
// node.zig's comment) — if that bind() then fails for some reason spined
// wouldn't know it doesn't actually exist, unless publish()'s errdefer rolls
// the registration back. Proven here by forcing the bind() to fail for a
// reason spined has no visibility into: something else already bound at the
// producer's convention path (not another spine node, so spined's own
// hasProducer check can't catch it - only the OS-level bind() can). If the
// rollback didn't happen, the second publish() below (after freeing the OS
// socket) would fail with EntityAlreadyRegistered instead of succeeding.
test "spined + spine client-lib: a bind() failure after spined approves registration is rolled back" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    var node = try waitForRegisteredNode(io, allocator, "common", "rollback_test_node");
    defer node.deinit();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}{s}/{s}", .{ globals.PUBLISHER_SOCKET_DIR, "common", "rollback_test_topic" });

    var squatter = try protocol.network.bind(io, path);
    try testing.expectError(error.AlreadyBound, node.publish(u32, "rollback_test_topic"));
    squatter.deinit(io);

    // If publish()'s errdefer had NOT unregistered with spined, this would
    // come back error.EntityAlreadyRegistered instead of succeeding.
    const publisher = try node.publish(u32, "rollback_test_topic");
    const subscriber = try node.subscribe(u32, "rollback_test_topic");
    try publisher.publish(7);
    try testing.expectEqual(@as(u32, 7), try subscriber.next());
}

test "spined + squid: adding a namespace shows up in squid info" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);
    try waitForSpinedReady(io);

    const add = try std.process.run(allocator, io, .{
        .argv = &.{ squid_bin, "add", "namespace", "integration_test_ns" },
    });
    print("squid: {s}", .{add.stderr});
    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, add.term);

    const info = try std.process.run(allocator, io, .{
        .argv = &.{ squid_bin, "info" },
    });
    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, info.term);
    print("squid: {s}", .{info.stderr});
    // squid's output goes through std.debug.print, which writes to stderr,
    // not stdout.
    try testing.expect(std.mem.indexOf(u8, info.stderr, "integration_test_ns") != null);
}

test "spined + squid: registering the same namespace twice is rejected" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);
    try waitForSpinedReady(io);

    const first = try std.process.run(allocator, io, .{
        .argv = &.{ squid_bin, "add", "namespace", "dup_test_ns" },
    });
    print("squid: {s}", .{first.stderr});
    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, first.term);

    const second = try std.process.run(allocator, io, .{
        .argv = &.{ squid_bin, "add", "namespace", "dup_test_ns" },
    });
    print("squid: {s}", .{second.stderr});
    try testing.expectEqual(std.process.Child.Term{ .exited = 1 }, second.term);
}
