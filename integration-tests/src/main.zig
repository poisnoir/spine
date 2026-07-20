// Unlike client-lib's own tests (which run with spined unreachable on
// purpose, to test node-to-node local-only pub/sub), these tests spawn a
// real `spined` binary and drive it with the real `spine` client-lib, over
// the actual (hardcoded) Unix socket paths. This is the only place that
// exercises spined and its clients together as separate processes - it's
// what would have caught the Io.Mutex init deadlock earlier, which only
// ever showed up when a real client talked to a freshly started spined.
//
// Run with `zig build test-integration`. Requires zig-out/bin/spined to
// already be built (the build step depends on the install step for this),
// and must be run from the repository root, since the binary path below is
// relative.

const std = @import("std");
const testing = std.testing;
const print = std.debug.print;
const spine = @import("spine");
const protocol = @import("protocol");
const globals = protocol.globals;
const mad = protocol.mad;
const net = std.Io.net;

const spined_bin = "zig-out/bin/spined";

var test_threaded: std.Io.Threaded = undefined;
var test_arena: std.heap.ArenaAllocator = undefined;
var test_io_ready = false;

fn ensureTestIoReady() void {
    if (test_io_ready) return;
    test_threaded = .init(std.heap.page_allocator, .{
        // 256, not 64: the concurrent-registration regression test below
        // needs enough real concurrency to reliably exercise the race it
        // guards against - verified: 20 concurrent tasks (under the old 64
        // limit) and even 100 (under 128) didn't reproduce the pre-fix bug
        // reliably, only 150 concurrent tasks with this higher limit did,
        // twice in a row - see that test's own comment.
        .async_limit = .limited(256),
        .concurrent_limit = .limited(256),
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
// one-shot control commands like addNamespace/getInfo go over this one
// socket now).
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

test "spined + spine client-lib: addNamespace shows up in getInfo" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    // addNamespace/getInfo are free functions, not Node methods - they
    // aren't scoped to any particular node - but registering a node here
    // too lets this test also prove getInfo picks it up under "common".
    var node = try waitForRegisteredNode(io, allocator, "common", "add_namespace_test_node");
    defer node.deinit();

    try spine.addNamespace(io, "client_lib_test_ns");

    const info = try spine.getInfo(io, allocator);
    defer allocator.destroy(info);

    var found = false;
    for (0..info.namespace_num) |i| {
        const ns = &info.namespaces[i];
        if (std.mem.eql(u8, ns.name.data[0..ns.name.len], "client_lib_test_ns")) {
            found = true;
            break;
        }
    }
    try testing.expect(found);

    // The node registered above must also show up under "common", proving
    // getInfo() decoded the whole tree, not just the namespace it created.
    var found_node = false;
    for (0..info.namespace_num) |i| {
        const ns = &info.namespaces[i];
        if (!std.mem.eql(u8, ns.name.data[0..ns.name.len], "common")) continue;
        for (0..ns.node_num) |j| {
            const n = &ns.nodes[j];
            if (std.mem.eql(u8, n.name.data[0..n.name.len], "add_namespace_test_node")) {
                found_node = true;
                break;
            }
        }
    }
    try testing.expect(found_node);
}

test "spined + spine client-lib: addNamespace rejects a duplicate namespace" {
    const io = testIo();

    var child = try spawnSpined(io);
    defer child.kill(io);
    try waitForSpinedReady(io);

    try spine.addNamespace(io, "dup_client_lib_test_ns");
    try testing.expectError(
        error.NamespaceAlreadyRegistered,
        spine.addNamespace(io, "dup_client_lib_test_ns"),
    );
}

// Reads VmRSS out of /proc/<pid>/status, in kB. Linux-only (matches the rest
// of this project - SPINED_PATH etc. are all Unix-domain-socket-only
// already), but this is the only reliable way to observe a *running*
// process's current resident memory from the outside; std.process.Child's
// ResourceUsageStatistics is only populated after wait()/kill(), which is
// too late for a before/after comparison.
//
// BUGFIX: Dir.readFileAlloc (and File.readStreaming) return an empty read
// for /proc files on this Io backend - both size their transfer off the
// file's reported stat() size, which procfs reports as 0 for its virtual
// files regardless of actual content. File.readPositional doesn't consult
// that size at all (it's a bare pread), so looping it until it returns 0
// reads the real content - the standard workaround for this well-known
// procfs gotcha.
fn getRssKb(io: std.Io, pid: std.process.Child.Id) !usize {
    var path_buf: [64]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/proc/{d}/status", .{pid});

    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var buf: [16 * 1024]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        var vecs: [1][]u8 = .{buf[total..]};
        const n = try file.readPositional(io, &vecs, total);
        if (n == 0) break;
        total += n;
    }
    const content = buf[0..total];

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "VmRSS:")) continue;
        var it = std.mem.tokenizeAny(u8, line["VmRSS:".len..], " \t");
        const num_str = it.next() orelse return error.VmRSSParseFailed;
        return try std.fmt.parseInt(usize, num_str, 10);
    }
    return error.VmRSSNotFound;
}

fn getInfoRoundTrip(io: std.Io, response_buf: []u8) !void {
    const addr = try net.UnixAddress.init(globals.SPINED_PATH);
    const conn = try addr.connect(io);
    defer conn.close(io);

    var w_buf: [8]u8 = undefined;
    var w = conn.writer(io, &w_buf);
    try w.interface.writeInt(u8, globals.GET_INFO_CODE, .big);
    try w.interface.flush();

    // A large-ish reader buffer, not the 256 bytes node.zig's own getInfo()
    // uses for the same call: that's fine for a single one-off request, but
    // here it turned a ~271KB response into ~1000 tiny read syscalls per
    // connection, holding each connection open far longer than a real
    // client would - which inflated realized concurrency (more connections
    // genuinely in flight at once) and, through smp_allocator's per-active-
    // thread caching, apparent RSS, well past what real traffic causes.
    var r_buf: [8192]u8 = undefined;
    var r = conn.reader(io, &r_buf);
    try r.interface.readSliceAll(response_buf);
}

fn getInfoWorker(io: std.Io, response_buf: []u8, iterations: usize, successes: *std.atomic.Value(usize)) void {
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        getInfoRoundTrip(io, response_buf) catch continue;
        _ = successes.fetchAdd(1, .monotonic);
    }
}

// Fires `worker_count` concurrent workers, each doing `iterations_per_worker`
// sequential GetInfo round trips, and returns spined's RSS (in kB) once
// they've all finished.
fn runGetInfoBatch(io: std.Io, allocator: std.mem.Allocator, pid: std.process.Child.Id, worker_count: usize, iterations_per_worker: usize) !usize {
    const response_size = mad.getRequiredSize(protocol.payloads.GetInfoResponse);

    var successes: std.atomic.Value(usize) = .init(0);
    var group: std.Io.Group = .init;
    var w: usize = 0;
    while (w < worker_count) : (w += 1) {
        const response_buf = try allocator.alloc(u8, response_size);
        try group.concurrent(io, getInfoWorker, .{ io, response_buf, iterations_per_worker, &successes });
    }
    try group.await(io);

    // Sanity check this batch actually exercised spined rather than
    // silently failing every request and passing by accident.
    const total_requests = worker_count * iterations_per_worker;
    try testing.expect(successes.load(.monotonic) > total_requests / 2);

    return getRssKb(io, pid);
}

// Regression test for a real bug: spined used to wrap its entire lifetime in
// a single std.heap.ArenaAllocator (spined/src/main.zig). ArenaAllocator
// only ever reclaims a destroy()/free() call when it happens to be the most
// recent allocation in the arena's current backing chunk - under concurrent
// connections (spined dispatches each one via io.concurrent, so alloc/
// destroy pairs interleave and don't complete in allocation order) that's
// effectively never true, so every connection's r_buf/w_buf and every
// ~270KB GetInfoResponse leaked for good. Reproduced live before the fix:
// RSS went from 7.6MB to 3.3GB after 6000 concurrent GetInfo round trips.
// Fixed by switching spined's allocator to std.heap.smp_allocator, a real
// thread-safe general-purpose allocator that reclaims regardless of order.
//
// Concurrency matters here, not just request count: a purely sequential
// burst of GetInfo calls barely grew RSS even against the old, buggy
// allocator (verified separately) - only genuinely interleaved, concurrent
// connections reproduce the leak, matching how spined actually serves
// traffic in practice.
//
// This asserts *convergence* across repeated batches rather than one
// absolute RSS ceiling. smp_allocator caches memory per active thread and
// isn't eager to return it to the OS, so even the fixed, non-leaking
// allocator legitimately grows RSS well past any small fixed threshold on
// the very first batch under enough concurrency (verified: >100MB on a
// first batch alone at this worker count) - an absolute bound calibrated to
// look "safe" on one run is just a flaky test waiting to happen on
// different hardware/scheduling. What the old, genuinely-leaking arena
// could never do is *plateau*: each batch leaked a roughly constant amount
// more, forever. So this runs several batches against the same spined
// process and requires each later batch's growth to be markedly smaller
// than the first batch's - the actual signature of "warmed up and done
// growing" vs. "leaking a little more every time."
test "spined: repeated concurrent GetInfo batches plateau instead of leaking" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);
    try waitForSpinedReady(io);

    const pid = child.id orelse return error.SpinedHasNoPid;

    // Let RSS settle right after startup before taking the baseline.
    try io.sleep(std.Io.Duration.fromMilliseconds(100), .awake);
    const baseline_kb = try getRssKb(io, pid);

    const worker_count = 40;
    const iterations_per_worker = 75; // 3000 requests per batch, concurrently interleaved
    const batch_count = 4;

    var prev_kb = baseline_kb;
    var first_growth_kb: usize = 0;
    var last_growth_kb: usize = 0;

    var batch: usize = 0;
    while (batch < batch_count) : (batch += 1) {
        const after_kb = try runGetInfoBatch(io, allocator, pid, worker_count, iterations_per_worker);
        const growth_kb = if (after_kb > prev_kb) after_kb - prev_kb else 0;
        print("batch {d}: spined RSS {d}KB -> {d}KB (+{d}KB)\n", .{ batch, prev_kb, after_kb, growth_kb });

        if (batch == 0) first_growth_kb = growth_kb;
        last_growth_kb = growth_kb;
        prev_kb = after_kb;
    }

    // The old, leaking arena grew by a roughly constant amount every batch
    // (no plateau, ever) - the fixed allocator's later batches should cost
    // only a small fraction of the first batch's one-time warm-up growth.
    // Guard against divide-by-zero if the first batch happened to cost
    // nothing at all (would only make the bound below stricter, not wrong).
    const plateau_ceiling_kb = @max(first_growth_kb / 4, 5 * 1024);
    if (last_growth_kb >= plateau_ceiling_kb) {
        print(
            "growth did not plateau: batch 0 grew {d}KB, batch {d} still grew {d}KB (ceiling {d}KB)\n",
            .{ first_growth_kb, batch_count - 1, last_growth_kb, plateau_ceiling_kb },
        );
    }
    try testing.expect(last_growth_kb < plateau_ceiling_kb);
}

fn registerOneTopicConcurrently(node: *spine.Node, idx: usize, errors_out: *std.atomic.Value(usize)) void {
    var name_buf: [64]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, "concurrent_reg_test_topic_{d}", .{idx}) catch return;
    _ = node.publish(u32, name) catch |err| {
        print("worker {d} failed: {any}\n", .{ idx, err });
        _ = errors_out.fetchAdd(1, .monotonic);
    };
}

// Regression test for a real deadlock: Node.sendRegistration (node.zig)
// used to write to and read from self.spined_conn - a single connection
// shared by every entity a Node ever registers - with no lock. Two tasks
// registering entities on the same Node concurrently (e.g. two
// io.concurrent calls to publish()/newService() on one Node) raced their
// reads against each other's writes: spined answers requests in whatever
// order it processes them, not tied to which task's read happens to be
// waiting for a response, so a task could end up blocked forever reading a
// response a different task's read had already consumed. Reproduced live
// before the fix: this exact shape of test hung indefinitely (25s+,
// confirmed via wall-clock timing that it wasn't a compile-time artifact)
// at high enough concurrency - 20 concurrent tasks didn't reproduce it, 150
// reliably did.
//
// Fixed by adding spined_conn_lock (std.Io.Mutex) to Node, held across the
// whole write+read exchange in sendRegistration and across deinit's close
// (not just the read half - that would still let two writes interleave).
//
// Caveat: unlike a wrong-value assertion, if this specific bug ever comes
// back, this test doesn't fail fast - group.await(io) blocks forever right
// along with the deadlocked tasks, so the whole test run hangs rather than
// reporting a clean failure. That mirrors the bug's own nature (no
// timeout, no error) rather than a limitation of the test; relies on the
// CI runner's own job-level timeout to eventually surface it.
test "spine client-lib: concurrent entity registration on one node does not deadlock" {
    const io = testIo();
    const allocator = testAllocator();

    var child = try spawnSpined(io);
    defer child.kill(io);

    var node = try waitForRegisteredNode(io, allocator, "common", "concurrent_reg_test_node");
    defer node.deinit();

    const worker_count = 150;
    var errors_out: std.atomic.Value(usize) = .init(0);
    var group: std.Io.Group = .init;
    var w: usize = 0;
    while (w < worker_count) : (w += 1) {
        try group.concurrent(io, registerOneTopicConcurrently, .{ &node, w, &errors_out });
    }
    try group.await(io);

    try testing.expectEqual(@as(usize, 0), errors_out.load(.monotonic));
}
