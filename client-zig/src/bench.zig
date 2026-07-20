const std = @import("std");
const spine = @import("spine");

// Rough equivalents of spine-go's pubsub_benchmark_test.go and
// service_benchmark_test.go. Zig's test runner has no built-in benchmark
// harness (unlike `go test -bench`), so this is its own small executable
// instead of `test` blocks — run it with `zig build bench` (add
// `-Doptimize=ReleaseFast` for numbers that mean anything; Debug builds
// include safety checks that would otherwise dominate the timing).
pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();

    var node = try spine.Node.init("common", "spine-zig-bench-node", io, allocator);
    defer node.deinit();

    try benchPubSub(&node, io);
    try benchServiceCall(&node, io);
    try benchServiceCallParallel(&node, io);

    // BUGFIX: returning normally from main() here would hang forever instead
    // of exiting. Publisher/Service both spawn an accept loop onto init.io's
    // own Threaded thread pool, and that loop blocks in accept() forever (no
    // Close()/deinit() for entities — see Node.subscribe's BUGFIX notes).
    // start.zig's wrapper runs `defer threaded.deinit()` around main(), and
    // deinit() joins every worker thread on that pool before letting the
    // process actually exit — so it waits on those accept() calls forever.
    // std.process.exit terminates the process directly, the same way an
    // external SIGTERM/SIGKILL already did for every other long-running
    // demo in this codebase (main.zig's publish loop, spined itself, etc.),
    // instead of going through that join.
    std.process.exit(0);
}

fn now(io: std.Io) std.Io.Clock.Timestamp {
    return std.Io.Clock.Timestamp.now(io, .awake);
}

fn elapsedNs(start: std.Io.Clock.Timestamp, io: std.Io) u64 {
    return @intCast(start.untilNow(io).raw.toNanoseconds());
}

fn report(name: []const u8, iterations: u64, elapsed_ns: u64) void {
    std.debug.print("{s}: {d} iterations, {d} ns/op, {d}ms total\n", .{
        name,
        iterations,
        elapsed_ns / iterations,
        elapsed_ns / std.time.ns_per_ms,
    });
}

// Equivalent of BenchmarkPubSub: one publisher/subscriber pair, loop
// publish+next, timing only the loop.
fn benchPubSub(node: *spine.Node, io: std.Io) !void {
    const publisher = try node.publish(u32, "bench_pubsub_topic");
    const subscriber = try node.subscribe(u32, "bench_pubsub_topic");

    const iterations: u64 = 20_000;
    const start = now(io);
    var i: u64 = 0;
    while (i < iterations) : (i += 1) {
        try publisher.publish(@intCast(i));
        _ = try subscriber.next();
    }
    report("BenchmarkPubSub", iterations, elapsedNs(start, io));
}

fn timesTwo(input: u32) anyerror!u32 {
    return input * 2;
}

// Equivalent of BenchmarkServiceCall: one service/caller pair, loop call(),
// timing only the loop.
fn benchServiceCall(node: *spine.Node, io: std.Io) !void {
    _ = try node.newService(u32, u32, "bench_service_call", timesTwo);
    const caller = try node.newServiceCaller(u32, u32, "bench_service_call");

    const iterations: u64 = 20_000;
    const start = now(io);
    var i: u64 = 0;
    while (i < iterations) : (i += 1) {
        _ = try caller.call(@intCast(i));
    }
    report("BenchmarkServiceCall", iterations, elapsedNs(start, io));
}

fn serviceCallWorker(caller: *spine.ServiceCaller(u32, u32), iterations: u64) void {
    var i: u64 = 0;
    while (i < iterations) : (i += 1) {
        _ = caller.call(@intCast(i)) catch |err| {
            std.debug.print("BenchmarkServiceCallParallel: worker call failed: {any}\n", .{err});
            return;
        };
    }
}

// Equivalent of BenchmarkServiceCallParallel: several workers hammering the
// *same* caller (and so the same underlying connection) concurrently —
// mirrors Go's b.RunParallel, which shares one caller across goroutines too.
// This measures contention on ServiceCaller.call's mutex plus the service's
// single per-connection handler loop, not "N independent connections."
fn benchServiceCallParallel(node: *spine.Node, io: std.Io) !void {
    _ = try node.newService(u32, u32, "bench_service_call_parallel", timesTwo);
    const caller = try node.newServiceCaller(u32, u32, "bench_service_call_parallel");

    const worker_count: u64 = 8;
    const iterations_per_worker: u64 = 2_500;
    const total_iterations = worker_count * iterations_per_worker;

    var group: std.Io.Group = .init;
    const start = now(io);
    var w: u64 = 0;
    while (w < worker_count) : (w += 1) {
        try group.concurrent(io, serviceCallWorker, .{ caller, iterations_per_worker });
    }
    try group.await(io);
    report("BenchmarkServiceCallParallel", total_iterations, elapsedNs(start, io));
}
