const std = @import("std");
const protocol = @import("protocol");

// Every entity type in this module (Publisher, Service) spawns an
// acceptLoop that runs forever on a background thread — there's no
// Close()/deinit() for entities yet (see readme.md's Known gaps) — so every
// test file in this module shares this one Io.Threaded/arena instead of one
// per test, to avoid tearing down a std.Io.Threaded that would mean joining
// a thread permanently blocked in accept(). Never deinit-ing here and
// letting the test process exit reclaim everything mirrors how the rest of
// this codebase already treats entity lifetime.
var test_threaded: std.Io.Threaded = undefined;
var test_arena: std.heap.ArenaAllocator = undefined;
var test_io_ready = false;

fn ensureTestIoReady() void {
    if (test_io_ready) return;

    // BUGFIX: default async_limit is cpu_count-1 — fine for a handful of
    // background accept-loop tasks, but every test file in this module
    // shares this one Io, and Service adds a *second* forever-running task
    // per test (one per accepted caller connection, since ServiceCaller
    // never closes its connection either). Set generously high so the limit
    // tracks "how many tests exist across the whole module," not "how many
    // cores this machine has."
    test_threaded = .init(std.heap.page_allocator, .{
        .async_limit = .limited(256),
        .concurrent_limit = .limited(256),
    });
    test_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    test_io_ready = true;

    // Test topic/service names are fixed, so a previous run's socket files
    // are still sitting there on every subsequent run - network.bind's
    // connect probe finds them (nothing listening) and self-heals, but that
    // path currently makes this Zig version print a noisy "unexpected errno:
    // 111" stack trace per test (see network.zig's bind comment). Clearing
    // both directories once, up front, means a normal test run never has to
    // touch that path at all - only an actual crash mid-suite would leave
    // something for the next run to clean up.
    const io = test_threaded.io();
    std.Io.Dir.cwd().deleteTree(io, protocol.globals.PUBLISHER_SOCKET_DIR) catch {};
    std.Io.Dir.cwd().deleteTree(io, protocol.globals.SERVICE_SOCKET_DIR) catch {};
}

pub fn testIo() std.Io {
    ensureTestIoReady();
    return test_threaded.io();
}

pub fn testAllocator() std.mem.Allocator {
    ensureTestIoReady();
    return test_arena.allocator();
}
