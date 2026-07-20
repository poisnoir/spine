const std = @import("std");
const Spined = @import("spined.zig").Spined;

// zig build test's root for this target is this file. Only grabbing the
// `Spined` declaration above doesn't pull spined.zig's own test blocks (or,
// transitively, namespace.zig's) into spined_tests - Zig only analyzes what
// a file actually uses unless something forces the whole target's import
// graph to be walked. Without this, spined_tests reports "0 tests passed"
// even though namespace.zig has real test blocks in it.
test {
    _ = @import("spined.zig");
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    // Not an ArenaAllocator: spined is a long-running daemon that
    // create()/destroy()s per-connection buffers and per-GetInfo response
    // buffers for its entire lifetime, never resetting. An arena only ever
    // grows its backing chunks and reclaims a `free`/`destroy` call solely
    // when it happens to be the most recent allocation in the arena's
    // current chunk (see std.heap.ArenaAllocator's doc comment) - under
    // concurrent connections (handled via io.concurrent, so alloc/destroy
    // pairs interleave and don't complete in the same order they started)
    // that's effectively never true, so every connection's buffers and
    // every GetInfoResponse leaked for good. smp_allocator is a real,
    // thread-safe general-purpose allocator that reclaims every destroy()
    // regardless of ordering, which is what a long-lived daemon needs.
    const allocator = std.heap.smp_allocator;
    var s = try Spined.init(io, allocator);
    try s.run();
}
