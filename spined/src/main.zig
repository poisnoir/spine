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
    // Initialize Socketchild_allocator: Allocator
    const io = init.io;
    const gpa = std.heap.page_allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();
    var s = try Spined.init(io, allocator);
    try s.run();
}
