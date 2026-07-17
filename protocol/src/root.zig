const std = @import("std");

pub const mad = @import("mad.zig");
pub const globals = @import("globals.zig");
pub const payloads = @import("payloads.zig");
pub const network = @import("network.zig");

// Without this, `zig build test` on the protocol module would have nothing
// forcing it to analyze mad.zig/etc., and would silently run ~0 tests (this
// bit spined pre-merge: see the "renamed server to spined" cleanup).
test {
    std.testing.refAllDecls(@This());
}
