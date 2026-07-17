const std = @import("std");

// Shared exponential-backoff policy for Subscriber.connect and
// ServiceCaller.connect: both retry a dial+handshake against a peer that may
// not have started listening yet (or is mid-restart), and a tight retry
// loop would otherwise spin a core at 100% doing nothing but failing
// connects.
const initial_ms: i64 = 100;
const max_ms: i64 = 5000;

pub const Backoff = struct {
    ms: i64 = initial_ms,

    // Sleeps for the current backoff duration, then grows it (capped at
    // max_ms) for the next call.
    pub fn sleep(self: *Backoff, io: std.Io) !void {
        try io.sleep(std.Io.Duration.fromMilliseconds(self.ms), .awake);
        self.ms = @min(self.ms * 2, max_ms);
    }
};
