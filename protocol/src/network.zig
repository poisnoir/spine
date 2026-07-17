const std = @import("std");
const net = std.Io.net;
const Dir = std.Io.Dir;

pub const BindError = error{AlreadyBound};

// Binds a Unix domain socket at `path`, self-healing past a stale socket
// file left by a crashed previous owner. bind() failing because the path
// is occupied is ambiguous on its own: it looks identical whether
// something is genuinely listening there right now, or it's just a stale
// file with nothing behind it. Probing with connect() first disambiguates:
// something answering means a real, live owner already exists (fail with
// error.AlreadyBound, don't steal its socket out from under it); nothing
// answering means it's dead and safe to clear before binding for real.
//
// Note: connecting to a stale socket file with nothing listening currently
// makes this Zig version print a noisy (but harmless) "unexpected errno:
// 111" trace to stderr - it's still a normal, catchable error.Unexpected,
// not a crash, just cosmetically alarming on every restart-after-crash.
pub fn bind(io: std.Io, path: []const u8) !net.Server {
    const dir = std.fs.path.dirname(path) orelse return error.InvalidPath;
    try Dir.cwd().createDirPath(io, dir);

    const addr = try net.UnixAddress.init(path);

    if (addr.connect(io)) |stream| {
        stream.close(io);
        return BindError.AlreadyBound;
    } else |_| {
        Dir.deleteFileAbsolute(io, path) catch {};
    }

    return try addr.listen(io, .{});
}

// Dials a Unix domain socket at `path` that some other process is expected
// to own (a producer, spined, ...). If the owner crashed, its socket file
// lingers with nothing listening - connect() fails here too, but unlike
// bind() above, a dialer isn't trying to claim the path for itself, just
// reporting "not up" to its own retry loop. Without clearing the file,
// every retry would re-trigger the noisy errno path against a file already
// known to be dead; clearing it here means only the first attempt against
// a given stale file is noisy - either this dialer's next retry, or the
// real owner's own bind() rebind, sees a clean slate afterward.
pub fn dial(io: std.Io, path: []const u8) !net.Stream {
    const addr = try net.UnixAddress.init(path);
    return addr.connect(io) catch |err| {
        Dir.deleteFileAbsolute(io, path) catch {};
        return err;
    };
}
