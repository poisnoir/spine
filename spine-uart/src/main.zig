const std = @import("std");

fn usage() void {
    std.debug.print(
        \\usage: spine_uart <name> <port> <speed>
        \\
    , .{});
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // args[0] is the program name.
    const tokens = args[1..];

    if (tokens.len != 3) {
        usage();
        std.process.exit(1);
    }

    const name = tokens[0];
    const port = tokens[1];
    const speed = std.fmt.parseInt(u32, tokens[2], 10) catch {
        std.debug.print("invalid speed \"{s}\": must be a positive integer baud rate\n", .{tokens[2]});
        std.process.exit(1);
    };

    // TODO: open the UART device at `port` @ `speed`, register as a spine
    // peer bridge under `name`, and start relaying.
    std.debug.print("spine_uart: name='{s}' port='{s}' speed={d} (not yet implemented)\n", .{ name, port, speed });
}
