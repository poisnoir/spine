const std = @import("std");
const spine = @import("spine");

fn timeTwo(input: u32) anyerror!u32 {
    return input * 2;
}

// Demo entry point: `zig build run -- <mode>` where mode is one of publish,
// subscribe, service, call (defaults to publish).
//
// publish/subscribe talk to the "temperature" uint32 topic in the "common"
// namespace, matching spine-go/example/publisher and
// spine-go/example/subscriber. service/call talk to the "time_two" uint32->
// uint32 service, matching spine-go/example/service and
// spine-go/example/service_caller — so any of these four can be tested
// against either language's counterpart.
pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);

    const mode = if (args.len > 1) args[1] else "publish";

    const node_name = if (std.mem.eql(u8, mode, "subscribe"))
        "spine-zig-subscriber-demo"
    else if (std.mem.eql(u8, mode, "service"))
        "spine-zig-service-demo"
    else if (std.mem.eql(u8, mode, "call"))
        "spine-zig-service-caller-demo"
    else
        "spine-zig-publisher-demo";

    var node = try spine.Node.init("common", node_name, io, allocator);
    defer node.deinit();

    if (node.spined_conn == null) {
        std.debug.print("running in local-only mode (spined not reachable)\n", .{});
    }

    if (std.mem.eql(u8, mode, "subscribe")) {
        const sub = try node.subscribe(u32, "temperature");
        var i: usize = 0;
        while (i < 20) : (i += 1) {
            const value = try sub.next();
            std.debug.print("received temperature: {d}\n", .{value});
        }
    } else if (std.mem.eql(u8, mode, "service")) {
        _ = try node.newService(u32, u32, "time_two", timeTwo);
        std.debug.print("service started\n", .{});
        while (true) {
            try io.sleep(std.Io.Duration.fromSeconds(3600), .awake);
        }
    } else if (std.mem.eql(u8, mode, "call")) {
        const caller = try node.newServiceCaller(u32, u32, "time_two");
        const result = try caller.call(2);
        std.debug.print("{d}\n", .{result});
    } else if (std.mem.eql(u8, mode, "call-loop")) {
        // Long-running caller for exercising reconnect-on-drop live: start
        // this, kill the service process it's talking to, restart it, and
        // watch calls fail then recover on their own.
        const caller = try node.newServiceCaller(u32, u32, "time_two");
        var i: u32 = 0;
        while (true) : (i += 1) {
            if (caller.call(i)) |result| {
                std.debug.print("call({d}) = {d}\n", .{ i, result });
            } else |err| {
                std.debug.print("call({d}) failed: {any}\n", .{ i, err });
            }
            try io.sleep(std.Io.Duration.fromMilliseconds(500), .awake);
        }
    } else {
        const publisher = try node.publish(u32, "temperature");
        var temp: u32 = 0;
        while (true) : (temp += 1) {
            try publisher.publish(temp);
            std.debug.print("published temperature: {d}\n", .{temp});
            try io.sleep(std.Io.Duration.fromMilliseconds(15), .awake);
        }
    }

    // BUGFIX: same reasoning as bench.zig — service/call both leave a
    // Publisher/Service-style background accept loop alive on init.io's own
    // thread pool, so returning normally from main() here would hang forever
    // in start.zig's `defer threaded.deinit()` waiting to join it.
    std.process.exit(0);
}
