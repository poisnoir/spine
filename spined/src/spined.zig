const std = @import("std");
const protocol = @import("protocol");
const Namespace = @import("namespace.zig").Namespace;

const net = std.Io.net;
const print = std.debug.print;

const mad = protocol.mad;
const string = mad.string;

// Only grabbing the `Namespace` declaration above doesn't pull its `test`
// blocks into spined_tests - Zig only analyzes what's actually used unless
// something forces the whole file's top-level scope to be walked. Without
// this, namespace.zig's tests silently never run (spined_tests reports "0
// tests passed" despite namespace.zig having real test blocks in it).
test {
    _ = @import("namespace.zig");
}

pub const Spined = struct {
    io: std.Io,
    allocator: std.mem.Allocator,

    namespaces: [protocol.globals.MAX_NAMESPACES]Namespace = [_]Namespace{Namespace{}} ** protocol.globals.MAX_NAMESPACES,
    namespace_num: u8 = 0,
    namespaces_lock: std.Io.Mutex = std.Io.Mutex.init,

    server: net.Server,

    pub fn init(io: std.Io, allc: std.mem.Allocator) !*@This() {
        const server = protocol.network.bind(io, protocol.globals.SPINED_PATH) catch |err| switch (err) {
            error.AlreadyBound => return error.SpinedAlreadyRunning,
            else => return err,
        };

        const s = try allc.create(@This());
        s.* = .{
            .io = io,
            .allocator = allc,
            .server = server,
        };

        s.namespaces[0].name = try string.fromConst("common");
        s.namespace_num = 1;

        return s;
    }

    pub fn run(self: *@This()) !void {
        while (true) {
            const s = try self.server.accept(self.io);
            print("client connected\n", .{});
            // heap-allocated per connection rather than stack-allocated here.
            const r_buf = try self.allocator.create([protocol.globals.MAX_PACKET_SIZE]u8);
            const w_buf = try self.allocator.create([protocol.globals.MAX_PACKET_SIZE]u8);

            _ = try self.io.concurrent(handle_connection, .{ self, s, self.io, r_buf, w_buf });
        }
    }

    // Every connection starts with one command byte (protocol/src/globals.zig)
    // telling us whether this is a spine node registering itself, or a
    // one-shot squid control command - one socket for both instead of two
    // that had to be kept in sync by hand.
    pub fn handle_connection(self: *@This(), stream: net.Stream, io: std.Io, r_buf: *[protocol.globals.MAX_PACKET_SIZE]u8, w_buf: *[protocol.globals.MAX_PACKET_SIZE]u8) !void {
        defer stream.close(io);
        defer self.allocator.destroy(r_buf);
        defer self.allocator.destroy(w_buf);

        var r = stream.reader(io, r_buf);
        var w = stream.writer(io, w_buf);

        const command = try r.interface.takeByte();

        if (command == protocol.globals.REGISTER_NODE_CODE) {
            return self.handle_register_node(io, r, w);
        }

        if (command == protocol.globals.GET_INFO_CODE) {
            try self.handle_get_info(io, &w);
            return;
        }

        const status = switch (command) {
            protocol.globals.ADD_NAMESPACE_CODE => try self.handle_add_namespace(io, &r),
            else => protocol.globals.INVALID_COMMAND,
        };

        try w.interface.writeInt(u8, status, .big);
        try w.interface.flush();
    }

    fn handle_register_node(self: *@This(), io: std.Io, reader: net.Stream.Reader, writer: net.Stream.Writer) !void {
        var r = reader;
        var w = writer;

        const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.RegisterNodePayload));
        var nodeData: protocol.payloads.RegisterNodePayload = undefined;
        _ = mad.decode(protocol.payloads.RegisterNodePayload, &nodeData, msg);

        try self.namespaces_lock.lock(io);
        var found = false;
        var namespace_index: usize = 0;
        while (namespace_index < self.namespace_num) : (namespace_index += 1) {
            if (string.equal(&self.namespaces[namespace_index].name, &nodeData.namespace_name)) {
                found = true;
                break;
            }
        }
        self.namespaces_lock.unlock(io);

        if (!found) {
            try w.interface.writeInt(u8, protocol.globals.INVALID_NAMESPACE, .big);
            try w.interface.flush();
            return;
        }

        const ns = &self.namespaces[namespace_index];
        try ns.handleNode(io, &nodeData.node_name, r, w);
    }

    fn handle_add_namespace(self: *@This(), io: std.Io, r: *net.Stream.Reader) !u8 {
        const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.CreateNamespacePayload));
        var payload: protocol.payloads.CreateNamespacePayload = undefined;
        _ = mad.decode(protocol.payloads.CreateNamespacePayload, &payload, msg);

        try self.namespaces_lock.lock(io);
        defer self.namespaces_lock.unlock(io);

        var i: usize = 0;
        while (i < self.namespace_num) : (i += 1) {
            if (string.equal(&self.namespaces[i].name, &payload.name)) {
                return protocol.globals.NAMESPACE_ALREADY_REGISTERED;
            }
        }

        if (self.namespace_num >= protocol.globals.MAX_NAMESPACES) {
            return protocol.globals.TOO_MANY_NAMESPACES;
        }

        self.namespaces[self.namespace_num] = Namespace{};
        self.namespaces[self.namespace_num].name = payload.name;
        self.namespace_num += 1;

        print("namespace registered: {s}\n", .{payload.name.data[0..payload.name.len]});
        return protocol.globals.OK_STATUS;
    }

    // TODO: This size looks too big for a response. It might be good to
    // reinvest in slices in mad
    // GetInfoResponse is ~270KB fully populated (fixed-size arrays sized to
    fn handle_get_info(self: *@This(), io: std.Io, w: *net.Stream.Writer) !void {
        const response = try self.allocator.create(protocol.payloads.GetInfoResponse);
        defer self.allocator.destroy(response);
        @memset(std.mem.asBytes(response), 0);

        try self.namespaces_lock.lock(io);
        const namespace_num = self.namespace_num;
        self.namespaces_lock.unlock(io);

        response.namespace_num = namespace_num;
        for (0..namespace_num) |i| {
            const ns = &self.namespaces[i];
            const info = &response.namespaces[i];

            try ns.lock.lock(io);
            info.name = ns.name;

            info.node_num = ns.node_num;
            for (0..ns.node_num) |j| {
                info.nodes[j] = .{ .name = ns.nodes[j].name };
            }

            ns.lock.unlock(io);
        }

        const size = mad.getRequiredSize(protocol.payloads.GetInfoResponse);
        const encoded = try self.allocator.alloc(u8, size);
        defer self.allocator.free(encoded);
        const n = mad.encode(protocol.payloads.GetInfoResponse, response.*, encoded);

        try w.interface.writeAll(encoded[0..n]);
        try w.interface.flush();
    }
};
