const std = @import("std");
const protocol = @import("protocol");
const Subscriber = @import("subscriber.zig").Subscriber;
const Publisher = @import("publisher.zig").Publisher;
const ServiceCaller = @import("service_caller.zig").ServiceCaller;
const Service = @import("service.zig").Service;

const net = std.Io.net;
const mad = protocol.mad;
const string = protocol.mad.string;
const print = std.debug.print;

pub const RegisterError = error{
    InvalidNamespace,
    NodeAlreadyRegistered,
    TooManyNodes,
    UnexpectedStatus,
};

pub const EntityRegisterError = error{
    TooManyEntities,
    InvalidEntityType,
    EntityAlreadyRegistered,
    TooManyUnknownEntities,
    UnexpectedStatus,
};

// Errors returned by addNamespace - mirrors squid's own `add namespace`
// command (squid/src/main.zig's addNamespace), just as a library call
// instead of a separate CLI invocation.
pub const NamespaceError = error{
    NamespaceAlreadyRegistered,
    TooManyNamespaces,
    UnexpectedStatus,
};

pub const Node = struct {
    namespace: []const u8,
    name: []const u8,
    io: std.Io,
    allocator: std.mem.Allocator,

    // TODO: Investigate removing local-mode:
    // If spined reaches its stable point I think it might be good to retire local-mode
    // ------------------------------------------------------------------------
    // null when spined isn't reachable at all — mirrors spine-go's
    // "local-only mode" fallback (node.go's CreateNode): a node still works
    // for purely local use, it just isn't registered anywhere and can't be
    // discovered by anything else.
    spined_conn: ?net.Stream,

    pub fn init(
        namespace: []const u8,
        name: []const u8,
        io: std.Io,
        allocator: std.mem.Allocator,
    ) !Node {
        const addr = net.UnixAddress.init(protocol.globals.SPINED_PATH) catch |err| {
            print("spine: invalid spined path {s}: {any}\n", .{ protocol.globals.SPINED_PATH, err });
            return localOnly(namespace, name, io, allocator);
        };

        const conn = addr.connect(io) catch |err| {
            print("spine: could not connect to spined ({any}), operating in local-only mode\n", .{err});
            return localOnly(namespace, name, io, allocator);
        };

        // If spined rejects node it shouldn't be even running in local node.
        registerNode(conn, io, namespace, name) catch |err| {
            conn.close(io);
            return err;
        };

        print("spine: node '{s}' registered in namespace '{s}'\n", .{ name, namespace });

        return .{
            .namespace = namespace,
            .name = name,
            .io = io,
            .allocator = allocator,
            .spined_conn = conn,
        };
    }

    pub fn deinit(self: *Node) void {
        if (self.spined_conn) |conn| {
            conn.close(self.io);
            self.spined_conn = null;
        }
    }

    // Subscribes to `topic`, decoding each published message as K. K must
    // match the publisher's payload type exactly — mad's type-fingerprint
    // handshake (see Subscriber(K).connect below) rejects the connection
    // otherwise, mirroring spine-go's Subscriber/Publisher pair.
    //
    // Heap-allocated (via self.allocator) rather than returned by value: the
    // returned Subscriber holds a buffered net.Stream.Reader whose buffer
    // slice points back into the Subscriber itself, so its address must
    // never change after connect() wires that reader up.
    pub fn subscribe(self: *Node, comptime K: type, topic: []const u8) !*Subscriber(K) {
        // if running locally, there's nothing to tell spined, but the
        // subscriber can still connect directly to the publisher's socket
        // by convention path (mirrors spine-go's registerToSpined no-op).
        // Waiting for a publisher, not a service: is_service = false.
        try self.registerConsumer(topic, false);
        errdefer self.unregisterConsumer(topic, false) catch {};

        const sub = try self.allocator.create(Subscriber(K));
        errdefer self.allocator.destroy(sub);

        try sub.connect(self.io, self.namespace, topic);
        return sub;
    }

    // Sends a one-byte operation code (telling spined which of the three
    // registration payloads follows - see protocol/src/globals.zig) and
    // then the payload itself. The status-handling is identical across all
    // three kinds; only the outgoing payload shape differs, which is why
    // this is generic over PayloadType rather than three separate copies of
    // the same send/read-status logic.
    fn sendRegistration(self: *Node, comptime PayloadType: type, op_code: u8, payload: PayloadType) !void {
        const conn = self.spined_conn orelse return;

        var w_buf: [256]u8 = undefined;
        var r_buf: [256]u8 = undefined;
        var w = conn.writer(self.io, &w_buf);
        var r = conn.reader(self.io, &r_buf);

        try w.interface.writeInt(u8, op_code, .big);

        const size = mad.getRequiredSize(PayloadType);
        var msg_buf: [256]u8 = undefined;
        _ = mad.encode(PayloadType, payload, msg_buf[0..size]);

        try w.interface.writeAll(msg_buf[0..size]);
        try w.interface.flush();

        const status = try r.interface.takeInt(u8, .big);
        return switch (status) {
            protocol.globals.OK_STATUS => {},
            protocol.globals.TOO_MANY_ENTITIES => EntityRegisterError.TooManyEntities,
            protocol.globals.INVALID_ENTITY_TYPE => EntityRegisterError.InvalidEntityType,
            protocol.globals.ENTITY_ALREADY_REGISTERED => EntityRegisterError.EntityAlreadyRegistered,
            protocol.globals.TOO_MANY_UNKNOWN_ENTITIES => EntityRegisterError.TooManyUnknownEntities,
            else => EntityRegisterError.UnexpectedStatus,
        };
    }

    fn registerPublisher(self: *Node, name: []const u8, out_type: mad.MadType) !void {
        try self.sendRegistration(protocol.payloads.RegisterPublisherPayload, protocol.globals.REGISTER_PUBLISHER_CODE, .{
            .name = try string.fromConst(name),
            .out_type = out_type,
        });
    }

    fn registerService(self: *Node, name: []const u8, in_type: mad.MadType, out_type: mad.MadType) !void {
        try self.sendRegistration(protocol.payloads.RegisterServicePayload, protocol.globals.REGISTER_SERVICE_CODE, .{
            .name = try string.fromConst(name),
            .in_type = in_type,
            .out_type = out_type,
        });
    }

    // is_service: which kind of producer this consumer is waiting for -
    // false for a subscriber (wants a publisher), true for a service_caller
    // (wants a service). Mirrors entity.zig's Consumer.producer_type.
    fn registerConsumer(self: *Node, name: []const u8, is_service: bool) !void {
        try self.sendRegistration(protocol.payloads.RegisterConsumerPayload, protocol.globals.REGISTER_CONSUMER_CODE, .{
            .name = try string.fromConst(name),
            .is_service = is_service,
        });
    }

    // Rolls back registerPublisher/registerService/registerConsumer - used
    // via errdefer right after a successful register call, so that if a
    // later step (binding the real socket, connecting to a producer) fails,
    // spined doesn't keep believing this entity exists with nothing backing
    // it. Errors from the unregister call itself are intentionally
    // swallowed by callers (`catch {}`): we're already unwinding from a
    // different failure, and there's nothing more useful to do with a
    // second one here than leave spined's entity list slightly stale until
    // the node disconnects (cleanNode still catches it then).
    fn unregisterPublisher(self: *Node, name: []const u8) !void {
        try self.sendRegistration(protocol.payloads.UnregisterPayload, protocol.globals.UNREGISTER_PUBLISHER_CODE, .{
            .name = try string.fromConst(name),
        });
    }

    fn unregisterService(self: *Node, name: []const u8) !void {
        try self.sendRegistration(protocol.payloads.UnregisterPayload, protocol.globals.UNREGISTER_SERVICE_CODE, .{
            .name = try string.fromConst(name),
        });
    }

    fn unregisterConsumer(self: *Node, name: []const u8, is_service: bool) !void {
        try self.sendRegistration(protocol.payloads.UnregisterConsumerPayload, protocol.globals.UNREGISTER_CONSUMER_CODE, .{
            .name = try string.fromConst(name),
            .is_service = is_service,
        });
    }

    // Publishes K values on `topic`. Registers with spined *first* - if
    // spined already has a producer under this name (see namespace.zig's
    // hasProducer), it says so immediately and this returns
    // EntityRegisterError.EntityAlreadyRegistered without ever attempting to
    // bind a socket. In local-only mode (spined unreachable) registration is
    // a no-op, so bind() remains the only check that runs - it's still the
    // real, OS-enforced backstop for that case, and for the (rarer) case of
    // a genuine duplicate spined doesn't know about yet.
    //
    // If the bind() below fails for some reason after spined has already
    // approved the name, the errdefer unregisters it again immediately. Same
    // heap-allocation reasoning as subscribe() above — the returned Publisher's
    // client list is mutated from a background accept task, so its address must be stable.
    pub fn publish(self: *Node, comptime K: type, topic: []const u8) !*Publisher(K) {
        try self.registerPublisher(topic, try mad.madTypeOf(K));
        errdefer self.unregisterPublisher(topic) catch {};

        const p = try self.allocator.create(Publisher(K));
        errdefer self.allocator.destroy(p);

        try p.listen(self.io, self.namespace, topic);
        return p;
    }

    // Serves K->V requests on `name`. Mirrors spine-go's NewService /
    // NewThreadedService — they share an identical wire protocol
    // (service_common.go), so this one type is compatible with either.
    // handler is called once per request, possibly concurrently across
    // clients (each accepted connection gets its own io.concurrent task).
    // Registers with spined before binding - see publish()'s comment above.
    pub fn newService(self: *Node, comptime K: type, comptime V: type, name: []const u8, handler: *const fn (K) anyerror!V) !*Service(K, V) {
        try self.registerService(name, try mad.madTypeOf(K), try mad.madTypeOf(V));
        errdefer self.unregisterService(name) catch {};

        const s = try self.allocator.create(Service(K, V));
        errdefer self.allocator.destroy(s);

        try s.listen(self.io, self.namespace, name, handler);
        return s;
    }

    // Calls the K->V service registered as `name`. Mirrors spine-go's
    // NewServiceCaller.
    pub fn newServiceCaller(self: *Node, comptime K: type, comptime V: type, name: []const u8) !*ServiceCaller(K, V) {
        // Waiting for a service, not a publisher: is_service = true.
        try self.registerConsumer(name, true);
        errdefer self.unregisterConsumer(name, true) catch {};

        const c = try self.allocator.create(ServiceCaller(K, V));
        errdefer self.allocator.destroy(c);
        // one-time init — dial() (called by connect(), including on later
        // reconnects) never touches this again; see dial()'s BUGFIX comment.
        c.lock = .init;

        try c.connect(self.io, self.namespace, name);
        return c;
    }
};

// Creates a namespace on spined - exposed here as a library call.
pub fn addNamespace(io: std.Io, name: []const u8) !void {
    const addr = try net.UnixAddress.init(protocol.globals.SPINED_PATH);
    const conn = try addr.connect(io);
    defer conn.close(io);

    var w_buf: [256]u8 = undefined;
    var r_buf: [8]u8 = undefined;
    var w = conn.writer(io, &w_buf);
    var r = conn.reader(io, &r_buf);

    try w.interface.writeInt(u8, protocol.globals.ADD_NAMESPACE_CODE, .big);

    const payload = protocol.payloads.CreateNamespacePayload{ .name = try string.fromConst(name) };
    const size = mad.getRequiredSize(protocol.payloads.CreateNamespacePayload);
    var msg_buf: [256]u8 = undefined;
    _ = mad.encode(protocol.payloads.CreateNamespacePayload, payload, msg_buf[0..size]);

    try w.interface.writeAll(msg_buf[0..size]);
    try w.interface.flush();

    const status = try r.interface.takeInt(u8, .big);
    return switch (status) {
        protocol.globals.OK_STATUS => {},
        protocol.globals.NAMESPACE_ALREADY_REGISTERED => NamespaceError.NamespaceAlreadyRegistered,
        protocol.globals.TOO_MANY_NAMESPACES => NamespaceError.TooManyNamespaces,
        else => NamespaceError.UnexpectedStatus,
    };
}

// Fetches every namespace/node spined currently knows about
// GetInfoResponse is ~270KB fully populated (see spined.zig's
// handle_get_info), so it's heap-allocated via `allocator` rather than
// returned by value.
pub fn getInfo(io: std.Io, allocator: std.mem.Allocator) !*protocol.payloads.GetInfoResponse {
    const addr = try net.UnixAddress.init(protocol.globals.SPINED_PATH);
    const conn = try addr.connect(io);
    defer conn.close(io);

    var w_buf: [8]u8 = undefined;
    var w = conn.writer(io, &w_buf);
    try w.interface.writeInt(u8, protocol.globals.GET_INFO_CODE, .big);
    try w.interface.flush();

    const size = mad.getRequiredSize(protocol.payloads.GetInfoResponse);
    const raw = try allocator.alloc(u8, size);
    defer allocator.free(raw);

    var r_buf: [256]u8 = undefined;
    var r = conn.reader(io, &r_buf);
    try r.interface.readSliceAll(raw);

    const info = try allocator.create(protocol.payloads.GetInfoResponse);
    _ = mad.decode(protocol.payloads.GetInfoResponse, info, raw);
    return info;
}

fn localOnly(namespace: []const u8, name: []const u8, io: std.Io, allocator: std.mem.Allocator) Node {
    return .{
        .namespace = namespace,
        .name = name,
        .io = io,
        .allocator = allocator,
        .spined_conn = null,
    };
}

fn registerNode(conn: net.Stream, io: std.Io, namespace: []const u8, name: []const u8) !void {
    const payload = protocol.payloads.RegisterNodePayload{
        .namespace_name = try string.fromConst(namespace),
        .node_name = try string.fromConst(name),
    };

    var w_buf: [256]u8 = undefined;
    var r_buf: [256]u8 = undefined;
    var w = conn.writer(io, &w_buf);
    var r = conn.reader(io, &r_buf);

    try w.interface.writeInt(u8, protocol.globals.REGISTER_NODE_CODE, .big);

    const size = mad.getRequiredSize(protocol.payloads.RegisterNodePayload);
    var msg_buf: [256]u8 = undefined;
    _ = mad.encode(protocol.payloads.RegisterNodePayload, payload, msg_buf[0..size]);

    try w.interface.writeAll(msg_buf[0..size]);
    try w.interface.flush();

    const status = try r.interface.takeInt(u8, .big);
    return switch (status) {
        protocol.globals.OK_STATUS => {},
        protocol.globals.INVALID_NAMESPACE => RegisterError.InvalidNamespace,
        protocol.globals.NODE_ALREADY_REGISTERED => RegisterError.NodeAlreadyRegistered,
        protocol.globals.TOO_MANY_NODES => RegisterError.TooManyNodes,
        else => RegisterError.UnexpectedStatus,
    };
}
