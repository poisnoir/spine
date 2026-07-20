const protocol = @import("protocol");
const std = @import("std");
const entity_mod = @import("entity.zig");
const Entity = entity_mod.Entity;
const print = std.debug.print;
const net = std.Io.net;
const mad = protocol.mad;
const string = mad.string;

// mostly used for logging
const Node = struct {
    name: string = string{},
    id: usize = 0,
};

const EntitySlot = struct {
    entity: Entity = .{ .consumer = .{ .name = string{}, .producer_type = .publisher } },
    node_id: u32 = 0,
};

// have it read from settings the max or st
pub const Namespace = struct {
    name: string = string{},

    entities: [protocol.globals.MAX_ENTITIES]EntitySlot = [_]EntitySlot{EntitySlot{}} ** protocol.globals.MAX_ENTITIES,
    entities_num: u32 = 0,

    nodes: [protocol.globals.MAX_NODES]Node = [_]Node{Node{}} ** protocol.globals.MAX_NODES,
    node_num: u32 = 0,
    node_id_counter: u32 = 0,

    lock: std.Io.Mutex = std.Io.Mutex.init,

    const ProducerKind = enum { Publisher, Service };

    // Only Producer entities (publisher/service) can be "already registered"
    // under a name - Consumers (subscriber/service_caller) are exempt: any
    // number of consumers may share a producer's name. Caller must hold
    // ns.lock.
    fn hasProducer(ns: *@This(), name: *string, kind: ProducerKind) bool {
        for (0..ns.entities_num) |i| {
            const producer = switch (ns.entities[i].entity) {
                .Producer => |*p| p,
                .consumer => continue,
            };
            const matches = switch (kind) {
                .Publisher => switch (producer.*) {
                    .publisher => |*p| string.equal(&p.name, name),
                    .service => false,
                },
                .Service => switch (producer.*) {
                    .service => |*s| string.equal(&s.name, name),
                    .publisher => false,
                },
            };
            if (matches) return true;
        }
        return false;
    }

    // Swap-removes the entity at `index`, same pattern as cleanNode's own
    // removal below. Caller must hold ns.lock and have already verified
    // entities_num > 0 (true here since index came from a loop bounded by
    // entities_num).
    fn removeEntityAt(ns: *@This(), index: usize) void {
        const last_index = ns.entities_num - 1;
        if (index != last_index) {
            ns.entities[index] = ns.entities[last_index];
        }
        ns.entities[last_index] = EntitySlot{};
        ns.entities_num -= 1;
    }

    // Unregisters a Producer entity registered by this connection (node_id)
    // matching name and kind. Scoped to node_id so one node can't unregister
    // another's entity. No-op if there's no match - unregistering something
    // that was never registered (or already removed) is not an error, since
    // the caller's intent is already satisfied either way. Caller must hold
    // ns.lock.
    fn removeProducer(ns: *@This(), node_id: u32, name: *string, kind: ProducerKind) void {
        var i: usize = 0;
        while (i < ns.entities_num) : (i += 1) {
            if (ns.entities[i].node_id != node_id) continue;
            const producer = switch (ns.entities[i].entity) {
                .Producer => |*p| p,
                .consumer => continue,
            };
            const matches = switch (kind) {
                .Publisher => switch (producer.*) {
                    .publisher => |*p| string.equal(&p.name, name),
                    .service => false,
                },
                .Service => switch (producer.*) {
                    .service => |*s| string.equal(&s.name, name),
                    .publisher => false,
                },
            };
            if (matches) {
                ns.removeEntityAt(i);
                return;
            }
        }
    }

    // Same as removeProducer, but for Consumer entities - matched by name
    // and producer_type (subscriber vs service_caller) rather than by kind,
    // since any number of consumers may otherwise share a name. Caller must
    // hold ns.lock.
    fn removeConsumer(ns: *@This(), node_id: u32, name: *string, is_service: bool) void {
        const wanted_type: entity_mod.ProducerType = if (is_service) .service else .publisher;
        var i: usize = 0;
        while (i < ns.entities_num) : (i += 1) {
            if (ns.entities[i].node_id != node_id) continue;
            const consumer = switch (ns.entities[i].entity) {
                .consumer => |*c| c,
                .Producer => continue,
            };
            if (consumer.producer_type == wanted_type and string.equal(&consumer.name, name)) {
                ns.removeEntityAt(i);
                return;
            }
        }
    }

    pub fn handleNode(ns: *@This(), io: std.Io, name: *string, reader: net.Stream.Reader, writer: net.Stream.Writer) !void {
        try ns.lock.lock(io);

        var w = writer;
        var r = reader;

        if (ns.node_num >= protocol.globals.MAX_NODES) {
            ns.lock.unlock(io);
            try w.interface.writeInt(u8, protocol.globals.TOO_MANY_NODES, .big);
            try w.interface.flush();
            return;
        }

        // check if another node with the same name is already created
        for (0..ns.node_num) |i| {
            if (string.equal(&ns.nodes[i].name, name)) {
                ns.lock.unlock(io);
                try w.interface.writeInt(u8, protocol.globals.NODE_ALREADY_REGISTERED, .big);
                try w.interface.flush();
                return;
            }
        }

        const node_id = ns.node_id_counter;
        ns.node_id_counter += 1;

        ns.nodes[ns.node_num] = Node{
            .name = name.*,
            .id = node_id,
        };
        ns.node_num += 1;
        ns.lock.unlock(io);

        // when node disconnects
        defer ns.cleanNode(node_id, io);
        try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
        try w.interface.flush();

        print("node registered: {s}\n", .{name.*.data});
        // ------------------------------------------------------------------------------------------
        // Each iteration starts with a one-byte operation code telling us
        // which of the three registration payloads follows - mirrors how
        // handle_connection dispatches on its own top-level command byte,
        // rather than reading one fixed shape and branching on a field
        // inside it.
        while (true) : (try w.interface.flush()) {
            const op = try r.interface.takeByte();

            const new_entity: EntitySlot = op_blk: switch (op) {
                protocol.globals.REGISTER_PUBLISHER_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.RegisterPublisherPayload));
                    var payload: protocol.payloads.RegisterPublisherPayload = undefined;
                    _ = mad.decode(protocol.payloads.RegisterPublisherPayload, &payload, msg);

                    try ns.lock.lock(io);
                    if (ns.entities_num >= protocol.globals.MAX_ENTITIES) {
                        ns.lock.unlock(io);
                        try w.interface.writeInt(u8, protocol.globals.TOO_MANY_ENTITIES, .big);
                        continue;
                    }
                    if (ns.hasProducer(&payload.name, .Publisher)) {
                        ns.lock.unlock(io);
                        try w.interface.writeInt(u8, protocol.globals.ENTITY_ALREADY_REGISTERED, .big);
                        continue;
                    }

                    print("publisher {s} with len {any} registered, out_type={s} ({d} bytes)\n", .{
                        payload.name.data,                                        payload.name.len,
                        payload.out_type.code.data[0..payload.out_type.code.len], payload.out_type.requiredSize,
                    });

                    break :op_blk .{
                        .entity = .{ .Producer = .{ .publisher = .{
                            .name = payload.name,
                            .outType = payload.out_type,
                        } } },
                        .node_id = node_id,
                    };
                    // TODO: Notify peers
                },
                protocol.globals.REGISTER_SERVICE_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.RegisterServicePayload));
                    var payload: protocol.payloads.RegisterServicePayload = undefined;
                    _ = mad.decode(protocol.payloads.RegisterServicePayload, &payload, msg);

                    try ns.lock.lock(io);
                    if (ns.entities_num >= protocol.globals.MAX_ENTITIES) {
                        ns.lock.unlock(io);
                        try w.interface.writeInt(u8, protocol.globals.TOO_MANY_ENTITIES, .big);
                        continue;
                    }
                    if (ns.hasProducer(&payload.name, .Service)) {
                        ns.lock.unlock(io);
                        try w.interface.writeInt(u8, protocol.globals.ENTITY_ALREADY_REGISTERED, .big);
                        continue;
                    }

                    print("service {s} with len {any} registered, in_type={s} ({d} bytes), out_type={s} ({d} bytes)\n", .{
                        payload.name.data,                                        payload.name.len,
                        payload.in_type.code.data[0..payload.in_type.code.len],   payload.in_type.requiredSize,
                        payload.out_type.code.data[0..payload.out_type.code.len], payload.out_type.requiredSize,
                    });

                    break :op_blk .{
                        .entity = .{ .Producer = .{ .service = .{
                            .name = payload.name,
                            .inType = payload.in_type,
                            .outType = payload.out_type,
                        } } },
                        .node_id = node_id,
                    };
                    // TODO: Notify peers
                },
                protocol.globals.REGISTER_CONSUMER_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.RegisterConsumerPayload));
                    var payload: protocol.payloads.RegisterConsumerPayload = undefined;
                    _ = mad.decode(protocol.payloads.RegisterConsumerPayload, &payload, msg);

                    try ns.lock.lock(io);
                    if (ns.entities_num >= protocol.globals.MAX_ENTITIES) {
                        ns.lock.unlock(io);
                        try w.interface.writeInt(u8, protocol.globals.TOO_MANY_ENTITIES, .big);
                        continue;
                    }

                    print("consumer {s} with len {any} registered, waiting for a {s}\n", .{
                        payload.name.data,                                  payload.name.len,
                        if (payload.is_service) "service" else "publisher",
                    });

                    break :op_blk .{
                        .entity = .{ .consumer = .{
                            .name = payload.name,
                            .producer_type = if (payload.is_service) .service else .publisher,
                        } },
                        .node_id = node_id,
                    };
                },
                protocol.globals.UNREGISTER_PUBLISHER_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.UnregisterPayload));
                    var payload: protocol.payloads.UnregisterPayload = undefined;
                    _ = mad.decode(protocol.payloads.UnregisterPayload, &payload, msg);

                    try ns.lock.lock(io);
                    ns.removeProducer(node_id, &payload.name, .Publisher);
                    ns.lock.unlock(io);

                    try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
                    continue;
                },
                protocol.globals.UNREGISTER_SERVICE_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.UnregisterPayload));
                    var payload: protocol.payloads.UnregisterPayload = undefined;
                    _ = mad.decode(protocol.payloads.UnregisterPayload, &payload, msg);

                    try ns.lock.lock(io);
                    ns.removeProducer(node_id, &payload.name, .Service);
                    ns.lock.unlock(io);

                    try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
                    continue;
                },
                protocol.globals.UNREGISTER_CONSUMER_CODE => {
                    const msg = try r.interface.take(mad.getRequiredSize(protocol.payloads.UnregisterConsumerPayload));
                    var payload: protocol.payloads.UnregisterConsumerPayload = undefined;
                    _ = mad.decode(protocol.payloads.UnregisterConsumerPayload, &payload, msg);

                    try ns.lock.lock(io);
                    ns.removeConsumer(node_id, &payload.name, payload.is_service);
                    ns.lock.unlock(io);

                    try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
                    continue;
                },
                else => {
                    try w.interface.writeInt(u8, protocol.globals.INVALID_ENTITY_TYPE, .big);
                    continue;
                },
            };

            const entity_index = ns.entities_num;
            ns.entities[entity_index] = new_entity;
            ns.entities_num += 1;
            ns.lock.unlock(io);

            try w.interface.writeInt(u8, protocol.globals.OK_STATUS, .big);
        }
    }

    fn cleanNode(ns: *@This(), node_id: usize, io: std.Io) void {
        ns.lock.lockUncancelable(io);
        defer ns.lock.unlock(io);

        // Lookup node
        var found = false;
        var node_index: usize = 0;
        while (node_index < ns.node_num) : (node_index += 1) {
            if (ns.nodes[node_index].id == node_id) {
                found = true;
                break;
            }
        }

        // TODO: Add log here, it should never happen
        if (!found) {
            return;
        }

        print("removing node : {s} in namespace : {s} \n", .{ ns.nodes[node_index].name.data, ns.name.data[0..ns.name.len] });

        var i: usize = ns.entities_num;
        while (i > 0) {
            i -= 1;
            if (ns.entities[i].node_id == node_id) {
                const entities_num = ns.entities_num;
                const last_index = entities_num - 1;
                if (i != last_index) {
                    ns.entities[i] = ns.entities[last_index];
                }
                ns.entities[last_index] = EntitySlot{};
                ns.entities_num -= 1;
            }
        }

        const node_num = ns.node_num;
        const last_node_index = node_num - 1;
        if (node_index != last_node_index) {
            ns.nodes[node_index] = ns.nodes[last_node_index];
        }
        ns.nodes[last_node_index] = Node{};
        ns.node_num -= 1;
    }
};

const testing = @import("std").testing;

test "hasProducer finds a registered publisher by name, ignores other kinds/names" {
    var ns = Namespace{};
    ns.entities[0] = .{
        .entity = .{ .Producer = .{ .publisher = .{ .name = try string.fromConst("temperature") } } },
        .node_id = 1,
    };
    ns.entities_num = 1;

    var wanted = try string.fromConst("temperature");
    try testing.expect(ns.hasProducer(&wanted, .Publisher));
    try testing.expect(!ns.hasProducer(&wanted, .Service));

    var other = try string.fromConst("pressure");
    try testing.expect(!ns.hasProducer(&other, .Publisher));
}

test "hasProducer ignores consumer entities" {
    var ns = Namespace{};
    ns.entities[0] = .{
        .entity = .{ .consumer = .{ .name = try string.fromConst("temperature"), .producer_type = .publisher } },
        .node_id = 1,
    };
    ns.entities_num = 1;

    var wanted = try string.fromConst("temperature");
    try testing.expect(!ns.hasProducer(&wanted, .Publisher));
    try testing.expect(!ns.hasProducer(&wanted, .Service));
}

test "hasProducer returns false on an empty namespace" {
    var ns = Namespace{};
    var wanted = try string.fromConst("anything");
    try testing.expect(!ns.hasProducer(&wanted, .Publisher));
    try testing.expect(!ns.hasProducer(&wanted, .Service));
}

test "removeProducer removes a matching publisher and hasProducer stops finding it" {
    var ns = Namespace{};
    ns.entities[0] = .{
        .entity = .{ .Producer = .{ .publisher = .{ .name = try string.fromConst("temperature") } } },
        .node_id = 1,
    };
    ns.entities_num = 1;

    var wanted = try string.fromConst("temperature");
    ns.removeProducer(1, &wanted, .Publisher);

    try testing.expectEqual(@as(u32, 0), ns.entities_num);
    try testing.expect(!ns.hasProducer(&wanted, .Publisher));
}

test "removeProducer is a no-op for a different node_id, wrong kind, or unknown name" {
    var ns = Namespace{};
    ns.entities[0] = .{
        .entity = .{ .Producer = .{ .publisher = .{ .name = try string.fromConst("temperature") } } },
        .node_id = 1,
    };
    ns.entities_num = 1;

    var wanted = try string.fromConst("temperature");
    var other = try string.fromConst("pressure");

    ns.removeProducer(2, &wanted, .Publisher); // wrong node_id
    ns.removeProducer(1, &wanted, .Service); // wrong kind
    ns.removeProducer(1, &other, .Publisher); // wrong name
    try testing.expectEqual(@as(u32, 1), ns.entities_num);

    ns.removeProducer(1, &wanted, .Publisher); // the real match
    try testing.expectEqual(@as(u32, 0), ns.entities_num);
}

test "removeConsumer removes a matching consumer scoped by node_id and producer_type" {
    var ns = Namespace{};
    ns.entities[0] = .{
        .entity = .{ .consumer = .{ .name = try string.fromConst("temperature"), .producer_type = .publisher } },
        .node_id = 1,
    };
    ns.entities_num = 1;

    var wanted = try string.fromConst("temperature");

    ns.removeConsumer(2, &wanted, false); // wrong node_id
    ns.removeConsumer(1, &wanted, true); // wrong producer_type (wants service, this is publisher)
    try testing.expectEqual(@as(u32, 1), ns.entities_num);

    ns.removeConsumer(1, &wanted, false);
    try testing.expectEqual(@as(u32, 0), ns.entities_num);
}

// Regression test for a real crash: cleanNode used to do
// `ns.lock.lock(io) catch unreachable` - the `defer`-only call site made
// the original author assume lock() "can't fail" here, but lock()'s
// contended path (io.futexWait) is a genuine cancellation point. Verified
// live before the fix: a task blocked in cleanNode on a contended ns.lock,
// when its surrounding Io.Group was cancelled, panicked with "attempt to
// unwrap error: Canceled" at exactly that line. Fixed by switching to
// lockUncancelable, which absorbs cancellation instead of surfacing it -
// exactly what cleanup code invoked from a defer (which can't propagate an
// error anyway) needs.
//
// Reproduces the same shape directly against the real Namespace.cleanNode:
// one task holds ns.lock, a second task calls cleanNode (which must block,
// contended) via a Group, then that Group is cancelled while cleanNode is
// still waiting. Pre-fix this panicked the whole test process; post-fix
// cleanNode simply returns without having done its cleanup, same as any
// other cancelled task would.
test "cleanNode does not panic when cancelled while blocked on a contended lock" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var ns = Namespace{};
    ns.name = try string.fromConst("common");
    ns.nodes[0] = .{ .name = try string.fromConst("held_by_other_task"), .id = 42 };
    ns.node_num = 1;

    const Holder = struct {
        fn run(namespace: *Namespace, io_: std.Io, ready: *std.Io.Event) void {
            namespace.lock.lock(io_) catch unreachable; // uncontended, fine
            ready.set(io_);
            io_.sleep(std.Io.Duration.fromMilliseconds(300), .awake) catch {};
            namespace.lock.unlock(io_);
        }
    };

    var ready: std.Io.Event = .unset;
    var holder = io.async(Holder.run, .{ &ns, io, &ready });
    defer holder.await(io);

    ready.wait(io) catch {};

    var group: std.Io.Group = .init;
    // cleanNode is void, not error-returning, so it's a direct match for
    // Group.concurrent's expected signature - no wrapper needed.
    try group.concurrent(io, Namespace.cleanNode, .{ &ns, 42, io });
    try io.sleep(std.Io.Duration.fromMilliseconds(50), .awake); // let it become contended

    group.cancel(io); // this used to panic the whole process

    // If we get here, the fix held. Whether cleanNode finished its cleanup
    // before being cancelled or not isn't the point of this test (either
    // is a legitimate outcome of racing a cancellation against a lock
    // acquisition) - not panicking is.
}
