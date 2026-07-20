const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Shared wire-protocol module (the mad codec, the fixed-size string
    // type, every command/status/peer-type code, and the
    // CreateNamespacePayload/AddPeerPayload/GetInfoResponse payloads) that
    // spined and every client import, so they can't silently drift apart on
    // what the wire format means.
    const protocol = b.addModule("protocol", .{
        .root_source_file = b.path("protocol/src/root.zig"),
        .target = target,
    });

    const spined_exe = b.addExecutable(.{
        .name = "spined",
        .root_module = b.createModule(.{
            .root_source_file = b.path("spined/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protocol", .module = protocol },
            },
        }),
    });
    b.installArtifact(spined_exe);

    // spine_uart: standalone peer-bridge process for a UART device. spined
    // launches it (rather than driving serial I/O itself) so a flaky device
    // driver can't take down the registry daemon every node on the machine
    // depends on for discovery - see spined/readme.md's peer-process notes.
    // Takes no imports yet: it doesn't register with spined or client-zig as
    // a node yet, just validates its own argv (name, port, speed).
    const spine_uart_exe = b.addExecutable(.{
        .name = "spine_uart",
        .root_module = b.createModule(.{
            .root_source_file = b.path("spine-uart/src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(spine_uart_exe);

    // spine_tcp: standalone peer-bridge process for a TCP link to another
    // machine, same stub/supervised-subprocess role as spine_uart above but
    // for spined's still-missing cross-machine transport (see spined/readme.md's
    // Limitations: "Local machine only"). Takes no imports yet: it doesn't
    // register with spined or client-zig as a node yet, just validates its
    // own argv (connect <ip-addr> | listen <port>).
    const spine_tcp_exe = b.addExecutable(.{
        .name = "spine_tcp",
        .root_module = b.createModule(.{
            .root_source_file = b.path("spine-tcp/src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(spine_tcp_exe);

    // client-zig: the node client library. Its own module is exposed as
    // "spine" (so consumers write `@import("spine")`) and sits on top of
    // protocol the same way spined does.
    const spine = b.addModule("spine", .{
        .root_source_file = b.path("client-zig/src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "protocol", .module = protocol },
        },
    });

    const client_zig_exe = b.addExecutable(.{
        .name = "spine",
        .root_module = b.createModule(.{
            .root_source_file = b.path("client-zig/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spine", .module = spine },
            },
        }),
    });
    b.installArtifact(client_zig_exe);

    // Benchmark executable (see client-zig/src/bench.zig for why this is a
    // plain executable rather than a `test`: Zig's test runner has no
    // built-in equivalent of `go test -bench`).
    const client_zig_bench_exe = b.addExecutable(.{
        .name = "spine_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("client-zig/src/bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spine", .module = spine },
            },
        }),
    });
    b.installArtifact(client_zig_bench_exe);

    const run_spined_step = b.step("run-spined", "Run spined");
    const run_spined_cmd = b.addRunArtifact(spined_exe);
    run_spined_step.dependOn(&run_spined_cmd.step);
    run_spined_cmd.step.dependOn(b.getInstallStep());

    const run_spine_uart_step = b.step("run-spine-uart", "Run spine_uart -- <name> <port> <speed>");
    const run_spine_uart_cmd = b.addRunArtifact(spine_uart_exe);
    run_spine_uart_step.dependOn(&run_spine_uart_cmd.step);
    run_spine_uart_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_spine_uart_cmd.addArgs(args);
    }

    const run_spine_tcp_step = b.step("run-spine-tcp", "Run spine_tcp -- connect <ip-addr> | listen <port>");
    const run_spine_tcp_cmd = b.addRunArtifact(spine_tcp_exe);
    run_spine_tcp_step.dependOn(&run_spine_tcp_cmd.step);
    run_spine_tcp_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_spine_tcp_cmd.addArgs(args);
    }

    const run_client_zig_step = b.step("run-client-zig", "Run the spine client-zig demo (publish/subscribe/service/call)");
    const run_client_zig_cmd = b.addRunArtifact(client_zig_exe);
    run_client_zig_step.dependOn(&run_client_zig_cmd.step);
    run_client_zig_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_client_zig_cmd.addArgs(args);
    }

    const bench_step = b.step("bench", "Run client-zig's pub/sub and service benchmarks");
    const bench_cmd = b.addRunArtifact(client_zig_bench_exe);
    bench_step.dependOn(&bench_cmd.step);
    bench_cmd.step.dependOn(b.getInstallStep());

    // protocol_tests runs mad.zig's codec unit tests. spined_tests has no
    // test blocks of its own today, but building it as a test forces the
    // whole reachable file graph (namespace.zig, peer.zig, etc. for spined)
    // to compile-check, the same role spined's old root.zig hack served
    // pre-merge. client_zig_tests runs node.zig's pubsub/service tests
    // (real Unix-socket roundtrips, no spined required — Node.init falls
    // back to local-only mode when spined isn't reachable).
    const protocol_tests = b.addTest(.{ .root_module = protocol });
    const run_protocol_tests = b.addRunArtifact(protocol_tests);

    const spined_tests = b.addTest(.{ .root_module = spined_exe.root_module });
    const run_spined_tests = b.addRunArtifact(spined_tests);

    const spine_uart_tests = b.addTest(.{ .root_module = spine_uart_exe.root_module });
    const run_spine_uart_tests = b.addRunArtifact(spine_uart_tests);

    const spine_tcp_tests = b.addTest(.{ .root_module = spine_tcp_exe.root_module });
    const run_spine_tcp_tests = b.addRunArtifact(spine_tcp_tests);

    const client_zig_tests = b.addTest(.{ .root_module = spine });
    const run_client_zig_tests = b.addRunArtifact(client_zig_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_protocol_tests.step);
    test_step.dependOn(&run_spined_tests.step);
    test_step.dependOn(&run_spine_uart_tests.step);
    test_step.dependOn(&run_spine_tcp_tests.step);
    test_step.dependOn(&run_client_zig_tests.step);

    // Separate from `test`: these spawn a real spined binary as a
    // subprocess and drive it over its real Unix sockets, rather than
    // testing in-process logic - meaningfully slower, and needs the
    // binary actually built and installed first (hence depending on the
    // install step below), unlike the fast in-process tests above.
    const integration_tests_exe = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("integration-tests/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spine", .module = spine },
                .{ .name = "protocol", .module = protocol },
            },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests_exe);
    run_integration_tests.step.dependOn(b.getInstallStep());

    const integration_test_step = b.step("test-integration", "Run integration tests that spawn a real spined process");
    integration_test_step.dependOn(&run_integration_tests.step);
}
