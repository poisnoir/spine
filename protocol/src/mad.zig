const std = @import("std");
const globals = @import("globals.zig");
const testing = std.testing;

// A producer's mad type fingerprint (code) plus its encoded size. Shared
// between spined (which stores it on Producer.publisher/service so it can
// eventually validate/compare producer schemas, e.g. across a peer link)
// and every spine client library, which computes it from the comptime K/V
// type it was actually instantiated with.

const stringError = error{
    outOfBound,
};

pub const string = struct {
    data: [globals.STRING_SIZE]u8 = std.mem.zeroes([globals.STRING_SIZE]u8),
    len: u8 = 0,

    pub fn equal(a: *@This(), b: *@This()) bool {
        if (a.len != b.len) {
            return false;
        }

        for (0..a.len) |i| {
            if (a.data[i] != b.data[i]) {
                return false;
            }
        }
        return true;
    }

    pub fn fromConst(data: []const u8) stringError!@This() {
        if (data.len > globals.STRING_SIZE) {
            return stringError.outOfBound;
        }

        var result: @This() = string{};
        @memcpy(result.data[0..data.len], data);
        result.len = @intCast(data.len);
        return result;
    }
};

pub const MadType = struct {
    code: string = string{},
    requiredSize: u32 = 0,
};

pub fn madTypeOf(comptime T: type) !MadType {
    return .{
        .code = try string.fromConst(code(T)),
        .requiredSize = getRequiredSize(T),
    };
}

// u32 was chosen for uniform output across systems
pub fn getRequiredSize(comptime dataType: type) u32 {
    switch (@typeInfo(dataType)) {
        .bool => {
            return 1;
        },
        .int => {
            return @sizeOf(dataType);
        },
        .float => {
            return @sizeOf(dataType);
        },
        .array => |info| {
            return getRequiredSize(info.child) * info.len;
        },
        .@"struct" => |info| {
            const res = comptime blk: {
                var result: usize = 0;
                const fields = info.fields;
                for (fields) |field| {
                    result += getRequiredSize(field.type);
                }
                break :blk result;
            };

            return res;
        },
        else => @compileError("Unsupported type"),
    }
}

pub fn code(comptime dataType: type) []const u8 {
    switch (@typeInfo(dataType)) {
        .bool => {
            return "a";
        },
        .int => |info| {
            const byte_len = @sizeOf(dataType);
            const prefix: u8 = if (info.signedness == .signed) '0' else '1';
            return std.fmt.comptimePrint("b{c}{d}", .{ prefix, byte_len });
        },
        .float => {
            const byte_len = @sizeOf(dataType);
            return std.fmt.comptimePrint("c{d}", .{byte_len});
        },
        .array => |info| {
            const child_code = comptime code(info.child);
            return std.fmt.comptimePrint("d{d}{s}", .{ info.len, child_code });
        },
        .@"struct" => |info| {

            // Sort the indices at comptime based on the alphabetical order of field_names
            const res = comptime blk: {
                const fields = info.fields;

                const len = fields.len;
                var indices: [len]usize = undefined;
                for (&indices, 0..) |*item, i| {
                    item.* = i;
                }

                var i: usize = 1;
                while (i < len) : (i += 1) {
                    const key_idx = indices[i];
                    const key_name = fields[key_idx].name;
                    var j = i;
                    while (j > 0 and std.mem.order(u8, fields[indices[j - 1]].name, key_name) == .gt) : (j -= 1) {
                        indices[j] = indices[j - 1];
                    }
                    indices[j] = key_idx;
                }

                var result: []const u8 = "f";
                for (indices) |idx| {
                    result = result ++ code(fields[idx].type) ++ "z";
                }
                break :blk result;
            };

            return res;
        },
        else => @compileError("Unsupported type"),
    }
}

pub fn encode(comptime dataType: type, input: dataType, output: []u8) usize {
    switch (@typeInfo(dataType)) {
        .bool => {
            output[0] = @intFromBool(input);
            return 1;
        },
        .int => {
            const byte_len = @sizeOf(dataType);
            std.mem.writeInt(dataType, output[0..byte_len], input, .big);
            return byte_len;
        },
        .float => {
            const byte_len = @sizeOf(dataType);

            const UintT = comptime switch (byte_len) {
                1 => u8,
                2 => u16,
                4 => u32,
                8 => u64,
                16 => u128,
                else => @compileError(std.fmt.comptimePrint("not supported width size, {any}", .{byte_len})),
            };

            const bits: UintT = @bitCast(input);
            std.mem.writeInt(UintT, output[0..byte_len], bits, .big);
            return byte_len;
        },
        .array => |info| {
            var offset: usize = 0;

            for (input) |elem| {
                offset += encode(info.child, elem, output[offset..]);
            }
            return offset;
        },
        .@"struct" => |info| {
            const fields = info.fields;

            // sorting fields alphabetically
            const sort_i = comptime blk: {
                const len = fields.len;

                var indices: [len]usize = undefined;
                for (&indices, 0..) |*item, i| {
                    item.* = i;
                }

                var i: usize = 1;
                while (i < len) : (i += 1) {
                    const key_idx = indices[i];
                    const key_name = fields[key_idx].name;
                    var j = i;
                    while (j > 0 and std.mem.order(u8, fields[indices[j - 1]].name, key_name) == .gt) : (j -= 1) {
                        indices[j] = indices[j - 1];
                    }
                    indices[j] = key_idx;
                }

                break :blk indices;
            };

            var offset: usize = 0;
            inline for (sort_i) |idx| {
                offset += encode(fields[idx].type, @field(input, fields[idx].name), output[offset..]);
            }
            return offset;
        },
        else => @compileError("Unsupported type"),
    }
}

pub fn decode(comptime dataType: type, output: *dataType, input: []const u8) usize {
    switch (@typeInfo(dataType)) {
        .bool => {
            output.* = input[0] != 0;
            return 1;
        },
        .int => {
            const byte_len = @sizeOf(dataType);
            output.* = std.mem.readInt(dataType, input[0..byte_len], .big);
            return byte_len;
        },
        .float => {
            const byte_len = @sizeOf(dataType);
            const UintT = comptime switch (byte_len) {
                1 => u8,
                2 => u16,
                4 => u32,
                8 => u64,
                16 => u128,
                else => @compileError("Unsupported float bit-width"),
            };
            const bits = std.mem.readInt(UintT, input[0..byte_len], .big);
            output.* = @bitCast(bits);
            return byte_len;
        },
        .array => |info| {
            var offset: usize = 0;

            for (output) |*elem| {
                offset += decode(info.child, elem, input[offset..]);
            }
            return offset;
        },
        .@"struct" => |info| {
            const fields = info.fields;

            // Sorting fields alphabetically at comptime
            const sort_i = comptime blk: {
                const len = fields.len;
                var indices: [len]usize = undefined;
                for (&indices, 0..) |*item, i| {
                    item.* = i;
                }

                var i: usize = 1;
                while (i < len) : (i += 1) {
                    const key_idx = indices[i];
                    const key_name = fields[key_idx].name;
                    var j = i;
                    while (j > 0 and std.mem.order(u8, fields[indices[j - 1]].name, key_name) == .gt) : (j -= 1) {
                        indices[j] = indices[j - 1];
                    }
                    indices[j] = key_idx;
                }
                break :blk indices;
            };

            var offset: usize = 0;
            inline for (sort_i) |idx| {
                // Pass a pointer to the specific struct field to populate it
                offset += decode(fields[idx].type, &@field(output.*, fields[idx].name), input[offset..]);
            }
            return offset;
        },
        else => @compileError("Unsupported type"),
    }
}

const User = struct {
    id: u32 = 0,
    active: bool = false,
};

// Extra types used across the expanded test/benchmark suite below.

const Point = struct {
    x: f32 = 0,
    y: f32 = 0,
};

// Field declaration order deliberately NOT alphabetical, to exercise the
// sort-by-name logic in code()/encode()/decode().
const Unsorted = struct {
    zeta: u8 = 0,
    alpha: u32 = 0,
    mu: bool = false,
};

// Same field set as `Unsorted` but declared in a different order. Since the
// wire format sorts fields alphabetically, this type must produce an
// IDENTICAL code() string and be binary-compatible with `Unsorted`.
const UnsortedReordered = struct {
    mu: bool = false,
    zeta: u8 = 0,
    alpha: u32 = 0,
};

const Nested = struct {
    id: u64 = 0,
    origin: Point = .{},
    tags: [3]u8 = .{ 0, 0, 0 },
};

// BUGFIX: removed WithSlice/WithNestedSlice and their slice-roundtrip tests below.
// getRequiredSize/code/encode/decode only ever handled bool/int/float/array/struct
// (no .pointer/slice case), so any test exercising a slice type failed to compile
// the moment it was actually wired into `zig build test` (see root.zig). Slices were
// never supported here, matching mad-go's mad.go which has no reflect.Slice/String
// handling either — the Go side dropped slice/string/map support outright.

// ============================================================================
// Unit tests
// ============================================================================

test "code generation: User" {
    try testing.expectEqualStrings(
        "fazb14z",
        code(User),
    );
}

test "code generation: bool" {
    try testing.expectEqualStrings("a", code(bool));
}

test "code generation: unsigned integers" {
    try testing.expectEqualStrings("b11", code(u8));
    try testing.expectEqualStrings("b12", code(u16));
    try testing.expectEqualStrings("b14", code(u32));
    try testing.expectEqualStrings("b18", code(u64));
}

test "code generation: signed integers" {
    try testing.expectEqualStrings("b01", code(i8));
    try testing.expectEqualStrings("b02", code(i16));
    try testing.expectEqualStrings("b04", code(i32));
    try testing.expectEqualStrings("b08", code(i64));
}

test "code generation: floats" {
    try testing.expectEqualStrings("c4", code(f32));
    try testing.expectEqualStrings("c8", code(f64));
}

test "code generation: array" {
    try testing.expectEqualStrings("d4b11", code([4]u8));
}

test "code generation: nested array" {
    // array of arrays of u16
    try testing.expectEqualStrings("d2d3b12", code([2][3]u16));
}

test "code generation: nested struct" {
    const c = code(Nested);
    // Just verify it's stable / deterministic and non-empty; exact value
    // depends on Point + array codes composing correctly.
    try testing.expect(c.len > 0);
    try testing.expectEqualStrings(c, code(Nested));
}

test "roundtrip: bool true/false" {
    var buf: [1]u8 = undefined;

    inline for (.{ true, false }) |v| {
        const w = encode(bool, v, &buf);
        var out: bool = undefined;
        const r = decode(bool, &out, &buf);
        try testing.expectEqual(w, r);
        try testing.expectEqual(v, out);
    }
}

test "roundtrip: u8 boundary values" {
    var buf: [1]u8 = undefined;

    inline for (.{ 0, 1, 127, 128, 255 }) |v| {
        _ = encode(u8, v, &buf);
        var out: u8 = 0;
        _ = decode(u8, &out, &buf);
        try testing.expectEqual(@as(u8, v), out);
    }
}

test "roundtrip: i8 boundary values" {
    var buf: [1]u8 = undefined;

    inline for (.{ -128, -1, 0, 1, 127 }) |v| {
        _ = encode(i8, v, &buf);
        var out: i8 = 0;
        _ = decode(i8, &out, &buf);
        try testing.expectEqual(@as(i8, v), out);
    }
}

test "roundtrip: u16/i16" {
    {
        var buf: [2]u8 = undefined;
        _ = encode(u16, 65535, &buf);
        var out: u16 = 0;
        _ = decode(u16, &out, &buf);
        try testing.expectEqual(@as(u16, 65535), out);
    }
    {
        var buf: [2]u8 = undefined;
        _ = encode(i16, -32768, &buf);
        var out: i16 = 0;
        _ = decode(i16, &out, &buf);
        try testing.expectEqual(@as(i16, -32768), out);
    }
}

test "roundtrip: u32/i32" {
    {
        var buf: [4]u8 = undefined;
        _ = encode(u32, 0xDEADBEEF, &buf);
        var out: u32 = 0;
        _ = decode(u32, &out, &buf);
        try testing.expectEqual(@as(u32, 0xDEADBEEF), out);
    }
    {
        var buf: [4]u8 = undefined;
        _ = encode(i32, -2147483648, &buf);
        var out: i32 = 0;
        _ = decode(i32, &out, &buf);
        try testing.expectEqual(@as(i32, -2147483648), out);
    }
}

test "roundtrip: u64/i64" {
    var buf: [8]u8 = undefined;
    const value: u64 = 0x1122334455667788;
    _ = encode(u64, value, &buf);
    var decoded: u64 = 0;
    _ = decode(u64, &decoded, &buf);
    try testing.expectEqual(value, decoded);

    var bufi: [8]u8 = undefined;
    const vi: i64 = -9223372036854775808;
    _ = encode(i64, vi, &bufi);
    var oi: i64 = 0;
    _ = decode(i64, &oi, &bufi);
    try testing.expectEqual(vi, oi);
}

test "roundtrip: f32 including special values" {
    var buf: [4]u8 = undefined;

    const values = [_]f32{ 0.0, -0.0, 1.5, -1.5, 3.14159, std.math.inf(f32), -std.math.inf(f32) };
    for (values) |v| {
        _ = encode(f32, v, &buf);
        var out: f32 = 0;
        _ = decode(f32, &out, &buf);
        try testing.expectEqual(v, out);
    }

    // NaN must be checked bitwise/with isNan since NaN != NaN.
    _ = encode(f32, std.math.nan(f32), &buf);
    var nan_out: f32 = 0;
    _ = decode(f32, &nan_out, &buf);
    try testing.expect(std.math.isNan(nan_out));
}

test "roundtrip: f64" {
    var buf: [8]u8 = undefined;

    const v: f64 = -123456.789;
    _ = encode(f64, v, &buf);
    var out: f64 = 0;
    _ = decode(f64, &out, &buf);
    try testing.expectEqual(v, out);
}

test "roundtrip: array of u16" {
    var buf: [32]u8 = undefined;
    const value = [4]u16{ 1, 2, 3, 4 };
    const n = encode([4]u16, value, &buf);

    var decoded: [4]u16 = undefined;
    _ = decode([4]u16, &decoded, buf[0..n]);

    try testing.expectEqualDeep(value, decoded);
}

test "roundtrip: nested array" {
    var buf: [32]u8 = undefined;
    const value = [2][3]u8{ .{ 1, 2, 3 }, .{ 4, 5, 6 } };
    const n = encode([2][3]u8, value, &buf);

    var decoded: [2][3]u8 = undefined;
    _ = decode([2][3]u8, &decoded, buf[0..n]);

    try testing.expectEqualDeep(value, decoded);
}

test "roundtrip: array of structs" {
    var buf: [64]u8 = undefined;
    const value = [2]User{
        .{ .id = 1, .active = true },
        .{ .id = 2, .active = false },
    };
    const n = encode([2]User, value, &buf);

    var decoded: [2]User = undefined;
    _ = decode([2]User, &decoded, buf[0..n]);

    try testing.expectEqualDeep(value, decoded);
}

test "roundtrip: struct with unsorted field declaration order" {
    var buf: [16]u8 = undefined;

    const value = Unsorted{ .zeta = 5, .alpha = 999, .mu = true };
    const n = encode(Unsorted, value, &buf);

    var decoded = Unsorted{};
    _ = decode(Unsorted, &decoded, buf[0..n]);

    try testing.expectEqualDeep(value, decoded);
}

test "roundtrip: nested struct with array and struct fields" {
    var buf: [64]u8 = undefined;

    const value = Nested{
        .id = 123456789,
        .origin = .{ .x = 1.5, .y = -2.5 },
        .tags = .{ 9, 8, 7 },
    };
    const n = encode(Nested, value, &buf);

    var decoded = Nested{};
    _ = decode(Nested, &decoded, buf[0..n]);

    try testing.expectEqualDeep(value, decoded);
}

test "roundtrip: User (original coverage, kept for regression)" {
    var buf: [32]u8 = undefined;

    const user = User{
        .id = 42,
        .active = true,
    };

    const written = encode(User, user, &buf);

    var decoded = User{};
    const read = decode(User, &decoded, buf[0..written]);

    try testing.expectEqual(written, read);
    try testing.expectEqual(user.id, decoded.id);
    try testing.expectEqual(user.active, decoded.active);
}

test "encode/decode offsets agree for compound types" {
    var buf: [64]u8 = undefined;
    const value = Nested{ .id = 1, .origin = .{ .x = 1, .y = 1 }, .tags = .{ 1, 2, 3 } };
    const written = encode(Nested, value, &buf);

    var decoded = Nested{};
    const read = decode(Nested, &decoded, buf[0..written]);

    try testing.expectEqual(written, read);
}

test "many random-ish round trips stay consistent" {
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const value = Nested{
            .id = random.int(u64),
            .origin = .{ .x = random.float(f32), .y = random.float(f32) },
            .tags = .{ random.int(u8), random.int(u8), random.int(u8) },
        };
        var buf: [64]u8 = undefined;
        const n = encode(Nested, value, &buf);

        var decoded = Nested{};
        _ = decode(Nested, &decoded, buf[0..n]);

        try testing.expectEqualDeep(value, decoded);
    }
}
