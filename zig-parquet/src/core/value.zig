//! Dynamic value types for nested Parquet data
//!
//! This module provides a recursive `Value` type that can represent any
//! Parquet value, including arbitrarily nested lists, maps, and structs.
//! This enables reading Parquet files without knowing the schema at compile time.

const std = @import("std");

/// Thread-local override for byte payload allocations. When set (typically by
/// `DynamicReader.readRowsInternal`), all `bytes_val` / `fixed_bytes_val`
/// payloads produced via `dupeBytes` go into this allocator instead of the
/// per-value allocator. Paired with `Row.bytes_borrowed = true` so per-value
/// frees in `deinit` skip these payloads; the producer frees the arena en
/// masse. Save/restore the prior value at each entry point — the pattern is:
///
///     const prev = value_mod.bytes_arena_allocator;
///     value_mod.bytes_arena_allocator = arena.allocator();
///     defer value_mod.bytes_arena_allocator = prev;
///
/// This replaces the per-value `allocator.dupe(u8, ...)` hot path (~440k
/// allocations on covid.snappy.parquet) with one arena's worth of contiguous
/// allocations, avoiding the N*stack-trace overhead of DebugAllocator.
pub threadlocal var bytes_arena_allocator: ?std.mem.Allocator = null;

/// Duplicate a byte slice, routing through `bytes_arena_allocator` if set,
/// else through the provided `fallback` allocator. Every producer of
/// `Value.bytes_val` and `Value.fixed_bytes_val` should call this helper
/// instead of `allocator.dupe(u8, ...)` to participate in the arena scheme.
pub fn dupeBytes(fallback: std.mem.Allocator, bytes: []const u8) std.mem.Allocator.Error![]u8 {
    const alloc = bytes_arena_allocator orelse fallback;
    return alloc.dupe(u8, bytes);
}

/// A dynamic value that can represent any Parquet data.
///
/// Unlike the typed `Optional(T)` approach which requires knowing types at
/// compile time, `Value` can represent any nested structure dynamically.
///
/// ## Example
/// ```zig
/// // Represents: {"id": 1, "tags": ["a", "b"]}
/// const row = Value{ .struct_val = &[_]Value.FieldValue{
///     .{ .name = "id", .value = .{ .int64_val = 1 } },
///     .{ .name = "tags", .value = .{ .list_val = &[_]Value{
///         .{ .bytes_val = "a" },
///         .{ .bytes_val = "b" },
///     }}},
/// }};
/// ```
pub const Value = union(enum) {
    // Null
    null_val: void,

    // Primitives
    bool_val: bool,
    int32_val: i32,
    int64_val: i64,
    float_val: f32,
    double_val: f64,
    bytes_val: []const u8,
    fixed_bytes_val: []const u8,

    // Nested types (recursive via slices)
    list_val: []const Value,
    map_val: []const MapEntryValue,
    struct_val: []const FieldValue,

    /// A key-value pair in a map
    pub const MapEntryValue = struct {
        key: Value,
        value: Value,
    };

    /// A named field in a struct
    pub const FieldValue = struct {
        name: []const u8,
        value: Value,
    };

    /// Check if this value is null
    pub fn isNull(self: Value) bool {
        return self == .null_val;
    }

    /// Get the value as a specific type, or return null if type doesn't match
    pub fn asInt32(self: Value) ?i32 {
        return switch (self) {
            .int32_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a specific type, or return null if type doesn't match
    pub fn asInt64(self: Value) ?i64 {
        return switch (self) {
            .int64_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a specific type, or return null if type doesn't match
    pub fn asFloat(self: Value) ?f32 {
        return switch (self) {
            .float_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a specific type, or return null if type doesn't match
    pub fn asDouble(self: Value) ?f64 {
        return switch (self) {
            .double_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a specific type, or return null if type doesn't match
    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .bool_val => |v| v,
            else => null,
        };
    }

    /// Get the value as bytes (string), or return null if type doesn't match
    pub fn asBytes(self: Value) ?[]const u8 {
        return switch (self) {
            .bytes_val => |v| v,
            .fixed_bytes_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a list, or return null if not a list
    pub fn asList(self: Value) ?[]const Value {
        return switch (self) {
            .list_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a map, or return null if not a map
    pub fn asMap(self: Value) ?[]const MapEntryValue {
        return switch (self) {
            .map_val => |v| v,
            else => null,
        };
    }

    /// Get the value as a struct, or return null if not a struct
    pub fn asStruct(self: Value) ?[]const FieldValue {
        return switch (self) {
            .struct_val => |v| v,
            else => null,
        };
    }

    /// Get a field from a struct value by name
    pub fn getField(self: Value, name: []const u8) ?Value {
        const fields = self.asStruct() orelse return null;
        for (fields) |f| {
            if (std.mem.eql(u8, f.name, name)) {
                return f.value;
            }
        }
        return null;
    }

    /// Format value for printing
    pub fn format(
        self: Value,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        switch (self) {
            .null_val => try writer.writeAll("null"),
            .bool_val => |v| try writer.print("{}", .{v}),
            .int32_val => |v| try writer.print("{}", .{v}),
            .int64_val => |v| try writer.print("{}", .{v}),
            .float_val => |v| try formatFloat(f32, v, writer),
            .double_val => |v| try formatFloat(f64, v, writer),
            .bytes_val => |v| try writer.print("\"{s}\"", .{v}),
            .fixed_bytes_val => |v| try writer.print("\"{s}\"", .{v}),
            .list_val => |items| {
                try writer.writeAll("[");
                for (items, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try item.format(writer);
                }
                try writer.writeAll("]");
            },
            .map_val => |entries| {
                try writer.writeAll("{");
                for (entries, 0..) |entry, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try entry.key.format(writer);
                    try writer.writeAll(": ");
                    try entry.value.format(writer);
                }
                try writer.writeAll("}");
            },
            .struct_val => |fields| {
                try writer.writeAll("{");
                for (fields, 0..) |field, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.print("{s}: ", .{field.name});
                    try field.value.format(writer);
                }
                try writer.writeAll("}");
            },
        }
    }

    fn formatFloat(comptime T: type, v: T, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        if (std.math.isInf(v)) {
            if (v < 0) {
                try writer.writeAll("-inf");
            } else {
                try writer.writeAll("inf");
            }
        } else if (std.math.isNan(v)) {
            try writer.writeAll("nan");
        } else {
            try writer.print("{d}", .{v});
        }
    }

    pub const DeinitOptions = struct {
        /// When true, skip freeing `bytes_val` / `fixed_bytes_val` payloads.
        /// Set by producers that routed byte allocations through an arena
        /// (see `bytes_arena_allocator`); those slices are sub-slices of
        /// arena chunks and cannot be freed individually.
        skip_bytes: bool = false,
    };

    /// Free all memory owned by this value recursively.
    ///
    /// If `bytes_arena_allocator` is currently set (i.e. we're inside a
    /// decode context), byte payloads are automatically skipped — they
    /// live in the arena and are freed en masse by the producer. Outside
    /// a decode context this behaves as the classic owned-everything free.
    ///
    /// For explicit control use `deinitWithOptions`.
    pub fn deinit(self: Value, allocator: std.mem.Allocator) void {
        self.deinitWithOptions(allocator, .{ .skip_bytes = bytes_arena_allocator != null });
    }

    /// Like `deinit` but with explicit byte-payload freeing control.
    /// Callers holding a `Row` with `bytes_borrowed = true` pass
    /// `.{ .skip_bytes = true }` so the arena-owned payloads are preserved.
    pub fn deinitWithOptions(self: Value, allocator: std.mem.Allocator, opts: DeinitOptions) void {
        switch (self) {
            .null_val, .bool_val, .int32_val, .int64_val, .float_val, .double_val => {},
            .bytes_val => |v| if (!opts.skip_bytes) allocator.free(v),
            .fixed_bytes_val => |v| if (!opts.skip_bytes) allocator.free(v),
            .list_val => |items| {
                for (items) |item| {
                    item.deinitWithOptions(allocator, opts);
                }
                allocator.free(items);
            },
            .map_val => |entries| {
                for (entries) |entry| {
                    entry.key.deinitWithOptions(allocator, opts);
                    entry.value.deinitWithOptions(allocator, opts);
                }
                allocator.free(entries);
            },
            .struct_val => |fields| {
                for (fields) |field| {
                    allocator.free(field.name);
                    field.value.deinitWithOptions(allocator, opts);
                }
                allocator.free(fields);
            },
        }
    }
};

/// A row of values, one per column
pub const Row = struct {
    values: []Value,
    allocator: std.mem.Allocator,
    /// When true, bytes_val / fixed_bytes_val payloads live in an arena
    /// owned by the producer (e.g. DynamicReader.bytes_arena) and must
    /// NOT be freed per-value. List/map/struct container slices and
    /// struct field names are still freed through `allocator`.
    bytes_borrowed: bool = false,

    /// Get the value for a specific column index
    pub fn getColumn(self: Row, col_idx: usize) ?Value {
        if (col_idx >= self.values.len) return null;
        return self.values[col_idx];
    }

    /// Get the number of columns in this row
    pub fn columnCount(self: Row) usize {
        return self.values.len;
    }

    /// Iterate over all columns, calling the callback for each
    /// Returns early if callback returns false
    pub fn range(self: Row, context: anytype, callback: fn (ctx: @TypeOf(context), col_idx: usize, value: Value) bool) void {
        for (self.values, 0..) |value, col_idx| {
            if (!callback(context, col_idx, value)) {
                break;
            }
        }
    }

    /// Free all memory owned by this row
    pub fn deinit(self: Row) void {
        const opts: Value.DeinitOptions = .{ .skip_bytes = self.bytes_borrowed };
        for (self.values) |v| v.deinitWithOptions(self.allocator, opts);
        self.allocator.free(self.values);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Value null" {
    const v = Value{ .null_val = {} };
    try std.testing.expect(v.isNull());
}

test "Value primitives" {
    const int_v = Value{ .int32_val = 42 };
    try std.testing.expectEqual(@as(?i32, 42), int_v.asInt32());
    try std.testing.expectEqual(@as(?i64, null), int_v.asInt64());

    const str_v = Value{ .bytes_val = "hello" };
    try std.testing.expectEqualStrings("hello", str_v.asBytes().?);
}

test "Value list" {
    const items = [_]Value{
        .{ .int32_val = 1 },
        .{ .int32_val = 2 },
        .{ .int32_val = 3 },
    };
    const list_v = Value{ .list_val = &items };

    const list = list_v.asList().?;
    try std.testing.expectEqual(@as(usize, 3), list.len);
    try std.testing.expectEqual(@as(?i32, 2), list[1].asInt32());
}

test "Value map" {
    const entries = [_]Value.MapEntryValue{
        .{ .key = .{ .bytes_val = "a" }, .value = .{ .int32_val = 1 } },
        .{ .key = .{ .bytes_val = "b" }, .value = .{ .int32_val = 2 } },
    };
    const map_v = Value{ .map_val = &entries };

    const map = map_v.asMap().?;
    try std.testing.expectEqual(@as(usize, 2), map.len);
    try std.testing.expectEqualStrings("a", map[0].key.asBytes().?);
}

test "Value struct" {
    const fields = [_]Value.FieldValue{
        .{ .name = "id", .value = .{ .int64_val = 123 } },
        .{ .name = "name", .value = .{ .bytes_val = "test" } },
    };
    const struct_v = Value{ .struct_val = &fields };

    try std.testing.expectEqual(@as(?i64, 123), struct_v.getField("id").?.asInt64());
    try std.testing.expectEqualStrings("test", struct_v.getField("name").?.asBytes().?);
    try std.testing.expectEqual(@as(?Value, null), struct_v.getField("missing"));
}

test "Value format" {
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();

    const v = Value{ .int32_val = 42 };
    try v.format(&out.writer);
    try std.testing.expectEqualStrings("42", out.written());
}

test "Row getColumn and columnCount" {
    const allocator = std.testing.allocator;

    // Create row values (not owned by allocator for this test)
    var values = try allocator.alloc(Value, 3);
    values[0] = .{ .int32_val = 1 };
    values[1] = .{ .int64_val = 100 };
    values[2] = .{ .bool_val = true };

    const row = Row{
        .values = values,
        .allocator = allocator,
    };
    defer row.deinit();

    try std.testing.expectEqual(@as(usize, 3), row.columnCount());
    try std.testing.expectEqual(@as(?i32, 1), row.getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i64, 100), row.getColumn(1).?.asInt64());
    try std.testing.expectEqual(@as(?bool, true), row.getColumn(2).?.asBool());
    try std.testing.expectEqual(@as(?Value, null), row.getColumn(10));
}

test "Row range" {
    const allocator = std.testing.allocator;

    var values = try allocator.alloc(Value, 2);
    values[0] = .{ .int32_val = 42 };
    values[1] = .{ .int32_val = 99 };

    const row = Row{
        .values = values,
        .allocator = allocator,
    };
    defer row.deinit();

    // Use range to sum all int32 values
    const Context = struct {
        sum: i32 = 0,
    };

    var ctx = Context{};
    row.range(&ctx, struct {
        fn callback(c: *Context, _: usize, value: Value) bool {
            if (value.asInt32()) |v| {
                c.sum += v;
            }
            return true; // continue
        }
    }.callback);

    try std.testing.expectEqual(@as(i32, 141), ctx.sum);
}
