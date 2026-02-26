//! Statistics Builder
//!
//! Computes column statistics (min/max/null_count) during column writing.
//! Uses PLAIN encoding for min/max values.

const std = @import("std");
const format = @import("format.zig");
const Optional = @import("types.zig").Optional;
const geo = @import("geo/mod.zig");

/// Generic statistics builder for primitive types.
/// Tracks min/max values and null count during column writing.
pub fn StatisticsBuilder(comptime T: type) type {
    return struct {
        min: ?T = null,
        max: ?T = null,
        null_count: i64 = 0,
        has_values: bool = false,

        const Self = @This();

        /// Update statistics with a slice of non-null values.
        pub fn update(self: *Self, values: []const T) void {
            for (values) |v| {
                self.updateValue(v);
            }
        }

        /// Update statistics with a single value.
        pub fn updateValue(self: *Self, value: T) void {
            if (!self.has_values) {
                self.min = value;
                self.max = value;
                self.has_values = true;
            } else {
                if (compare(value, self.min.?) == .lt) {
                    self.min = value;
                }
                if (compare(value, self.max.?) == .gt) {
                    self.max = value;
                }
            }
        }

        /// Update statistics with a slice of nullable values.
        /// Deprecated: Use updateOptional with Optional(T) instead.
        pub fn updateNullable(self: *Self, values: []const ?T) void {
            for (values) |v| {
                if (v) |val| {
                    self.updateValue(val);
                } else {
                    self.null_count += 1;
                }
            }
        }

        /// Update statistics with a slice of Optional values (unified API).
        /// This is the preferred method - accepts the same Optional(T) type that Reader returns.
        pub fn updateOptional(self: *Self, values: []const Optional(T)) void {
            for (values) |v| {
                switch (v) {
                    .value => |val| self.updateValue(val),
                    .null_value => self.null_count += 1,
                }
            }
        }

        /// Add to null count (for cases where nulls are tracked separately).
        pub fn addNulls(self: *Self, count: i64) void {
            self.null_count += count;
        }

        /// Build the Statistics struct with PLAIN-encoded min/max values.
        /// Returns null if no values were recorded.
        pub fn build(self: *const Self, allocator: std.mem.Allocator) !?format.Statistics {
            if (!self.has_values) {
                // No values - only return null_count if we have nulls
                if (self.null_count > 0) {
                    return format.Statistics{
                        .null_count = self.null_count,
                    };
                }
                return null;
            }

            const min_bytes = try encodeValue(allocator, self.min.?);
            errdefer allocator.free(min_bytes);

            const max_bytes = try encodeValue(allocator, self.max.?);
            errdefer allocator.free(max_bytes);

            // Duplicate for deprecated fields (both point to same encoded value)
            const min_deprecated = try allocator.dupe(u8, min_bytes);
            errdefer allocator.free(min_deprecated);

            const max_deprecated = try allocator.dupe(u8, max_bytes);

            return format.Statistics{
                .min = min_deprecated, // deprecated field
                .max = max_deprecated, // deprecated field
                .null_count = self.null_count,
                .distinct_count = null,
                .min_value = min_bytes, // current standard
                .max_value = max_bytes, // current standard
            };
        }

        /// Compare two values of type T.
        fn compare(a: T, b: T) std.math.Order {
            if (T == bool) {
                // false < true
                const a_int: u1 = if (a) 1 else 0;
                const b_int: u1 = if (b) 1 else 0;
                return std.math.order(a_int, b_int);
            } else if (T == []const u8) {
                // Lexicographic byte comparison
                return std.mem.order(u8, a, b);
            } else {
                // Numeric types: use standard comparison
                return std.math.order(a, b);
            }
        }

        /// Encode a value to bytes using PLAIN encoding.
        fn encodeValue(allocator: std.mem.Allocator, value: T) ![]u8 {
            if (T == i32) {
                const bytes = try allocator.alloc(u8, 4);
                std.mem.writeInt(i32, bytes[0..4], value, .little);
                return bytes;
            } else if (T == i64) {
                const bytes = try allocator.alloc(u8, 8);
                std.mem.writeInt(i64, bytes[0..8], value, .little);
                return bytes;
            } else if (T == f32) {
                const bytes = try allocator.alloc(u8, 4);
                const bits: u32 = @bitCast(value);
                std.mem.writeInt(u32, bytes[0..4], bits, .little);
                return bytes;
            } else if (T == f64) {
                const bytes = try allocator.alloc(u8, 8);
                const bits: u64 = @bitCast(value);
                std.mem.writeInt(u64, bytes[0..8], bits, .little);
                return bytes;
            } else if (T == bool) {
                const bytes = try allocator.alloc(u8, 1);
                bytes[0] = if (value) 1 else 0;
                return bytes;
            } else if (T == []const u8) {
                // Byte arrays: copy raw bytes (no length prefix in statistics)
                return try allocator.dupe(u8, value);
            } else {
                @compileError("Unsupported type for statistics: " ++ @typeName(T));
            }
        }
    };
}

/// Statistics builder for byte array columns (variable length).
/// Uses lexicographic comparison for min/max.
pub const ByteArrayStatisticsBuilder = struct {
    min: ?[]u8 = null,
    max: ?[]u8 = null,
    null_count: i64 = 0,
    has_values: bool = false,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        if (self.min) |m| self.allocator.free(m);
        if (self.max) |m| self.allocator.free(m);
    }

    /// Update statistics with a slice of byte array values.
    pub fn update(self: *Self, values: []const []const u8) !void {
        for (values) |v| {
            try self.updateValue(v);
        }
    }

    /// Update statistics with a single byte array value.
    pub fn updateValue(self: *Self, value: []const u8) !void {
        if (!self.has_values) {
            self.min = try self.allocator.dupe(u8, value);
            self.max = try self.allocator.dupe(u8, value);
            self.has_values = true;
        } else {
            if (std.mem.order(u8, value, self.min.?) == .lt) {
                self.allocator.free(self.min.?);
                self.min = try self.allocator.dupe(u8, value);
            }
            if (std.mem.order(u8, value, self.max.?) == .gt) {
                self.allocator.free(self.max.?);
                self.max = try self.allocator.dupe(u8, value);
            }
        }
    }

    /// Update statistics with a slice of nullable byte array values.
    /// Deprecated: Use updateOptional with Optional([]const u8) instead.
    pub fn updateNullable(self: *Self, values: []const ?[]const u8) !void {
        for (values) |v| {
            if (v) |val| {
                try self.updateValue(val);
            } else {
                self.null_count += 1;
            }
        }
    }

    /// Update statistics with a slice of Optional byte array values (unified API).
    /// This is the preferred method - accepts the same Optional type that Reader returns.
    pub fn updateOptional(self: *Self, values: []const Optional([]const u8)) !void {
        for (values) |v| {
            switch (v) {
                .value => |val| try self.updateValue(val),
                .null_value => self.null_count += 1,
            }
        }
    }

    /// Add to null count.
    pub fn addNulls(self: *Self, count: i64) void {
        self.null_count += count;
    }

    /// Build the Statistics struct.
    /// Ownership of min/max bytes is transferred to the returned Statistics.
    pub fn build(self: *Self) ?format.Statistics {
        if (!self.has_values) {
            if (self.null_count > 0) {
                return format.Statistics{
                    .null_count = self.null_count,
                };
            }
            return null;
        }

        // Transfer ownership - duplicate for deprecated fields
        const min_bytes = self.min.?;
        const max_bytes = self.max.?;

        // Duplicate for deprecated fields
        const min_deprecated = self.allocator.dupe(u8, min_bytes) catch return null;
        const max_deprecated = self.allocator.dupe(u8, max_bytes) catch {
            self.allocator.free(min_deprecated);
            return null;
        };

        // Clear our references since we're transferring ownership
        self.min = null;
        self.max = null;

        return format.Statistics{
            .min = min_deprecated,
            .max = max_deprecated,
            .null_count = self.null_count,
            .distinct_count = null,
            .min_value = min_bytes,
            .max_value = max_bytes,
        };
    }
};

/// Geospatial statistics builder for GEOMETRY and GEOGRAPHY columns.
/// Computes bounding box and geometry types from WKB values.
/// Note: Min/max statistics are NOT written for geospatial types (undefined sort order).
pub const GeospatialStatisticsBuilder = struct {
    bbox_builder: geo.BoundingBoxBuilder,
    null_count: i64 = 0,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .bbox_builder = geo.BoundingBoxBuilder.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.bbox_builder.deinit();
    }

    /// Update with a slice of WKB values.
    pub fn update(self: *Self, values: []const []const u8) void {
        for (values) |v| {
            self.bbox_builder.update(v);
        }
    }

    /// Update with a single WKB value.
    pub fn updateValue(self: *Self, value: []const u8) void {
        self.bbox_builder.update(value);
    }

    /// Update with a slice of nullable WKB values.
    pub fn updateNullable(self: *Self, values: []const ?[]const u8) void {
        for (values) |v| {
            if (v) |val| {
                self.bbox_builder.update(val);
            } else {
                self.null_count += 1;
            }
        }
    }

    /// Update with a slice of Optional WKB values (unified API).
    pub fn updateOptional(self: *Self, values: []const Optional([]const u8)) void {
        for (values) |v| {
            switch (v) {
                .value => |val| self.bbox_builder.update(val),
                .null_value => self.null_count += 1,
            }
        }
    }

    /// Add to null count.
    pub fn addNulls(self: *Self, count: i64) void {
        self.null_count += count;
    }

    /// Build GeospatialStatistics for ColumnMetaData.
    /// Returns null if no valid coordinates were accumulated.
    pub fn buildGeospatial(self: *Self) !?format.GeospatialStatistics {
        return try self.bbox_builder.buildStatistics(self.allocator);
    }

    /// Build regular Statistics (only null_count, no min/max for geospatial).
    pub fn buildStatistics(self: *const Self) ?format.Statistics {
        if (self.null_count > 0) {
            return format.Statistics{
                .null_count = self.null_count,
            };
        }
        return null;
    }

    /// Reset to initial state.
    pub fn reset(self: *Self) void {
        self.bbox_builder.reset();
        self.null_count = 0;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "StatisticsBuilder i32 basic" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i32){};
    builder.update(&[_]i32{ 5, 2, 8, 1, 9 });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    // Check min value (1 as little-endian i32)
    try std.testing.expectEqual(@as(usize, 4), stats.min_value.?.len);
    try std.testing.expectEqual(@as(i32, 1), std.mem.readInt(i32, stats.min_value.?[0..4], .little));

    // Check max value (9 as little-endian i32)
    try std.testing.expectEqual(@as(usize, 4), stats.max_value.?.len);
    try std.testing.expectEqual(@as(i32, 9), std.mem.readInt(i32, stats.max_value.?[0..4], .little));

    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);
}

test "StatisticsBuilder i64 with nulls" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i64){};
    builder.updateNullable(&[_]?i64{ 100, null, 50, null, 75, null });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqual(@as(i64, 50), std.mem.readInt(i64, stats.min_value.?[0..8], .little));
    try std.testing.expectEqual(@as(i64, 100), std.mem.readInt(i64, stats.max_value.?[0..8], .little));
    try std.testing.expectEqual(@as(i64, 3), stats.null_count.?);
}

test "StatisticsBuilder f64" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(f64){};
    builder.update(&[_]f64{ 3.14, 2.71, 1.41, 1.73 });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    const min_bits = std.mem.readInt(u64, stats.min_value.?[0..8], .little);
    const max_bits = std.mem.readInt(u64, stats.max_value.?[0..8], .little);
    const min_val: f64 = @bitCast(min_bits);
    const max_val: f64 = @bitCast(max_bits);

    try std.testing.expectApproxEqAbs(@as(f64, 1.41), min_val, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), max_val, 0.001);
}

test "StatisticsBuilder bool" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(bool){};
    builder.update(&[_]bool{ true, false, true });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqual(@as(u8, 0), stats.min_value.?[0]); // false
    try std.testing.expectEqual(@as(u8, 1), stats.max_value.?[0]); // true
}

test "StatisticsBuilder empty returns null" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i32){};
    const stats = try builder.build(allocator);

    try std.testing.expect(stats == null);
}

test "StatisticsBuilder all nulls" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i32){};
    builder.updateNullable(&[_]?i32{ null, null, null });

    const stats = (try builder.build(allocator)).?;

    try std.testing.expect(stats.min_value == null);
    try std.testing.expect(stats.max_value == null);
    try std.testing.expectEqual(@as(i64, 3), stats.null_count.?);
}

test "ByteArrayStatisticsBuilder basic" {
    const allocator = std.testing.allocator;

    var builder = ByteArrayStatisticsBuilder.init(allocator);
    defer builder.deinit();

    try builder.update(&[_][]const u8{ "banana", "apple", "cherry" });

    const stats = builder.build().?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqualStrings("apple", stats.min_value.?);
    try std.testing.expectEqualStrings("cherry", stats.max_value.?);
}

test "ByteArrayStatisticsBuilder with nulls" {
    const allocator = std.testing.allocator;

    var builder = ByteArrayStatisticsBuilder.init(allocator);
    defer builder.deinit();

    try builder.updateNullable(&[_]?[]const u8{ "zebra", null, "aardvark", null });

    const stats = builder.build().?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqualStrings("aardvark", stats.min_value.?);
    try std.testing.expectEqualStrings("zebra", stats.max_value.?);
    try std.testing.expectEqual(@as(i64, 2), stats.null_count.?);
}

// =============================================================================
// Tests for updateOptional (unified API)
// =============================================================================

test "StatisticsBuilder i64 with Optional" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i64){};
    builder.updateOptional(&[_]Optional(i64){
        .{ .value = 100 },
        .{ .null_value = {} },
        .{ .value = 50 },
        .{ .null_value = {} },
        .{ .value = 75 },
        .{ .null_value = {} },
    });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqual(@as(i64, 50), std.mem.readInt(i64, stats.min_value.?[0..8], .little));
    try std.testing.expectEqual(@as(i64, 100), std.mem.readInt(i64, stats.max_value.?[0..8], .little));
    try std.testing.expectEqual(@as(i64, 3), stats.null_count.?);
}

test "StatisticsBuilder i32 with Optional all values" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i32){};
    builder.updateOptional(&[_]Optional(i32){
        .{ .value = 5 },
        .{ .value = 2 },
        .{ .value = 8 },
        .{ .value = 1 },
        .{ .value = 9 },
    });

    const stats = (try builder.build(allocator)).?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqual(@as(i32, 1), std.mem.readInt(i32, stats.min_value.?[0..4], .little));
    try std.testing.expectEqual(@as(i32, 9), std.mem.readInt(i32, stats.max_value.?[0..4], .little));
    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);
}

test "StatisticsBuilder all nulls with Optional" {
    const allocator = std.testing.allocator;

    var builder = StatisticsBuilder(i32){};
    builder.updateOptional(&[_]Optional(i32){
        .{ .null_value = {} },
        .{ .null_value = {} },
        .{ .null_value = {} },
    });

    const stats = (try builder.build(allocator)).?;

    try std.testing.expect(stats.min_value == null);
    try std.testing.expect(stats.max_value == null);
    try std.testing.expectEqual(@as(i64, 3), stats.null_count.?);
}

test "ByteArrayStatisticsBuilder with Optional" {
    const allocator = std.testing.allocator;

    var builder = ByteArrayStatisticsBuilder.init(allocator);
    defer builder.deinit();

    try builder.updateOptional(&[_]Optional([]const u8){
        .{ .value = "zebra" },
        .{ .null_value = {} },
        .{ .value = "aardvark" },
        .{ .null_value = {} },
    });

    const stats = builder.build().?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqualStrings("aardvark", stats.min_value.?);
    try std.testing.expectEqualStrings("zebra", stats.max_value.?);
    try std.testing.expectEqual(@as(i64, 2), stats.null_count.?);
}

test "ByteArrayStatisticsBuilder with Optional all values" {
    const allocator = std.testing.allocator;

    var builder = ByteArrayStatisticsBuilder.init(allocator);
    defer builder.deinit();

    try builder.updateOptional(&[_]Optional([]const u8){
        .{ .value = "banana" },
        .{ .value = "apple" },
        .{ .value = "cherry" },
    });

    const stats = builder.build().?;
    defer {
        if (stats.min) |m| allocator.free(m);
        if (stats.max) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
    }

    try std.testing.expectEqualStrings("apple", stats.min_value.?);
    try std.testing.expectEqualStrings("cherry", stats.max_value.?);
    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);
}
