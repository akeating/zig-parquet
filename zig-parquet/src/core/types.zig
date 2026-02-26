//! Core types used across the parquet library
//!
//! This module contains shared types like Optional(T) that are used
//! by both the reader and writer, as well as logical type wrappers
//! for the RowWriter API.

const std = @import("std");
const safe = @import("safe.zig");
const parquet_format = @import("format.zig");

// ============================================================================
// Logical Type Wrappers for RowWriter
// ============================================================================
// These wrapper types allow users to specify Parquet logical types when using
// RowWriter(T). The RowWriter detects these types at comptime and emits the
// appropriate logical type annotations in the schema.

/// Re-export TimeUnit for convenience
pub const TimeUnit = parquet_format.TimeUnit;

/// Parameterized Timestamp type
/// Matches parquet-go: Timestamp(unit) and Arrow: TimestampType(unit)
/// Stored as INT64 with TIMESTAMP(unit, isAdjustedToUTC=true) logical type
pub fn Timestamp(comptime unit: TimeUnit) type {
    return struct {
        value: i64,

        const Self = @This();

        /// Parquet logical type marker (detected by RowWriter at comptime)
        pub const parquet_logical_type: parquet_format.LogicalType = .{
            .timestamp = .{
                .is_adjusted_to_utc = true,
                .unit = unit,
            },
        };

        /// The time unit for this timestamp type
        pub const time_unit = unit;

        /// Create a timestamp from the native unit value
        pub fn from(val: i64) Self {
            return .{ .value = val };
        }

        /// Create a timestamp from nanoseconds since Unix epoch
        pub fn fromNanos(nanos: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => nanos,
                .micros => @divTrunc(nanos, 1000),
                .millis => @divTrunc(nanos, 1_000_000),
            } };
        }

        /// Create a timestamp from microseconds since Unix epoch
        pub fn fromMicros(micros: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => micros * 1000,
                .micros => micros,
                .millis => @divTrunc(micros, 1000),
            } };
        }

        /// Create a timestamp from milliseconds since Unix epoch
        pub fn fromMillis(millis: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => millis * 1_000_000,
                .micros => millis * 1000,
                .millis => millis,
            } };
        }

        /// Create a timestamp from seconds since Unix epoch
        pub fn fromSeconds(secs: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => secs * 1_000_000_000,
                .micros => secs * 1_000_000,
                .millis => secs * 1000,
            } };
        }
    };
}

/// Backward compatibility alias for microsecond precision timestamps
pub const TimestampMicros = Timestamp(.micros);

/// Date (days since Unix epoch)
/// Stored as INT32 with DATE logical type
pub const Date = struct {
    days: i32,

    /// Parquet logical type marker
    pub const parquet_logical_type: parquet_format.LogicalType = .date;

    /// Create a date from days since Unix epoch (1970-01-01)
    pub fn fromDays(days: i32) Date {
        return .{ .days = days };
    }
};

/// UUID (128-bit identifier)
/// Stored as FIXED_LEN_BYTE_ARRAY(16) with UUID logical type
pub const Uuid = struct {
    bytes: [16]u8,

    /// Parquet logical type marker
    pub const parquet_logical_type: parquet_format.LogicalType = .uuid;

    /// Create a UUID from a 16-byte array
    pub fn fromBytes(bytes: [16]u8) Uuid {
        return .{ .bytes = bytes };
    }

    /// Parse a UUID from string format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    pub fn parse(str: []const u8) !Uuid {
        if (str.len != 36) return error.InvalidUuidFormat;

        var bytes: [16]u8 = undefined;
        var byte_idx: usize = 0;
        var i: usize = 0;

        while (i < str.len) : (i += 1) {
            if (str[i] == '-') continue;

            if (i + 1 >= str.len) return error.InvalidUuidFormat;

            const high = try hexDigit(str[i]);
            const low = try hexDigit(str[i + 1]);
            bytes[byte_idx] = (high << 4) | low;
            byte_idx += 1;
            i += 1; // Skip the second hex digit
        }

        if (byte_idx != 16) return error.InvalidUuidFormat;
        return .{ .bytes = bytes };
    }

    fn hexDigit(c: u8) !u4 {
        return switch (c) {
            '0'...'9' => try safe.castTo(u4, c - '0'),
            'a'...'f' => try safe.castTo(u4, c - 'a' + 10),
            'A'...'F' => try safe.castTo(u4, c - 'A' + 10),
            else => error.InvalidHexDigit,
        };
    }
};

/// Parameterized Time type
/// Matches parquet-go: Time(unit) and Arrow: TimeType(unit)
/// Stored as INT64 (micros/nanos) or INT32 (millis) with TIME(unit, isAdjustedToUTC=true) logical type
pub fn Time(comptime unit: TimeUnit) type {
    return struct {
        value: i64,

        const Self = @This();

        /// Parquet logical type marker
        pub const parquet_logical_type: parquet_format.LogicalType = .{
            .time = .{
                .is_adjusted_to_utc = true,
                .unit = unit,
            },
        };

        /// The time unit for this time type
        pub const time_unit = unit;

        /// Create a time from the native unit value
        pub fn from(val: i64) Self {
            return .{ .value = val };
        }

        /// Create a time from nanoseconds since midnight
        pub fn fromNanos(nanos: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => nanos,
                .micros => @divTrunc(nanos, 1000),
                .millis => @divTrunc(nanos, 1_000_000),
            } };
        }

        /// Create a time from microseconds since midnight
        pub fn fromMicros(micros: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => micros * 1000,
                .micros => micros,
                .millis => @divTrunc(micros, 1000),
            } };
        }

        /// Create a time from milliseconds since midnight
        pub fn fromMillis(millis: i64) Self {
            return .{ .value = switch (unit) {
                .nanos => millis * 1_000_000,
                .micros => millis * 1000,
                .millis => millis,
            } };
        }

        /// Create a time from hours, minutes, seconds, and sub-second component
        pub fn fromHms(hours: u8, minutes: u8, seconds: u8, subsec: u32) Self {
            const base: i64 = @as(i64, hours) * 3_600 +
                @as(i64, minutes) * 60 +
                @as(i64, seconds);
            return .{ .value = switch (unit) {
                .nanos => base * 1_000_000_000 + @as(i64, subsec),
                .micros => base * 1_000_000 + @as(i64, subsec),
                .millis => base * 1_000 + @as(i64, subsec),
            } };
        }
    };
}

/// Backward compatibility alias for microsecond precision times
pub const TimeMicros = Time(.micros);

/// INT96 timestamp (legacy format)
/// 12 bytes: 8 bytes nanoseconds within day (little-endian) + 4 bytes Julian day (little-endian)
/// This format is deprecated but still used by Apache Impala, Spark 2.x, and many legacy files.
pub const Int96 = struct {
    bytes: [12]u8,

    /// Julian day number at Unix epoch (1970-01-01)
    pub const julian_epoch: i64 = 2440588;
    /// Nanoseconds per day
    pub const nanos_per_day: i64 = 86_400_000_000_000;

    /// Convert to nanoseconds since Unix epoch
    /// Uses saturating arithmetic to avoid overflow panics on malformed data.
    pub fn toNanos(self: Int96) i64 {
        const max_i64: i64 = std.math.maxInt(i64);
        const min_i64: i64 = std.math.minInt(i64);

        // First 8 bytes: nanoseconds within the day (little-endian uint64)
        const day_nanos: u64 = std.mem.readInt(u64, self.bytes[0..8], .little);
        // Last 4 bytes: Julian day number (little-endian int32)
        const julian_day: i32 = std.mem.readInt(i32, self.bytes[8..12], .little);

        // Convert Julian day to days since Unix epoch
        const days_since_epoch: i64 = @as(i64, julian_day) - julian_epoch;

        // Use saturating arithmetic to handle overflow on malformed data
        const days_nanos = std.math.mul(i64, days_since_epoch, nanos_per_day) catch {
            return if (days_since_epoch > 0) max_i64 else min_i64;
        };

        // Convert day_nanos to i64, saturating if too large
        const day_nanos_i64: i64 = if (day_nanos > @as(u64, max_i64))
            max_i64
        else
            // This is guaranteed safe because of the check above
            safe.castTo(i64, day_nanos) catch unreachable;

        // Combine with saturating add
        return std.math.add(i64, days_nanos, day_nanos_i64) catch max_i64;
    }

    /// Create from nanoseconds since Unix epoch
    pub fn fromNanos(nanos: i64) Int96 {
        var result: Int96 = undefined;

        // Handle negative timestamps (before Unix epoch)
        var days: i64 = undefined;
        var day_nanos: u64 = undefined;

        if (nanos >= 0) {
            days = @divFloor(nanos, nanos_per_day);
            // The modulo ensures it easily fits within u64 bounds
            day_nanos = safe.castTo(u64, @mod(nanos, nanos_per_day)) catch unreachable;
        } else {
            // For negative values, we need floor division
            days = @divFloor(nanos, nanos_per_day);
            const remainder = @mod(nanos, nanos_per_day);
            day_nanos = safe.castTo(u64, remainder) catch unreachable; // @mod result is non-negative and < nanos_per_day
        }

        // Convert days since epoch to Julian day, saturating for out-of-range timestamps
        const julian_day: i32 = std.math.cast(i32, days + julian_epoch) orelse
            if (days >= 0) std.math.maxInt(i32) else std.math.minInt(i32);

        // Write little-endian
        std.mem.writeInt(u64, result.bytes[0..8], day_nanos, .little);
        std.mem.writeInt(i32, result.bytes[8..12], julian_day, .little);

        return result;
    }

    /// Create from raw bytes (for reading from Parquet)
    pub fn fromBytes(bytes: [12]u8) Int96 {
        return .{ .bytes = bytes };
    }
};

/// TimestampInt96 wrapper type for user structs
/// Use this when you need to write timestamps in the legacy INT96 format
/// for compatibility with older systems like Impala or Spark 2.x.
///
/// ## Example
/// ```zig
/// const Record = struct {
///     event_time: types.TimestampInt96,
/// };
///
/// var writer = try RowWriter(Record).init(allocator, file, .{});
/// try writer.writeRow(.{
///     .event_time = types.TimestampInt96.fromNanos(timestamp_nanos),
/// });
/// ```
pub const TimestampInt96 = struct {
    value: i64, // nanoseconds since epoch

    /// Physical type marker (detected by RowWriter at comptime)
    pub const parquet_physical_type = parquet_format.PhysicalType.int96;
    /// INT96 has no logical type annotation
    pub const parquet_logical_type: ?parquet_format.LogicalType = null;

    /// Create from nanoseconds since Unix epoch
    pub fn fromNanos(nanos: i64) TimestampInt96 {
        return .{ .value = nanos };
    }

    /// Create from microseconds since Unix epoch
    pub fn fromMicros(micros: i64) TimestampInt96 {
        return .{ .value = micros * 1000 };
    }

    /// Create from milliseconds since Unix epoch
    pub fn fromMillis(millis: i64) TimestampInt96 {
        return .{ .value = millis * 1_000_000 };
    }

    /// Create from seconds since Unix epoch
    pub fn fromSeconds(secs: i64) TimestampInt96 {
        return .{ .value = secs * 1_000_000_000 };
    }

    /// Convert to Int96 format for encoding
    pub fn toInt96(self: TimestampInt96) Int96 {
        return Int96.fromNanos(self.value);
    }
};

/// Runtime Decimal type with arbitrary precision up to 38 digits.
/// Used by RowReader when precision/scale are discovered from the file schema at runtime.
/// For RowWriter, use the parameterized `Decimal(precision, scale)` instead.
/// The bytes are big-endian two's complement representation.
pub const DecimalValue = struct {
    /// Raw bytes (big-endian two's complement), up to 16 bytes for 38 digits
    bytes: [16]u8,
    /// Number of valid bytes in the bytes array
    byte_len: u8,
    /// Total number of digits
    precision: u8,
    /// Number of digits after decimal point
    scale: u8,

    /// Parquet logical type marker (detected by RowReader at comptime)
    /// This prevents DecimalValue from being treated as a nested struct
    pub const parquet_logical_type: parquet_format.LogicalType = .{
        .decimal = .{
            .scale = 0,
            .precision = 0,
        },
    };

    /// Create a DecimalValue from raw bytes (big-endian two's complement)
    pub fn fromBytes(raw_bytes: []const u8, precision: u8, scale: u8) error{InvalidDecimalLength}!DecimalValue {
        if (raw_bytes.len > 16) return error.InvalidDecimalLength;

        var result = DecimalValue{
            .bytes = [_]u8{0} ** 16,
            .byte_len = safe.castTo(u8, raw_bytes.len) catch unreachable, // checked <= 16 above
            .precision = precision,
            .scale = scale,
        };

        // Copy bytes to the end of the array (right-aligned)
        const start = 16 - raw_bytes.len;
        // Sign-extend if negative (first bit is 1)
        if (raw_bytes.len > 0 and (raw_bytes[0] & 0x80) != 0) {
            @memset(&result.bytes, 0xFF);
        }
        @memcpy(result.bytes[start..][0..raw_bytes.len], raw_bytes);

        return result;
    }

    /// Convert to i128 (unscaled value)
    pub fn toI128(self: DecimalValue) i128 {
        var result: i128 = 0;
        for (self.bytes) |b| {
            result = (result << 8) | @as(i128, b);
        }
        return result;
    }

    /// Convert to f64 (may lose precision for large values)
    pub fn toF64(self: DecimalValue) f64 {
        const unscaled = self.toI128();
        const divisor = std.math.pow(f64, 10.0, @floatFromInt(self.scale));
        return @as(f64, @floatFromInt(unscaled)) / divisor;
    }

    /// Format decimal as a string (for debugging and std.fmt)
    pub fn format(
        self: DecimalValue,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("{d}", .{self.toF64()});
    }
};

/// Minimum FLBA byte length for a given decimal precision (Parquet spec).
/// Formula: ceil((precision * log2(10) + 1) / 8)
pub fn decimalByteLength(comptime precision: comptime_int) comptime_int {
    // Lookup table for precisions 1-38
    const table = [38]comptime_int{
        1, 1, 2, 2, 3, 3, 4, 4, 4, // P1-P9
        5, 5, 6, 6, 6, 7, 7, 8, 8, // P10-P18
        9, 9, 9, 10, 10, 11, 11, 11, 12, 12, // P19-P28
        13, 13, 13, 14, 14, 15, 15, 16, 16, 16, // P29-P38
    };
    if (precision < 1 or precision > 38) @compileError("Decimal precision must be 1-38");
    return table[precision - 1];
}

/// Parameterized Decimal type for RowWriter.
/// Precision and scale are baked in at comptime for schema inference.
/// Stored as FIXED_LEN_BYTE_ARRAY with DECIMAL logical type.
///
/// ## Example
/// ```zig
/// const Record = struct {
///     price: types.Decimal(9, 2),
///     amount: ?types.Decimal(18, 4),
/// };
///
/// var writer = try RowWriter(Record).init(allocator, file, .{});
/// try writer.writeRow(.{
///     .price = types.Decimal(9, 2).fromUnscaled(12345), // 123.45
///     .amount = types.Decimal(18, 4).fromF64(99.99),
/// });
/// ```
pub fn Decimal(comptime precision: comptime_int, comptime scale: comptime_int) type {
    if (precision < 1 or precision > 38) @compileError("Decimal precision must be 1-38");
    if (scale < 0 or scale > precision) @compileError("Decimal scale must be 0..precision");
    const byte_len = decimalByteLength(precision);

    return struct {
        bytes: [byte_len]u8,

        const Self = @This();

        pub const parquet_logical_type: parquet_format.LogicalType = .{
            .decimal = .{ .precision = precision, .scale = scale },
        };
        pub const decimal_precision: comptime_int = precision;
        pub const decimal_scale: comptime_int = scale;
        pub const decimal_byte_len: comptime_int = byte_len;

        /// Create from an unscaled i128 value.
        /// E.g. Decimal(9,2).fromUnscaled(12345) represents 123.45
        pub fn fromUnscaled(value: i128) Self {
            var result: Self = .{ .bytes = undefined };
            var v = value;
            // Fill bytes in big-endian order
            comptime var i: usize = byte_len;
            inline while (i > 0) {
                i -= 1;
                result.bytes[i] = @truncate(@as(u128, @bitCast(v)));
                v >>= 8;
            }
            return result;
        }

        /// Create from an f64 value (applies scale).
        /// E.g. Decimal(9,2).fromF64(123.45) stores unscaled 12345
        pub fn fromF64(value: f64) Self {
            const multiplier = comptime std.math.pow(f64, 10.0, @floatFromInt(scale));
            const unscaled: i128 = @intFromFloat(@round(value * multiplier));
            return fromUnscaled(unscaled);
        }

        /// Convert to i128 (unscaled value)
        pub fn toI128(self: Self) i128 {
            var result: i128 = 0;
            // Sign-extend from the first byte
            if (self.bytes[0] & 0x80 != 0) {
                result = -1; // all 1s for sign extension
            }
            inline for (0..byte_len) |i| {
                result = (result << 8) | @as(i128, self.bytes[i]);
            }
            return result;
        }

        /// Convert to f64 (may lose precision for large values)
        pub fn toF64(self: Self) f64 {
            const unscaled = self.toI128();
            const divisor = comptime std.math.pow(f64, 10.0, @floatFromInt(scale));
            return @as(f64, @floatFromInt(unscaled)) / divisor;
        }

        /// Convert to the runtime DecimalValue type
        pub fn toDecimalValue(self: Self) DecimalValue {
            var dv = DecimalValue{
                .bytes = [_]u8{0} ** 16,
                .byte_len = byte_len,
                .precision = precision,
                .scale = scale,
            };
            // Right-align into the 16-byte buffer
            const start = 16 - byte_len;
            // Sign-extend if negative
            if (self.bytes[0] & 0x80 != 0) {
                @memset(&dv.bytes, 0xFF);
            }
            @memcpy(dv.bytes[start..][0..byte_len], &self.bytes);
            return dv;
        }

        /// Create from raw bytes (big-endian two's complement).
        /// Bytes are right-aligned into the fixed-size buffer.
        pub fn fromBytes(raw_bytes: []const u8) error{InvalidDecimalLength}!Self {
            if (raw_bytes.len > byte_len) return error.InvalidDecimalLength;
            var result: Self = .{ .bytes = undefined };
            // Sign-extend if negative
            if (raw_bytes.len > 0 and (raw_bytes[0] & 0x80) != 0) {
                @memset(&result.bytes, 0xFF);
            } else {
                @memset(&result.bytes, 0x00);
            }
            const start = byte_len - raw_bytes.len;
            @memcpy(result.bytes[start..][0..raw_bytes.len], raw_bytes);
            return result;
        }

        /// Format decimal as a string (for debugging and std.fmt)
        pub fn format(
            self: Self,
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;
            try writer.print("{d}", .{self.toF64()});
        }
    };
}

/// INTERVAL type (legacy Parquet type)
/// Stored as FIXED_LEN_BYTE_ARRAY(12): months(u32) + days(u32) + millis(u32)
/// All components are little-endian unsigned 32-bit integers.
///
/// Note: INTERVAL uses ConvertedType (21), not LogicalType (reserved but undefined).
/// Statistics should NOT be written for INTERVAL (sort order is undefined per spec).
///
/// ## Example
/// ```zig
/// const Record = struct {
///     duration: types.Interval,
/// };
///
/// var writer = try RowWriter(Record).init(allocator, file, .{});
/// try writer.writeRow(.{
///     .duration = types.Interval.fromComponents(1, 15, 3600000), // 1 month, 15 days, 1 hour
/// });
/// ```
pub const Interval = struct {
    months: u32,
    days: u32,
    millis: u32,

    /// Marker for RowWriter detection (uses converted_type, not logical_type)
    /// ConvertedType.INTERVAL = 21
    pub const parquet_converted_type: i32 = 21;

    /// Create an interval from individual components
    pub fn fromComponents(months: u32, days: u32, millis: u32) Interval {
        return .{ .months = months, .days = days, .millis = millis };
    }

    /// Create an interval from a 12-byte array (little-endian)
    pub fn fromBytes(bytes: [12]u8) Interval {
        return .{
            .months = std.mem.readInt(u32, bytes[0..4], .little),
            .days = std.mem.readInt(u32, bytes[4..8], .little),
            .millis = std.mem.readInt(u32, bytes[8..12], .little),
        };
    }

    /// Convert to a 12-byte array (little-endian)
    pub fn toBytes(self: Interval) [12]u8 {
        var result: [12]u8 = undefined;
        std.mem.writeInt(u32, result[0..4], self.months, .little);
        std.mem.writeInt(u32, result[4..8], self.days, .little);
        std.mem.writeInt(u32, result[8..12], self.millis, .little);
        return result;
    }

    /// Create an interval representing only months
    pub fn fromMonths(months: u32) Interval {
        return .{ .months = months, .days = 0, .millis = 0 };
    }

    /// Create an interval representing only days
    pub fn fromDays(days: u32) Interval {
        return .{ .months = 0, .days = days, .millis = 0 };
    }

    /// Create an interval representing only milliseconds
    pub fn fromMillis(millis: u32) Interval {
        return .{ .months = 0, .days = 0, .millis = millis };
    }

    /// Create an interval from hours (converted to milliseconds)
    pub fn fromHours(hours: u32) Interval {
        return .{ .months = 0, .days = 0, .millis = hours * 3_600_000 };
    }

    /// Create an interval from minutes (converted to milliseconds)
    pub fn fromMinutes(minutes: u32) Interval {
        return .{ .months = 0, .days = 0, .millis = minutes * 60_000 };
    }

    /// Create an interval from seconds (converted to milliseconds)
    pub fn fromSeconds(seconds: u32) Interval {
        return .{ .months = 0, .days = 0, .millis = seconds * 1000 };
    }

    /// Get total milliseconds (excluding months and days, as they have variable length)
    pub fn totalMillis(self: Interval) u32 {
        return self.millis;
    }

    /// Format interval for display
    pub fn format(
        self: Interval,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("P", .{});
        if (self.months > 0) {
            try writer.print("{}M", .{self.months});
        }
        if (self.days > 0) {
            try writer.print("{}D", .{self.days});
        }
        if (self.millis > 0) {
            const hours = self.millis / 3_600_000;
            const remaining_after_hours = self.millis % 3_600_000;
            const minutes = remaining_after_hours / 60_000;
            const remaining_after_minutes = remaining_after_hours % 60_000;
            const seconds = remaining_after_minutes / 1000;
            const ms = remaining_after_minutes % 1000;

            try writer.print("T", .{});
            if (hours > 0) try writer.print("{}H", .{hours});
            if (minutes > 0) try writer.print("{}M", .{minutes});
            if (seconds > 0 or ms > 0) {
                if (ms > 0) {
                    try writer.print("{}.{:0>3}S", .{ seconds, ms });
                } else {
                    try writer.print("{}S", .{seconds});
                }
            }
        }
        // Handle zero interval
        if (self.months == 0 and self.days == 0 and self.millis == 0) {
            try writer.print("0D", .{});
        }
    }
};

/// Map entry type for RowWriter/RowReader map fields.
/// Keys are always required; values are always optional (standard Parquet convention).
///
/// ## Example
/// ```zig
/// const Record = struct {
///     attributes: ?[]const MapEntry([]const u8, i32),
/// };
///
/// var writer = try RowWriter(Record).init(allocator, file, .{});
/// try writer.writeRow(.{
///     .attributes = &[_]MapEntry([]const u8, i32){
///         .{ .key = "color", .value = 42 },
///         .{ .key = "size", .value = null },
///     },
/// });
/// ```
pub fn MapEntry(comptime K: type, comptime V: type) type {
    return struct {
        pub const is_parquet_map_entry = true;
        pub const KeyType = K;
        pub const ValueType = V;

        key: K,
        value: ?V,
    };
}

/// A value that may be null (for nullable Parquet columns)
///
/// Parquet columns with OPTIONAL repetition type can have null values.
/// This type wraps the actual value to represent nullability.
///
/// ## Example
/// ```zig
/// const values = try reader.readColumn(0, i32);
/// for (values) |v| {
///     switch (v) {
///         .value => |n| std.debug.print("{}\n", .{n}),
///         .null_value => std.debug.print("null\n", .{}),
///     }
/// }
/// ```
pub fn Optional(comptime T: type) type {
    return union(enum) {
        value: T,
        null_value: void,

        const Self = @This();

        /// Check if this value is null
        pub fn isNull(self: Self) bool {
            return self == .null_value;
        }

        /// Get the value or return a default
        pub fn valueOr(self: Self, default: T) T {
            return switch (self) {
                .value => |v| v,
                .null_value => default,
            };
        }

        /// Create an Optional from a value or Zig optional.
        /// Accepts both T and ?T (including comptime values that coerce to T).
        pub fn from(val: anytype) Self {
            const ValType = @TypeOf(val);
            const val_info = @typeInfo(ValType);
            if (val_info == .optional) {
                // It's an optional type - unwrap it
                return if (val) |v| .{ .value = v } else .null_value;
            } else {
                // Not optional - treat as direct value (compiler handles coercion)
                return .{ .value = val };
            }
        }

        /// Format for printing
        pub fn format(
            self: Self,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            switch (self) {
                .value => |v| {
                    if (T == []const u8) {
                        try writer.print("\"{s}\"", .{v});
                    } else if (T == f32 or T == f64) {
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
                    } else {
                        try writer.print("{any}", .{v});
                    }
                },
                .null_value => try writer.writeAll("null"),
            }
        }
    };
}

/// Categorized error sets — see core/errors.zig for definitions.
pub const errors = @import("errors.zig");

/// Errors that can occur when reading Parquet files.
/// Composed from transport, format, decoding, compression, checksum, and resource categories.
pub const ReaderError = errors.ReaderError;

/// Errors that can occur when writing Parquet files.
/// Composed from resource, caller, compression categories plus write-specific errors.
pub const WriterError = errors.WriterError;

// Tests
test "Optional isNull" {
    const opt_i32 = Optional(i32){ .value = 42 };
    const null_i32 = Optional(i32){ .null_value = {} };

    try std.testing.expect(!opt_i32.isNull());
    try std.testing.expect(null_i32.isNull());
}

test "Optional valueOr" {
    const opt_i32 = Optional(i32){ .value = 42 };
    const null_i32 = Optional(i32){ .null_value = {} };

    try std.testing.expectEqual(@as(i32, 42), opt_i32.valueOr(0));
    try std.testing.expectEqual(@as(i32, 0), null_i32.valueOr(0));
}

test "Optional from value" {
    const opt = Optional(i32).from(42);
    try std.testing.expectEqual(false, opt.isNull());
    try std.testing.expectEqual(@as(i32, 42), opt.valueOr(0));
}

test "Optional from nullable" {
    const val: ?i32 = 42;
    const opt_val = Optional(i32).from(val);
    try std.testing.expectEqual(false, opt_val.isNull());

    const null_val: ?i32 = null;
    const opt_null = Optional(i32).from(null_val);
    try std.testing.expectEqual(true, opt_null.isNull());
}

test "Int96 round-trip Unix epoch" {
    // Unix epoch should be Julian day 2440588 with 0 nanoseconds
    const int96 = Int96.fromNanos(0);
    const nanos = int96.toNanos();
    try std.testing.expectEqual(@as(i64, 0), nanos);
}

test "Int96 round-trip positive timestamp" {
    // 2024-01-15 12:30:45.123456789 UTC
    // This is a known timestamp value
    const original_nanos: i64 = 1705321845_123456789;
    const int96 = Int96.fromNanos(original_nanos);
    const recovered = int96.toNanos();
    try std.testing.expectEqual(original_nanos, recovered);
}

test "Int96 round-trip negative timestamp" {
    // Before Unix epoch: 1969-12-31 23:59:59.000000001
    const original_nanos: i64 = -999999999; // 1 nanosecond before epoch
    const int96 = Int96.fromNanos(original_nanos);
    const recovered = int96.toNanos();
    try std.testing.expectEqual(original_nanos, recovered);
}

test "Int96 fromBytes" {
    // Create known bytes for epoch
    var bytes: [12]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], 0, .little); // 0 nanoseconds
    std.mem.writeInt(i32, bytes[8..12], 2440588, .little); // Julian epoch
    const int96 = Int96.fromBytes(bytes);
    try std.testing.expectEqual(@as(i64, 0), int96.toNanos());
}

test "TimestampInt96 convenience constructors" {
    const ts_secs = TimestampInt96.fromSeconds(1);
    try std.testing.expectEqual(@as(i64, 1_000_000_000), ts_secs.value);

    const ts_millis = TimestampInt96.fromMillis(1);
    try std.testing.expectEqual(@as(i64, 1_000_000), ts_millis.value);

    const ts_micros = TimestampInt96.fromMicros(1);
    try std.testing.expectEqual(@as(i64, 1_000), ts_micros.value);

    const ts_nanos = TimestampInt96.fromNanos(1);
    try std.testing.expectEqual(@as(i64, 1), ts_nanos.value);
}

test "TimestampInt96 toInt96 conversion" {
    const ts = TimestampInt96.fromNanos(86_400_000_000_000); // 1 day after epoch
    const int96 = ts.toInt96();
    const recovered = int96.toNanos();
    try std.testing.expectEqual(ts.value, recovered);
}

test "Interval fromComponents" {
    const interval = Interval.fromComponents(12, 30, 3_600_000);
    try std.testing.expectEqual(@as(u32, 12), interval.months);
    try std.testing.expectEqual(@as(u32, 30), interval.days);
    try std.testing.expectEqual(@as(u32, 3_600_000), interval.millis);
}

test "Interval fromBytes/toBytes round-trip" {
    const original = Interval.fromComponents(5, 10, 123456);
    const bytes = original.toBytes();
    const recovered = Interval.fromBytes(bytes);
    try std.testing.expectEqual(original.months, recovered.months);
    try std.testing.expectEqual(original.days, recovered.days);
    try std.testing.expectEqual(original.millis, recovered.millis);
}

test "Interval fromBytes little-endian" {
    // Manually construct bytes for known values
    var bytes: [12]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], 24, .little); // 24 months
    std.mem.writeInt(u32, bytes[4..8], 365, .little); // 365 days
    std.mem.writeInt(u32, bytes[8..12], 86_400_000, .little); // 1 day in millis

    const interval = Interval.fromBytes(bytes);
    try std.testing.expectEqual(@as(u32, 24), interval.months);
    try std.testing.expectEqual(@as(u32, 365), interval.days);
    try std.testing.expectEqual(@as(u32, 86_400_000), interval.millis);
}

test "Interval convenience constructors" {
    const from_months = Interval.fromMonths(6);
    try std.testing.expectEqual(@as(u32, 6), from_months.months);
    try std.testing.expectEqual(@as(u32, 0), from_months.days);
    try std.testing.expectEqual(@as(u32, 0), from_months.millis);

    const from_days = Interval.fromDays(15);
    try std.testing.expectEqual(@as(u32, 0), from_days.months);
    try std.testing.expectEqual(@as(u32, 15), from_days.days);
    try std.testing.expectEqual(@as(u32, 0), from_days.millis);

    const from_hours = Interval.fromHours(2);
    try std.testing.expectEqual(@as(u32, 0), from_hours.months);
    try std.testing.expectEqual(@as(u32, 0), from_hours.days);
    try std.testing.expectEqual(@as(u32, 7_200_000), from_hours.millis);

    const from_minutes = Interval.fromMinutes(30);
    try std.testing.expectEqual(@as(u32, 1_800_000), from_minutes.millis);

    const from_seconds = Interval.fromSeconds(90);
    try std.testing.expectEqual(@as(u32, 90_000), from_seconds.millis);
}

test "Interval parquet_converted_type marker" {
    // Verify the marker is correctly set for RowWriter detection
    try std.testing.expectEqual(@as(i32, 21), Interval.parquet_converted_type);
}

test "DecimalValue.fromBytes valid inputs" {
    // Single byte positive
    const d1 = try DecimalValue.fromBytes(&[_]u8{0x7B}, 3, 0);
    try std.testing.expectEqual(@as(u8, 1), d1.byte_len);
    try std.testing.expectEqual(@as(i128, 123), d1.toI128());

    // Two bytes negative (two's complement)
    const d2 = try DecimalValue.fromBytes(&[_]u8{ 0xFF, 0x85 }, 5, 2);
    try std.testing.expectEqual(@as(i128, -123), d2.toI128());

    // Exactly 16 bytes (maximum)
    var max_bytes: [16]u8 = undefined;
    @memset(&max_bytes, 0);
    max_bytes[15] = 1;
    const d3 = try DecimalValue.fromBytes(&max_bytes, 38, 0);
    try std.testing.expectEqual(@as(u8, 16), d3.byte_len);
    try std.testing.expectEqual(@as(i128, 1), d3.toI128());

    // Zero bytes
    const d4 = try DecimalValue.fromBytes(&[_]u8{}, 1, 0);
    try std.testing.expectEqual(@as(u8, 0), d4.byte_len);
    try std.testing.expectEqual(@as(i128, 0), d4.toI128());
}

test "DecimalValue.fromBytes rejects oversized input" {
    var too_long: [17]u8 = undefined;
    @memset(&too_long, 0);
    try std.testing.expectError(error.InvalidDecimalLength, DecimalValue.fromBytes(&too_long, 38, 0));

    var way_too_long: [32]u8 = undefined;
    @memset(&way_too_long, 0);
    try std.testing.expectError(error.InvalidDecimalLength, DecimalValue.fromBytes(&way_too_long, 38, 0));
}

test "Decimal parameterized type basic operations" {
    const D9_2 = Decimal(9, 2);

    // fromUnscaled
    const d1 = D9_2.fromUnscaled(12345);
    try std.testing.expectEqual(@as(i128, 12345), d1.toI128());
    try std.testing.expectApproxEqRel(@as(f64, 123.45), d1.toF64(), 0.001);

    // Negative
    const d2 = D9_2.fromUnscaled(-99999);
    try std.testing.expectEqual(@as(i128, -99999), d2.toI128());
    try std.testing.expectApproxEqRel(@as(f64, -999.99), d2.toF64(), 0.001);

    // fromF64
    const d3 = D9_2.fromF64(123.45);
    try std.testing.expectEqual(@as(i128, 12345), d3.toI128());

    // Zero
    const d4 = D9_2.fromUnscaled(0);
    try std.testing.expectEqual(@as(i128, 0), d4.toI128());

    // toDecimalValue conversion
    const dv = d1.toDecimalValue();
    try std.testing.expectEqual(@as(u8, 9), dv.precision);
    try std.testing.expectEqual(@as(u8, 2), dv.scale);
    try std.testing.expectEqual(@as(i128, 12345), dv.toI128());
}

test "Decimal parameterized type high precision" {
    const D38_10 = Decimal(38, 10);

    const d1 = D38_10.fromUnscaled(1);
    try std.testing.expectEqual(@as(i128, 1), d1.toI128());

    // fromBytes
    const d2 = try D38_10.fromBytes(&[_]u8{0x01});
    try std.testing.expectEqual(@as(i128, 1), d2.toI128());

    // fromBytes rejects oversized
    var too_long: [17]u8 = undefined;
    @memset(&too_long, 0);
    try std.testing.expectError(error.InvalidDecimalLength, D38_10.fromBytes(&too_long));
}

test "decimalByteLength values" {
    try std.testing.expectEqual(@as(comptime_int, 4), decimalByteLength(9));
    try std.testing.expectEqual(@as(comptime_int, 8), decimalByteLength(18));
    try std.testing.expectEqual(@as(comptime_int, 16), decimalByteLength(38));
    try std.testing.expectEqual(@as(comptime_int, 1), decimalByteLength(1));
}

test "Int96.fromNanos extreme values saturate" {
    // maxInt(i64) nanos ~ year 2262, well beyond i32 Julian day range?
    // Actually max i64 nanos / nanos_per_day ~ 106 billion days, way past i32 max
    const max_int96 = Int96.fromNanos(std.math.maxInt(i64));
    const max_nanos = max_int96.toNanos();
    // Should not panic — the Julian day saturates to maxInt(i32)
    try std.testing.expect(max_nanos != 0);

    const min_int96 = Int96.fromNanos(std.math.minInt(i64));
    const min_nanos = min_int96.toNanos();
    try std.testing.expect(min_nanos != 0);
}
