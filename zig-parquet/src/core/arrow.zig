//! Arrow C Data Interface and Zig-friendly Arrow types
//!
//! This module provides:
//! - Arrow C Data Interface structs (ArrowSchema, ArrowArray) for zero-copy interop
//! - Zig-friendly ArrowColumn(T) wrapper for internal use
//! - Type mapping between Parquet and Arrow format strings
//!
//! See: https://arrow.apache.org/docs/format/CDataInterface.html

const std = @import("std");
const safe = @import("safe.zig");

// =============================================================================
// Arrow C Data Interface (ABI-compatible structs)
// =============================================================================

/// Arrow schema - describes the type of an array
/// See: https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure
pub const ArrowSchema = extern struct {
    /// Format string describing the data type
    format: [*:0]const u8,

    /// Optional name of the field
    name: ?[*:0]const u8,

    /// Optional metadata as Arrow-formatted key-value pairs
    metadata: ?[*:0]const u8,

    /// Flags (ARROW_FLAG_*)
    flags: i64,

    /// Number of children (for nested types)
    n_children: i64,

    /// Array of child schemas
    children: ?[*]*ArrowSchema,

    /// Dictionary schema (for dictionary-encoded arrays)
    dictionary: ?*ArrowSchema,

    /// Release callback - called when the consumer is done with the schema
    release: ?*const fn (*ArrowSchema) callconv(.c) void,

    /// Producer's private data
    private_data: ?*anyopaque,

    /// Check if this schema has been released
    pub fn isReleased(self: *const ArrowSchema) bool {
        return self.release == null;
    }

    /// Release the schema (call the release callback)
    pub fn doRelease(self: *ArrowSchema) void {
        if (self.release) |rel| {
            rel(self);
        }
    }
};

/// Arrow array - contains the actual data
/// See: https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure
pub const ArrowArray = extern struct {
    /// Number of elements in the array
    length: i64,

    /// Number of null values
    null_count: i64,

    /// Offset into buffers (for slicing)
    offset: i64,

    /// Number of buffers
    n_buffers: i64,

    /// Number of children (for nested types)
    n_children: i64,

    /// Array of buffer pointers
    /// Buffer 0 is always validity bitmap (or null if no nulls)
    buffers: [*]?*anyopaque,

    /// Array of child arrays
    children: ?[*]*ArrowArray,

    /// Dictionary values (for dictionary-encoded arrays)
    dictionary: ?*ArrowArray,

    /// Release callback - called when the consumer is done with the array
    release: ?*const fn (*ArrowArray) callconv(.c) void,

    /// Producer's private data
    private_data: ?*anyopaque,

    /// Check if this array has been released
    pub fn isReleased(self: *const ArrowArray) bool {
        return self.release == null;
    }

    /// Release the array (call the release callback)
    pub fn doRelease(self: *ArrowArray) void {
        if (self.release) |rel| {
            rel(self);
        }
    }
};

/// Arrow C Stream Interface -- an iterator that yields batches.
/// See: https://arrow.apache.org/docs/format/CStreamInterface.html
pub const ArrowArrayStream = extern struct {
    get_schema: ?*const fn (*ArrowArrayStream, *ArrowSchema) callconv(.c) c_int,
    get_next: ?*const fn (*ArrowArrayStream, *ArrowArray) callconv(.c) c_int,
    get_last_error: ?*const fn (*ArrowArrayStream) callconv(.c) ?[*:0]const u8,
    release: ?*const fn (*ArrowArrayStream) callconv(.c) void,
    private_data: ?*anyopaque,
};

// Arrow flags
pub const ARROW_FLAG_DICTIONARY_ORDERED: i64 = 1;
pub const ARROW_FLAG_NULLABLE: i64 = 2;
pub const ARROW_FLAG_MAP_KEYS_SORTED: i64 = 4;

// =============================================================================
// Zig-friendly Arrow Column
// =============================================================================

/// A Zig-friendly wrapper around Arrow's columnar memory layout.
/// Uses separate validity bitmap and dense value storage for memory efficiency.
///
/// Memory layout:
/// - validity: 1 bit per value, packed into bytes (LSB first)
/// - values: dense array of T, null positions contain undefined values
///
/// This uses ~50% less memory than Optional(T) for primitive types.
pub fn ArrowColumn(comptime T: type) type {
    return struct {
        /// Validity bitmap - 1 bit per value, packed (LSB first within each byte)
        /// null if there are no nulls in the column
        validity: ?[]u8,

        /// Dense values array - null positions have undefined values
        values: []T,

        /// Number of null values
        null_count: usize,

        /// Allocator used for memory
        allocator: std.mem.Allocator,

        const Self = @This();

        /// Initialize a new ArrowColumn with the given length.
        /// If has_nulls is true, allocates a validity bitmap.
        pub fn init(allocator: std.mem.Allocator, length: usize, has_nulls: bool) !Self {
            const validity = if (has_nulls) blk: {
                const bitmap_len = (std.math.add(usize, length, 7) catch return error.OutOfMemory) / 8;
                const bitmap = try allocator.alloc(u8, bitmap_len);
                // Default: all values are valid (bits set to 1)
                @memset(bitmap, 0xFF);
                break :blk bitmap;
            } else null;
            errdefer if (validity) |v| allocator.free(v);

            const values = try allocator.alloc(T, length);

            return .{
                .validity = validity,
                .values = values,
                .null_count = 0,
                .allocator = allocator,
            };
        }

        /// Free all memory associated with this column
        pub fn deinit(self: *Self) void {
            if (self.validity) |v| self.allocator.free(v);
            self.allocator.free(self.values);
            self.* = undefined;
        }

        /// Get the length of the column
        pub fn len(self: Self) usize {
            return self.values.len;
        }

        /// Check if the value at index i is null.
        /// Returns false for out-of-bounds indices.
        pub fn isNull(self: Self, i: usize) bool {
            if (i >= self.values.len) return false;
            const v = self.validity orelse return false;
            const byte_idx = i / 8;
            if (byte_idx >= v.len) return false;
            const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
            return (v[byte_idx] & (@as(u8, 1) << bit_idx)) == 0;
        }

        /// Check if the value at index i is valid (not null).
        /// Returns true for out-of-bounds indices.
        pub fn isValid(self: Self, i: usize) bool {
            return !self.isNull(i);
        }

        /// Set the value at index i to null. No-op for out-of-bounds.
        pub fn setNull(self: *Self, i: usize) void {
            if (i >= self.values.len) return;
            if (self.validity) |v| {
                const byte_idx = i / 8;
                if (byte_idx >= v.len) return;
                const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
                const was_valid = (v[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
                v[byte_idx] &= ~(@as(u8, 1) << bit_idx);
                if (was_valid) self.null_count += 1;
            }
        }

        /// Set the value at index i to valid (clear null bit). No-op for out-of-bounds.
        pub fn setValid(self: *Self, i: usize) void {
            if (i >= self.values.len) return;
            if (self.validity) |v| {
                const byte_idx = i / 8;
                if (byte_idx >= v.len) return;
                const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
                const was_null = (v[byte_idx] & (@as(u8, 1) << bit_idx)) == 0;
                v[byte_idx] |= (@as(u8, 1) << bit_idx);
                if (was_null and self.null_count > 0) self.null_count -= 1;
            }
        }

        /// Get the value at index i, returning null if the value is null or out-of-bounds.
        pub fn get(self: Self, i: usize) ?T {
            if (i >= self.values.len) return null;
            if (self.isNull(i)) return null;
            return self.values[i];
        }

        /// Set the value at index i. No-op for out-of-bounds.
        pub fn set(self: *Self, i: usize, value: ?T) void {
            if (i >= self.values.len) return;
            if (value) |v| {
                self.values[i] = v;
                self.setValid(i);
            } else {
                self.setNull(i);
            }
        }

        /// Get the Arrow format string for this type
        pub fn arrowFormat() [*:0]const u8 {
            return arrowFormatString(T);
        }
    };
}

// =============================================================================
// Validity Bitmap Utilities
// =============================================================================

/// Set a bit in a validity bitmap. No-op if i is out of bounds.
pub fn setBit(bitmap: []u8, i: usize) void {
    const byte_idx = i / 8;
    if (byte_idx >= bitmap.len) return;
    const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
    bitmap[byte_idx] |= (@as(u8, 1) << bit_idx);
}

/// Clear a bit in a validity bitmap (mark as null). No-op if i is out of bounds.
pub fn clearBit(bitmap: []u8, i: usize) void {
    const byte_idx = i / 8;
    if (byte_idx >= bitmap.len) return;
    const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
    bitmap[byte_idx] &= ~(@as(u8, 1) << bit_idx);
}

/// Get a bit from a validity bitmap (true = valid, false = null).
/// Returns false if i is out of bounds.
pub fn getBit(bitmap: []const u8, i: usize) bool {
    const byte_idx = i / 8;
    if (byte_idx >= bitmap.len) return false;
    const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
    return (bitmap[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
}

/// Count the number of set bits (valid values) in a bitmap.
/// Clamps len to the bitmap capacity (bitmap.len * 8).
pub fn countValidBits(bitmap: []const u8, len: usize) usize {
    const clamped_len = @min(len, bitmap.len * 8);
    var count: usize = 0;
    for (0..clamped_len) |i| {
        if (getBit(bitmap, i)) count += 1;
    }
    return count;
}

/// Count the number of null values in a bitmap.
/// Clamps len to the bitmap capacity (bitmap.len * 8).
pub fn countNullBits(bitmap: []const u8, len: usize) usize {
    const clamped_len = @min(len, bitmap.len * 8);
    return clamped_len - countValidBits(bitmap, clamped_len);
}

// =============================================================================
// Arrow Format Strings
// =============================================================================

/// Get the Arrow format string for a Zig type
pub fn arrowFormatString(comptime T: type) [*:0]const u8 {
    return switch (T) {
        bool => "b",
        i8 => "c",
        u8 => "C",
        i16 => "s",
        u16 => "S",
        i32 => "i",
        u32 => "I",
        i64 => "l",
        u64 => "L",
        f32 => "f",
        f64 => "g",
        []const u8 => "u", // UTF-8 string
        else => @compileError("Unsupported type for Arrow format: " ++ @typeName(T)),
    };
}

// Commonly used format strings
pub const FORMAT_BOOL = "b";
pub const FORMAT_INT8 = "c";
pub const FORMAT_UINT8 = "C";
pub const FORMAT_INT16 = "s";
pub const FORMAT_UINT16 = "S";
pub const FORMAT_INT32 = "i";
pub const FORMAT_UINT32 = "I";
pub const FORMAT_INT64 = "l";
pub const FORMAT_UINT64 = "L";
pub const FORMAT_FLOAT16 = "e";
pub const FORMAT_FLOAT32 = "f";
pub const FORMAT_FLOAT64 = "g";
pub const FORMAT_BINARY = "z"; // Variable-length binary
pub const FORMAT_STRING = "u"; // UTF-8 string
pub const FORMAT_LARGE_BINARY = "Z"; // Large variable-length binary (64-bit offsets)
pub const FORMAT_LARGE_STRING = "U"; // Large UTF-8 string (64-bit offsets)
pub const FORMAT_DATE32 = "tdD"; // Days since epoch
pub const FORMAT_DATE64 = "tdm"; // Milliseconds since epoch
pub const FORMAT_TIME32_S = "tts"; // Seconds since midnight
pub const FORMAT_TIME32_MS = "ttm"; // Milliseconds since midnight
pub const FORMAT_TIME64_US = "ttu"; // Microseconds since midnight
pub const FORMAT_TIME64_NS = "ttn"; // Nanoseconds since midnight
pub const FORMAT_TIMESTAMP_S = "tss:"; // Seconds since epoch (no timezone)
pub const FORMAT_TIMESTAMP_MS = "tsm:"; // Milliseconds since epoch
pub const FORMAT_TIMESTAMP_US = "tsu:"; // Microseconds since epoch
pub const FORMAT_TIMESTAMP_NS = "tsn:"; // Nanoseconds since epoch
pub const FORMAT_LIST = "+l"; // List
pub const FORMAT_LARGE_LIST = "+L"; // Large list (64-bit offsets)
pub const FORMAT_STRUCT = "+s"; // Struct
pub const FORMAT_MAP = "+m"; // Map

// =============================================================================
// Tests
// =============================================================================

test "ArrowColumn basic operations" {
    const allocator = std.testing.allocator;

    var col = try ArrowColumn(i64).init(allocator, 5, true);
    defer col.deinit();

    // Set some values
    col.set(0, 100);
    col.set(1, 200);
    col.set(2, null);
    col.set(3, 400);
    col.set(4, 500);

    // Check values
    try std.testing.expectEqual(@as(?i64, 100), col.get(0));
    try std.testing.expectEqual(@as(?i64, 200), col.get(1));
    try std.testing.expectEqual(@as(?i64, null), col.get(2));
    try std.testing.expectEqual(@as(?i64, 400), col.get(3));
    try std.testing.expectEqual(@as(?i64, 500), col.get(4));

    // Check null count
    try std.testing.expectEqual(@as(usize, 1), col.null_count);

    // Check validity
    try std.testing.expect(col.isValid(0));
    try std.testing.expect(col.isValid(1));
    try std.testing.expect(col.isNull(2));
    try std.testing.expect(col.isValid(3));
    try std.testing.expect(col.isValid(4));
}

test "ArrowColumn no nulls" {
    const allocator = std.testing.allocator;

    var col = try ArrowColumn(i32).init(allocator, 3, false);
    defer col.deinit();

    col.values[0] = 10;
    col.values[1] = 20;
    col.values[2] = 30;

    // No validity bitmap, all values are valid
    try std.testing.expect(col.validity == null);
    try std.testing.expect(col.isValid(0));
    try std.testing.expect(col.isValid(1));
    try std.testing.expect(col.isValid(2));

    try std.testing.expectEqual(@as(?i32, 10), col.get(0));
    try std.testing.expectEqual(@as(?i32, 20), col.get(1));
    try std.testing.expectEqual(@as(?i32, 30), col.get(2));
}

test "validity bitmap utilities" {
    var bitmap = [_]u8{ 0xFF, 0xFF }; // All valid

    // Clear bit 5
    clearBit(&bitmap, 5);
    try std.testing.expect(!getBit(&bitmap, 5));
    try std.testing.expect(getBit(&bitmap, 4));
    try std.testing.expect(getBit(&bitmap, 6));

    // Set bit 5 again
    setBit(&bitmap, 5);
    try std.testing.expect(getBit(&bitmap, 5));

    // Test across byte boundary
    clearBit(&bitmap, 8);
    try std.testing.expect(!getBit(&bitmap, 8));
    try std.testing.expect(getBit(&bitmap, 7));
    try std.testing.expect(getBit(&bitmap, 9));
}

test "arrow format strings" {
    try std.testing.expectEqualStrings("b", std.mem.sliceTo(arrowFormatString(bool), 0));
    try std.testing.expectEqualStrings("i", std.mem.sliceTo(arrowFormatString(i32), 0));
    try std.testing.expectEqualStrings("l", std.mem.sliceTo(arrowFormatString(i64), 0));
    try std.testing.expectEqualStrings("f", std.mem.sliceTo(arrowFormatString(f32), 0));
    try std.testing.expectEqualStrings("g", std.mem.sliceTo(arrowFormatString(f64), 0));
}

test "ArrowColumn OOB returns safe defaults" {
    const allocator = std.testing.allocator;
    var col = try ArrowColumn(i32).init(allocator, 3, true);
    defer col.deinit();

    col.set(0, 10);
    col.set(1, 20);
    col.set(2, 30);

    // OOB get returns null
    try std.testing.expectEqual(@as(?i32, null), col.get(10));
    try std.testing.expectEqual(@as(?i32, null), col.get(100));

    // OOB isNull returns false (safe default)
    try std.testing.expect(!col.isNull(10));

    // OOB set is a no-op (should not panic)
    col.set(10, 42);
    col.setNull(10);
    col.setValid(10);

    // In-bounds still works correctly
    try std.testing.expectEqual(@as(?i32, 10), col.get(0));
}

test "bitmap OOB is no-op" {
    var bitmap = [_]u8{ 0xFF, 0xFF };

    // OOB getBit returns false
    try std.testing.expect(!getBit(&bitmap, 100));

    // OOB setBit and clearBit are no-ops (should not panic)
    setBit(&bitmap, 100);
    clearBit(&bitmap, 100);

    // Existing bits unchanged
    try std.testing.expect(getBit(&bitmap, 0));
    try std.testing.expect(getBit(&bitmap, 15));

    // countValidBits clamps to bitmap capacity
    const count = countValidBits(&bitmap, 1000);
    try std.testing.expectEqual(@as(usize, 16), count);

    const null_count = countNullBits(&bitmap, 1000);
    try std.testing.expectEqual(@as(usize, 0), null_count);
}

test "memory efficiency comparison" {
    // This test documents the memory savings
    // Optional(i64) = 16 bytes per value (8 tag + 8 value)
    // ArrowColumn(i64) = 8.125 bytes per value (8 value + 1/8 validity bit)

    const n = 1000;

    // Optional would use: n * 16 = 16000 bytes
    const optional_size = n * 16;

    // Arrow uses: n * 8 + ceil(n/8) = 8000 + 125 = 8125 bytes
    const arrow_size = n * 8 + (n + 7) / 8;

    const savings = optional_size - arrow_size;
    const savings_pct = @as(f64, @floatFromInt(savings)) / @as(f64, @floatFromInt(optional_size)) * 100;

    // Should be approximately 49% savings
    try std.testing.expect(savings_pct > 48);
    try std.testing.expect(savings_pct < 51);
}
