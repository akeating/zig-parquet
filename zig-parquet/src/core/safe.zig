//! Safe operations for hardening against malformed data.
//!
//! All operations return errors instead of causing undefined behavior.
//! This module enables the library's safety guarantee: no UB from malformed input.

const std = @import("std");

/// Safe cast to usize. Uses anytype to infer source type.
/// Returns error.IntegerOverflow when value is negative or would overflow.
pub fn cast(val: anytype) error{IntegerOverflow}!usize {
    return castTo(usize, val);
}

/// Safe cast to a specific target type (when target is not usize).
pub fn castTo(comptime Target: type, val: anytype) error{IntegerOverflow}!Target {
    return std.math.cast(Target, val) orelse error.IntegerOverflow;
}

/// Safe slice with bounds checking.
pub fn slice(data: []const u8, offset: usize, len: usize) error{EndOfData}![]const u8 {
    if (offset > data.len) return error.EndOfData;
    if (len > data.len - offset) return error.EndOfData;
    return data[offset..][0..len];
}

/// Generic safe slice for any element type.
pub fn sliceOf(comptime T: type, data: []const T, offset: usize, len: usize) error{EndOfData}![]const T {
    if (offset > data.len) return error.EndOfData;
    if (len > data.len - offset) return error.EndOfData;
    return data[offset..][0..len];
}

/// Generic safe mutable slice for any element type.
pub fn sliceMutOf(comptime T: type, data: []T, offset: usize, len: usize) error{EndOfData}![]T {
    if (offset > data.len) return error.EndOfData;
    if (len > data.len - offset) return error.EndOfData;
    return data[offset..][0..len];
}

/// Safe mutable slice with bounds checking.
pub fn sliceMut(data: []u8, offset: usize, len: usize) error{EndOfData}![]u8 {
    if (offset > data.len) return error.EndOfData;
    if (len > data.len - offset) return error.EndOfData;
    return data[offset..][0..len];
}

/// Safe array indexing.
pub fn get(comptime T: type, data: []const T, index: usize) error{EndOfData}!T {
    if (index >= data.len) return error.EndOfData;
    return data[index];
}

// Tests
test "cast positive i32" {
    const result = try cast(@as(i32, 42));
    try std.testing.expectEqual(@as(usize, 42), result);
}

test "cast negative i32 returns error" {
    const result = cast(@as(i32, -1));
    try std.testing.expectError(error.IntegerOverflow, result);
}

test "cast unsigned always succeeds" {
    const result = try cast(@as(u32, 100));
    try std.testing.expectEqual(@as(usize, 100), result);
}

test "castTo within range" {
    const result = try castTo(u8, @as(i32, 200));
    try std.testing.expectEqual(@as(u8, 200), result);
}

test "castTo overflow returns error" {
    const result = castTo(u8, @as(i32, 300));
    try std.testing.expectError(error.IntegerOverflow, result);
}

test "slice valid range" {
    const data = "hello world";
    const result = try slice(data, 0, 5);
    try std.testing.expectEqualStrings("hello", result);
}

test "slice offset out of bounds" {
    const data = "hello";
    const result = slice(data, 10, 1);
    try std.testing.expectError(error.EndOfData, result);
}

test "slice length out of bounds" {
    const data = "hello";
    const result = slice(data, 3, 5);
    try std.testing.expectError(error.EndOfData, result);
}

test "get valid index" {
    const data = [_]u32{ 1, 2, 3, 4, 5 };
    const result = try get(u32, &data, 2);
    try std.testing.expectEqual(@as(u32, 3), result);
}

test "get out of bounds" {
    const data = [_]u32{ 1, 2, 3 };
    const result = get(u32, &data, 5);
    try std.testing.expectError(error.EndOfData, result);
}
