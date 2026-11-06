//! Helpers shared by tests.

/// Returns `str` repeated `n` times. Replaces the `str ** n` operator removed in Zig 0.17.
/// Same approach as `repeatString` in the Zig 0.17 standard library (std/tar/Writer.zig).
pub inline fn repeatString(comptime n: usize, comptime str: []const u8) []const u8 {
    const buf: [n][str.len]u8 = @splat(str[0..str.len].*);
    return @ptrCast(&buf);
}
