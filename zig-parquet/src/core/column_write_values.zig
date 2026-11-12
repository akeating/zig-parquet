//! Writes one leaf column chunk of a nested column from flattened values and levels: the
//! output of `nested.flattenValue`, one `Value` per level entry, with `null_val` where the
//! entry has no value.
//!
//! The chunk is split into data pages of about `max_page_size` bytes of encoded values. A
//! page ends only before an entry with repetition level 0, so every page starts at a row;
//! page indexes and the readers require this. With `page_index`, one entry per data page
//! is recorded: its location, first row, row count, min, max and null count. A dictionary,
//! when used, is built once for the chunk and written as one dictionary page before the
//! data pages.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const page_writer = @import("page_writer.zig");
const compress = @import("compress/mod.zig");
const rle_encoder = @import("encoding/rle_encoder.zig");
const statistics = @import("statistics.zig");
const page_index_writer = @import("page_index_writer.zig");
const column_writer = @import("column_writer.zig");
const Value = @import("value.zig").Value;

const Allocator = std.mem.Allocator;
pub const ColumnWriteError = column_writer.ColumnWriteError;
pub const ColumnChunkResult = column_writer.ColumnChunkResult;

/// Encoding options for nested (Value-based) column writing.
pub const NestedEncodingOpts = struct {
    use_dictionary: bool = false,
    encoding: ?format.Encoding = null,
    int_encoding: format.Encoding = .plain,
    float_encoding: format.Encoding = .plain,
    /// Target size of a data page's encoded values, in bytes. Null writes one data page.
    max_page_size: ?usize = null,
    dictionary_size_limit: ?usize = 1_048_576,
    dictionary_cardinality_threshold: ?f32 = null,
    write_page_checksum: bool = true,
    /// The column's physical type, used when no value is non-null (otherwise the type is
    /// taken from the values).
    physical_type: ?format.PhysicalType = null,
    /// Receives one entry per data page.
    page_index: ?*page_index_writer.PageIndexBuilder = null,
};

pub const Levels = struct {
    def: []const u32,
    rep: []const u32,
    max_def: u8,
    max_rep: u8,
};

const Kind = enum { int32, int64, float, double, boolean, byte_array, fixed_len_byte_array };

fn Elem(comptime kind: Kind) type {
    return switch (kind) {
        .int32 => i32,
        .int64 => i64,
        .float => f32,
        .double => f64,
        .boolean => bool,
        .byte_array, .fixed_len_byte_array => []const u8,
    };
}

fn isBytes(comptime kind: Kind) bool {
    return kind == .byte_array or kind == .fixed_len_byte_array;
}

/// Write a column chunk from Value data with definition and repetition levels.
pub fn writeColumnChunkFromValues(
    allocator: Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Value,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    opts: NestedEncodingOpts,
) ColumnWriteError!ColumnChunkResult {
    // The flattener appends a value and both levels for every entry.
    if (values.len != def_levels.len) return error.WriteError;
    if (max_rep_level > 0 and rep_levels.len != def_levels.len) return error.WriteError;

    const levels: Levels = .{ .def = def_levels, .rep = rep_levels, .max_def = max_def_level, .max_rep = max_rep_level };
    const kind: Kind = detectKind(values) orelse if (opts.physical_type) |t| switch (t) {
        .int32, .int96 => .int32,
        .int64 => .int64,
        .float => .float,
        .double => .double,
        .byte_array => .byte_array,
        .fixed_len_byte_array => .fixed_len_byte_array,
        .boolean => .boolean,
    } else .int32;

    const chunk: Chunk = .{
        .allocator = allocator,
        .output = output,
        .path_in_schema = path_in_schema,
        .values = values,
        .levels = levels,
        .start_offset = start_offset,
        .codec = codec,
        .opts = opts,
    };
    return switch (kind) {
        inline else => |k| chunk.write(k),
    };
}

/// The kind of the first non-null value, or null when there is none or it is not a
/// primitive value.
fn detectKind(values: []const Value) ?Kind {
    for (values) |v| {
        return switch (v) {
            .int32_val => .int32,
            .int64_val => .int64,
            .float_val => .float,
            .double_val => .double,
            .bytes_val => .byte_array,
            .fixed_bytes_val => .fixed_len_byte_array,
            .bool_val => .boolean,
            .null_val => continue,
            else => null,
        };
    }
    return null;
}

/// The entry's value as the column's element type, or null when the entry has none. A
/// value of another type is treated as absent.
fn extract(comptime kind: Kind, v: Value) ?Elem(kind) {
    return switch (kind) {
        .int32 => if (v == .int32_val) v.int32_val else null,
        .int64 => if (v == .int64_val) v.int64_val else null,
        .float => if (v == .float_val) v.float_val else null,
        .double => if (v == .double_val) v.double_val else null,
        .boolean => if (v == .bool_val) v.bool_val else null,
        .byte_array, .fixed_len_byte_array => switch (v) {
            .bytes_val => |b| b,
            .fixed_bytes_val => |b| b,
            else => null,
        },
    };
}

/// Value encoding of the data pages, or null for dictionary encoding.
fn chosenEncoding(comptime kind: Kind, opts: NestedEncodingOpts) ?format.Encoding {
    return switch (kind) {
        .int32, .int64 => opts.encoding orelse if (opts.use_dictionary) null else opts.int_encoding,
        .float, .double => opts.encoding orelse if (opts.use_dictionary) null else opts.float_encoding,
        .byte_array => opts.encoding orelse if (opts.use_dictionary) null else .plain,
        .fixed_len_byte_array => opts.encoding orelse .plain,
        .boolean => .plain,
    };
}

/// Encoding used when the dictionary exceeds its size or cardinality limit.
fn fallbackEncoding(comptime kind: Kind, opts: NestedEncodingOpts) format.Encoding {
    return switch (kind) {
        .int32, .int64 => opts.int_encoding,
        .float, .double => opts.float_encoding,
        else => .plain,
    };
}

fn physicalType(comptime kind: Kind) format.PhysicalType {
    return switch (kind) {
        .int32 => .int32,
        .int64 => .int64,
        .float => .float,
        .double => .double,
        .boolean => .boolean,
        .byte_array => .byte_array,
        .fixed_len_byte_array => .fixed_len_byte_array,
    };
}

fn mapEncodeError(e: anyerror) ColumnWriteError {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.UnsupportedEncoding => error.UnsupportedEncoding,
        error.InvalidFixedLength => error.InvalidFixedLength,
        error.ValueTooLarge => error.ValueTooLarge,
        error.IntegerOverflow => error.IntegerOverflow,
        else => error.WriteError,
    };
}

fn encodeValues(comptime kind: Kind, allocator: Allocator, vals: []const Elem(kind), encoding: format.Encoding, fixed_len: usize) ColumnWriteError![]u8 {
    return switch (kind) {
        .byte_array => page_writer.encodeByteArraysWithEncoding(allocator, vals, encoding),
        .fixed_len_byte_array => if (vals.len == 0)
            allocator.alloc(u8, 0)
        else
            page_writer.encodeFixedByteArraysWithEncoding(allocator, vals, fixed_len, encoding),
        else => page_writer.encodeValuesWithEncoding(allocator, Elem(kind), vals, encoding),
    } catch |e| mapEncodeError(e);
}

/// Size of a PLAIN-encoded value in bits; used to size pages.
fn plainBits(comptime kind: Kind, x: Elem(kind)) u64 {
    return switch (kind) {
        .boolean => 1,
        .byte_array => 8 * (4 + @as(u64, x.len)),
        .fixed_len_byte_array => 8 * @as(u64, x.len),
        else => @bitSizeOf(Elem(kind)),
    };
}

/// Statistics of `vals` with `nulls` null entries.
fn buildStatistics(comptime kind: Kind, allocator: Allocator, vals: []const Elem(kind), nulls: i64) ColumnWriteError!?format.Statistics {
    if (comptime isBytes(kind)) {
        var b = statistics.ByteArrayStatisticsBuilder.init(allocator);
        defer b.deinit();
        try b.update(vals);
        b.addNulls(nulls);
        return b.build();
    }
    var b = statistics.StatisticsBuilder(Elem(kind)){};
    b.update(vals);
    b.addNulls(nulls);
    return b.build(allocator) catch error.OutOfMemory;
}

/// Dictionary of a chunk's non-null values.
fn Dictionary(comptime kind: Kind) type {
    return struct {
        /// Index of each non-null value.
        indices: []u32,
        /// PLAIN-encoded dictionary entries.
        page: []u8,
        count: usize,
        bit_width: u5,

        fn deinit(self: *@This(), allocator: Allocator) void {
            allocator.free(self.indices);
            allocator.free(self.page);
        }

        /// Builds the dictionary, or returns null when it exceeds the size or cardinality
        /// limit in `opts`.
        fn build(allocator: Allocator, vals: []const Elem(kind), opts: NestedEncodingOpts) ColumnWriteError!?@This() {
            const Map = if (comptime isBytes(kind))
                std.StringHashMap(u32)
            else
                std.HashMap(Elem(kind), u32, column_writer.DictHashContext(Elem(kind)), std.hash_map.default_max_load_percentage);
            var map = Map.init(allocator);
            defer map.deinit();
            var entries: std.ArrayListUnmanaged(Elem(kind)) = .empty;
            defer entries.deinit(allocator);

            const indices = try allocator.alloc(u32, vals.len);
            errdefer allocator.free(indices);

            var byte_count: usize = 0;
            for (vals, 0..) |v, i| {
                if (comptime isBytes(kind)) if (v.len > std.math.maxInt(i32)) return error.ValueTooLarge;
                const gop = try map.getOrPut(v);
                if (!gop.found_existing) {
                    gop.value_ptr.* = try safe.castTo(u32, entries.items.len);
                    try entries.append(allocator, v);
                    byte_count += if (comptime isBytes(kind)) 4 + v.len else @sizeOf(Elem(kind));
                }
                indices[i] = gop.value_ptr.*;

                if (opts.dictionary_cardinality_threshold) |threshold| if (i >= 1024) {
                    const ratio = @as(f32, @floatFromInt(entries.items.len)) / @as(f32, @floatFromInt(i));
                    if (ratio > threshold) {
                        allocator.free(indices);
                        return null;
                    }
                };
            }
            if (opts.dictionary_size_limit) |limit| if (byte_count > limit) {
                allocator.free(indices);
                return null;
            };

            const n = entries.items.len;
            const bit_width: u5 = if (n <= 1) 0 else try safe.castTo(u5, std.math.log2_int(usize, n - 1) + 1);
            return .{
                .indices = indices,
                .page = try encodeValues(kind, allocator, entries.items, .plain, 0),
                .count = n,
                .bit_width = bit_width,
            };
        }
    };
}

/// Writes compressed pages and counts the bytes written.
const PageOutput = struct {
    allocator: Allocator,
    output: *std.Io.Writer,
    codec: format.CompressionCodec,
    write_page_checksum: bool,
    compressed_bytes: usize = 0,
    uncompressed_bytes: usize = 0,

    const Page = union(enum) {
        data: struct { num_values: usize, encoding: format.Encoding },
        dictionary: usize,
    };

    /// Compresses and writes one page. Returns its size in the file, header included.
    fn write(self: *PageOutput, data: []const u8, page: Page) ColumnWriteError!usize {
        const compressed: []const u8 = if (self.codec == .uncompressed)
            data
        else
            compress.compress(self.allocator, data, self.codec) catch |err| return switch (err) {
                error.UnsupportedCompression => error.UnsupportedCompression,
                error.OutOfMemory => error.OutOfMemory,
                else => error.CompressionError,
            };
        defer if (self.codec != .uncompressed) self.allocator.free(compressed);

        const header: format.PageHeader = .{
            .type_ = switch (page) {
                .data => .data_page,
                .dictionary => .dictionary_page,
            },
            .uncompressed_page_size = try safe.castTo(i32, data.len),
            .compressed_page_size = try safe.castTo(i32, compressed.len),
            .crc = if (self.write_page_checksum) column_writer.computePageCrc(compressed) else null,
            .data_page_header = switch (page) {
                .data => |d| .{
                    .num_values = try safe.castTo(i32, d.num_values),
                    .encoding = d.encoding,
                    .definition_level_encoding = .rle,
                    .repetition_level_encoding = .rle,
                    .statistics = null,
                },
                .dictionary => null,
            },
            .dictionary_page_header = switch (page) {
                .data => null,
                .dictionary => |n| .{
                    .num_values = try safe.castTo(i32, n),
                    .encoding = .plain,
                    .is_sorted = false,
                },
            },
        };
        var tw = thrift.CompactWriter.init(self.allocator);
        defer tw.deinit();
        header.serialize(&tw) catch return error.OutOfMemory;
        const header_bytes = tw.getWritten();

        self.output.writeAll(header_bytes) catch return error.WriteError;
        self.output.writeAll(compressed) catch return error.WriteError;

        const page_bytes = std.math.add(usize, header_bytes.len, compressed.len) catch return error.IntegerOverflow;
        self.compressed_bytes = std.math.add(usize, self.compressed_bytes, page_bytes) catch return error.IntegerOverflow;
        const uncompressed = std.math.add(usize, header_bytes.len, data.len) catch return error.IntegerOverflow;
        self.uncompressed_bytes = std.math.add(usize, self.uncompressed_bytes, uncompressed) catch return error.IntegerOverflow;
        return page_bytes;
    }
};

/// Repetition levels, definition levels and `value_bytes` of entries `[e0, e1)`, as a data
/// page v1 body.
fn pageBody(allocator: Allocator, levels: Levels, e0: usize, e1: usize, value_bytes: []const []const u8) ColumnWriteError![]u8 {
    const rep: []const u8 = if (levels.max_rep > 0)
        rle_encoder.encodeLevelsWithLength(allocator, levels.rep[e0..e1], levels.max_rep) catch |e| return mapEncodeError(e)
    else
        &.{};
    defer if (levels.max_rep > 0) allocator.free(rep);
    const def: []const u8 = if (levels.max_def > 0)
        rle_encoder.encodeLevelsWithLength(allocator, levels.def[e0..e1], levels.max_def) catch |e| return mapEncodeError(e)
    else
        &.{};
    defer if (levels.max_def > 0) allocator.free(def);

    var len = rep.len + def.len;
    for (value_bytes) |part| len += part.len;
    const body = try allocator.alloc(u8, len);
    var at: usize = 0;
    for ([_][]const u8{ rep, def }) |part| {
        @memcpy(body[at..][0..part.len], part);
        at += part.len;
    }
    for (value_bytes) |part| {
        @memcpy(body[at..][0..part.len], part);
        at += part.len;
    }
    return body;
}

const Chunk = struct {
    allocator: Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Value,
    levels: Levels,
    start_offset: i64,
    codec: format.CompressionCodec,
    opts: NestedEncodingOpts,

    fn write(self: *const Chunk, comptime kind: Kind) ColumnWriteError!ColumnChunkResult {
        const allocator = self.allocator;
        const T = Elem(kind);
        const num_entries = self.levels.def.len;

        var vals: std.ArrayListUnmanaged(T) = .empty;
        defer vals.deinit(allocator);
        for (self.values) |v| if (extract(kind, v)) |x| try vals.append(allocator, x);

        var fixed_len: usize = 0;
        if (comptime kind == .fixed_len_byte_array) if (vals.items.len > 0) {
            fixed_len = vals.items[0].len;
            if (fixed_len == 0) return error.InvalidFixedLength;
        };

        const chosen = chosenEncoding(kind, self.opts);
        var dict: ?Dictionary(kind) = if (chosen == null and vals.items.len > 0)
            try Dictionary(kind).build(allocator, vals.items, self.opts)
        else
            null;
        defer if (dict) |*d| d.deinit(allocator);
        const value_encoding: format.Encoding = if (dict != null)
            .rle_dictionary
        else
            chosen orelse fallbackEncoding(kind, self.opts);

        var out: PageOutput = .{
            .allocator = allocator,
            .output = self.output,
            .codec = self.codec,
            .write_page_checksum = self.opts.write_page_checksum,
        };
        if (dict) |d| _ = try out.write(d.page, .{ .dictionary = d.count });
        const data_page_offset = self.start_offset + try safe.castTo(i64, out.compressed_bytes);

        // Pages are cut before a row start once the page's values reach this many bits.
        const limit_bits: ?u64 = if (self.opts.max_page_size) |m| std.math.mul(u64, m, 8) catch std.math.maxInt(u64) else null;

        var e0: usize = 0;
        var v0: usize = 0;
        var first_row: i64 = 0;
        while (true) {
            // Extend the page to the first row start past the size limit.
            var e = e0;
            var v = v0;
            var rows: i64 = 0;
            var bits: u64 = 0;
            while (e < num_entries) : (e += 1) {
                if (self.levels.max_rep == 0 or self.levels.rep[e] == 0) {
                    if (limit_bits) |lim| if (e > e0 and bits >= lim) break;
                    rows += 1;
                }
                if (extract(kind, self.values[e])) |x| {
                    bits +|= if (dict) |d| @max(d.bit_width, 1) else plainBits(kind, x);
                    v += 1;
                }
            }

            const page_vals = vals.items[v0..v];
            var value_bytes: []u8 = undefined;
            var bit_width_byte: [1]u8 = undefined;
            if (dict) |d| {
                bit_width_byte[0] = d.bit_width;
                value_bytes = rle_encoder.encode(allocator, d.indices[v0..v], d.bit_width) catch return error.OutOfMemory;
            } else {
                value_bytes = try encodeValues(kind, allocator, page_vals, value_encoding, fixed_len);
            }
            defer allocator.free(value_bytes);
            const parts: []const []const u8 = if (dict != null) &.{ &bit_width_byte, value_bytes } else &.{value_bytes};
            const body = try pageBody(allocator, self.levels, e0, e, parts);
            defer allocator.free(body);

            const page_offset = self.start_offset + try safe.castTo(i64, out.compressed_bytes);
            const page_bytes = try out.write(body, .{ .data = .{ .num_values = e - e0, .encoding = value_encoding } });

            if (self.opts.page_index) |pib| {
                const nulls = try safe.castTo(i64, (e - e0) - page_vals.len);
                const st = try buildStatistics(kind, allocator, page_vals, nulls);
                defer if (st) |x| column_writer.freeStatistics(allocator, x);
                const empty: []const u8 = &.{};
                pib.recordPage(
                    page_offset,
                    try safe.castTo(i32, page_bytes),
                    first_row,
                    rows,
                    if (st) |x| x.getMinBytes() orelse empty else empty,
                    if (st) |x| x.getMaxBytes() orelse empty else empty,
                    nulls,
                    page_vals.len == 0,
                ) catch return error.OutOfMemory;
            }

            first_row = std.math.add(i64, first_row, rows) catch return error.IntegerOverflow;
            e0 = e;
            v0 = v;
            if (e0 >= num_entries) break;
        }

        const path = try allocator.alloc([]const u8, self.path_in_schema.len);
        var copied: usize = 0;
        errdefer {
            for (path[0..copied]) |seg| allocator.free(seg);
            allocator.free(path);
        }
        for (self.path_in_schema, path) |seg, *p| {
            p.* = try allocator.dupe(u8, seg);
            copied += 1;
        }
        const encodings = if (dict != null)
            try allocator.dupe(format.Encoding, &.{ .plain, .rle, .rle_dictionary })
        else
            try allocator.dupe(format.Encoding, &.{ .rle, value_encoding });
        errdefer allocator.free(encodings);

        const nulls = try safe.castTo(i64, num_entries - vals.items.len);
        return .{
            .metadata = .{
                .type_ = physicalType(kind),
                .encodings = encodings,
                .path_in_schema = path,
                .codec = self.codec,
                .num_values = try safe.castTo(i64, num_entries),
                .total_uncompressed_size = try safe.castTo(i64, out.uncompressed_bytes),
                .total_compressed_size = try safe.castTo(i64, out.compressed_bytes),
                .data_page_offset = data_page_offset,
                .index_page_offset = null,
                .dictionary_page_offset = if (dict != null) self.start_offset else null,
                .statistics = try buildStatistics(kind, allocator, vals.items, nulls),
            },
            .file_offset = self.start_offset,
            .total_bytes = out.compressed_bytes,
        };
    }
};
