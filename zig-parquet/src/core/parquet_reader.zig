//! Shared Parquet reading utilities
//!
//! Common code for parsing Parquet file structure using SeekableReader.
//! This eliminates code duplication across Reader, DynamicReader, and RowReader.

const std = @import("std");
const safe = @import("safe.zig");
const seekable_reader = @import("seekable_reader.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const types = @import("types.zig");

pub const SeekableReader = seekable_reader.SeekableReader;
pub const ReaderError = types.ReaderError;

/// Cleanup callback for owned backends (e.g., heap-allocated FileReader/BufferReader).
/// Set by convenience constructors in api/zig/. Core code does not create these.
pub const BackendCleanup = struct {
    ptr: *anyopaque,
    deinit_fn: *const fn (*anyopaque, std.mem.Allocator) void,
};

/// Options for controlling page checksum validation.
pub const ChecksumOptions = struct {
    /// Whether to validate page CRC32 checksums when reading.
    /// Default: false (for performance; checksums verified on-demand)
    validate_page_checksum: bool = false,

    /// If true, return error when a page has no CRC checksum.
    /// Only applies when validate_page_checksum is true.
    /// Default: false (allow pages without checksums)
    strict_checksum: bool = false,
};

/// Allocation limits for reading untrusted Parquet files.
///
/// These limits help prevent denial-of-service attacks where a malicious
/// Parquet file could cause excessive memory allocation.
pub const AllocationLimits = struct {
    /// Maximum dictionary size in bytes (default: 64MB)
    max_dictionary_size: usize = 64 * 1024 * 1024,

    /// Maximum page size in bytes (default: 128MB)
    max_page_size: usize = 128 * 1024 * 1024,

    /// Maximum string length in bytes (default: 16MB)
    max_string_length: usize = 16 * 1024 * 1024,

    /// Check if a dictionary size is within limits
    pub fn checkDictionarySize(self: AllocationLimits, size: usize) ReaderError!void {
        if (size > self.max_dictionary_size) return error.AllocationLimitExceeded;
    }

    /// Check if a page size is within limits
    pub fn checkPageSize(self: AllocationLimits, size: usize) ReaderError!void {
        if (size > self.max_page_size) return error.AllocationLimitExceeded;
    }

    /// Check if a string length is within limits
    pub fn checkStringLength(self: AllocationLimits, length: usize) ReaderError!void {
        if (length > self.max_string_length) return error.AllocationLimitExceeded;
    }

    /// Default limits (generous for most use cases)
    pub const default = AllocationLimits{};

    /// No limits (for trusted input only)
    pub const unlimited = AllocationLimits{
        .max_dictionary_size = std.math.maxInt(usize),
        .max_page_size = std.math.maxInt(usize),
        .max_string_length = std.math.maxInt(usize),
    };
};


/// Result of parsing a Parquet file footer
pub const FooterInfo = struct {
    metadata: format.FileMetaData,
    footer_data: []u8, // Caller must free with allocator
};

/// Parse Parquet file footer from a SeekableReader.
///
/// This validates the magic bytes, reads the footer, and parses the Thrift-encoded
/// FileMetaData. The caller owns the returned footer_data and must free it.
///
/// This is the shared implementation used by Reader, DynamicReader, and RowReader.
pub fn parseFooter(allocator: std.mem.Allocator, source: SeekableReader) ReaderError!FooterInfo {
    const file_size = source.size();
    if (file_size < 12) return error.FileTooSmall; // 4 (magic) + 4 (footer size) + 4 (magic)

    // Read the last 8 bytes to get footer size and validate trailing magic
    var tail: [8]u8 = undefined;
    const tail_read = source.readAt(file_size - 8, &tail) catch return error.InputOutput;
    if (tail_read != 8) return error.InputOutput;

    // Validate trailing magic
    if (!std.mem.eql(u8, tail[4..8], format.PARQUET_MAGIC)) {
        return error.InvalidMagic;
    }

    // Get footer size
    const footer_size = std.mem.readInt(u32, tail[0..4], .little);
    if (footer_size > file_size - 8) return error.FooterTooLarge;

    // Read footer data
    const footer_data = allocator.alloc(u8, footer_size) catch return error.OutOfMemory;
    errdefer allocator.free(footer_data);

    const footer_read = source.readAt(file_size - 8 - footer_size, footer_data) catch return error.InputOutput;
    if (footer_read != footer_size) return error.InputOutput;

    // Validate leading magic
    var head: [4]u8 = undefined;
    const head_read = source.readAt(0, &head) catch return error.InputOutput;
    if (head_read != 4) return error.InputOutput;
    if (!std.mem.eql(u8, &head, format.PARQUET_MAGIC)) {
        return error.InvalidMagic;
    }

    // Parse the Thrift-encoded metadata
    var thrift_reader = thrift.CompactReader.init(footer_data);
    const metadata = format.FileMetaData.parse(allocator, &thrift_reader) catch |e| {
        return e;
    };

    return .{
        .metadata = metadata,
        .footer_data = footer_data,
    };
}

/// Read raw page data for a column chunk from a SeekableReader.
///
/// This reads the entire column chunk (including dictionary page if present)
/// into a newly allocated buffer. The caller owns the returned data.
pub fn readColumnChunkData(
    allocator: std.mem.Allocator,
    source: SeekableReader,
    chunk: *const format.ColumnChunk,
) ReaderError![]u8 {
    const meta = chunk.meta_data orelse return error.InvalidArgument;

    // Determine where to start reading
    // Note: Some writers set dictionary_page_offset=0 to mean "no dictionary"
    // (0 is invalid as it's before the PAR1 magic bytes)
    const start_offset: u64 = if (meta.dictionary_page_offset) |dict_off|
        if (dict_off > 0) try safe.cast(dict_off) else try safe.cast(meta.data_page_offset)
    else
        try safe.cast(meta.data_page_offset);

    const size = try safe.cast(meta.total_compressed_size);

    // Read the data
    const data = allocator.alloc(u8, size) catch return error.OutOfMemory;
    errdefer allocator.free(data);

    const bytes_read = source.readAt(start_offset, data) catch return error.InputOutput;
    if (bytes_read != size) return error.InputOutput;

    return data;
}

// ============================================================================
// Page Header Helpers
// ============================================================================

/// Free the allocated contents within a PageHeader (statistics min/max values).
///
/// This does NOT free the PageHeader struct itself - only the heap-allocated
/// fields inside it (Statistics.min, max, min_value, max_value).
/// Call this on every PageHeader returned by PageIterator.next().
pub fn freePageHeaderContents(allocator: std.mem.Allocator, header: *const format.PageHeader) void {
    if (header.data_page_header) |dph| {
        if (dph.statistics) |stats| {
            freeStatistics(allocator, &stats);
        }
    }
    if (header.data_page_header_v2) |v2| {
        if (v2.statistics) |stats| {
            freeStatistics(allocator, &stats);
        }
    }
}

/// Free Statistics allocated memory
fn freeStatistics(allocator: std.mem.Allocator, stats: *const format.Statistics) void {
    if (stats.max) |m| allocator.free(m);
    if (stats.min) |m| allocator.free(m);
    if (stats.max_value) |m| allocator.free(m);
    if (stats.min_value) |m| allocator.free(m);
}

/// Result of parsing a page header
pub const PageInfo = struct {
    /// The parsed page header (caller must free with freePageHeaderContents)
    header: format.PageHeader,
    /// The compressed page body data (slice into page_data)
    body: []const u8,
    /// Position after this page in the source data
    next_pos: usize,
    /// Whether this is a dictionary page
    is_dictionary: bool,
    /// Whether this is a data page (V1 or V2)
    is_data_page: bool,
};

/// Iterator for parsing pages from a column chunk's raw data.
///
/// This handles the common logic of parsing page headers and extracting
/// page bodies, abstracting away the Thrift parsing and position tracking.
pub const PageIterator = struct {
    page_data: []const u8,
    pos: usize,
    allocator: std.mem.Allocator,
    checksum_options: ChecksumOptions,

    const Self = @This();
    const crc = std.hash.crc;

    /// Compute CRC32 checksum for page data (per Parquet spec, covers compressed data)
    fn computePageCrc(data: []const u8) i32 {
        return @bitCast(crc.Crc32.hash(data));
    }

    /// Initialize a page iterator
    pub fn init(allocator: std.mem.Allocator, page_data: []const u8) Self {
        return .{
            .page_data = page_data,
            .pos = 0,
            .allocator = allocator,
            .checksum_options = .{}, // Default: no validation
        };
    }

    /// Initialize a page iterator with checksum options
    pub fn initWithOptions(allocator: std.mem.Allocator, page_data: []const u8, checksum_options: ChecksumOptions) Self {
        return .{
            .page_data = page_data,
            .pos = 0,
            .allocator = allocator,
            .checksum_options = checksum_options,
        };
    }

    /// Get the next page, or null if no more pages.
    /// Caller must call freePageHeaderContents on the returned header when done.
    pub fn next(self: *Self) ReaderError!?PageInfo {
        if (self.pos >= self.page_data.len) {
            return null;
        }

        // Parse page header using Thrift
        var thrift_reader = thrift.CompactReader.init(self.page_data[self.pos..]);
        const header = format.PageHeader.parse(self.allocator, &thrift_reader) catch |e| {
            return e;
        };
        errdefer freePageHeaderContents(self.allocator, &header);

        const header_end = self.pos + thrift_reader.pos;

        // Get the compressed body
        const compressed_size = safe.cast(header.compressed_page_size) catch return error.IntegerOverflow;
        if (header_end + compressed_size > self.page_data.len) {
            freePageHeaderContents(self.allocator, &header);
            return error.InvalidPageData;
        }

        const body = try safe.slice(self.page_data, header_end, compressed_size);
        const next_pos = header_end + compressed_size;

        // Validate CRC32 checksum if enabled
        if (self.checksum_options.validate_page_checksum) {
            if (header.crc) |expected_crc| {
                const actual_crc = computePageCrc(body);
                if (actual_crc != expected_crc) {
                    freePageHeaderContents(self.allocator, &header);
                    return error.PageChecksumMismatch;
                }
            } else if (self.checksum_options.strict_checksum) {
                freePageHeaderContents(self.allocator, &header);
                return error.MissingPageChecksum;
            }
        }

        // Determine page type
        const is_dictionary = header.dictionary_page_header != null;
        const is_data_page = header.data_page_header != null or header.data_page_header_v2 != null;

        // Advance position
        self.pos = next_pos;

        return .{
            .header = header,
            .body = body,
            .next_pos = next_pos,
            .is_dictionary = is_dictionary,
            .is_data_page = is_data_page,
        };
    }

    /// Skip a page without returning it (useful for skipping dictionary pages already processed)
    pub fn skip(self: *Self) ReaderError!bool {
        const page_info = try self.next();
        if (page_info) |info| {
            freePageHeaderContents(self.allocator, &info.header);
            return true;
        }
        return false;
    }

    /// Peek at the next page header without consuming it.
    /// Caller must call freePageHeaderContents on the returned header when done.
    pub fn peek(self: *Self) ReaderError!?format.PageHeader {
        if (self.pos >= self.page_data.len) {
            return null;
        }

        var thrift_reader = thrift.CompactReader.init(self.page_data[self.pos..]);
        const header = format.PageHeader.parse(self.allocator, &thrift_reader) catch |e| {
            return e;
        };

        return header;
    }

    /// Check if there are more pages
    pub fn hasMore(self: *Self) bool {
        return self.pos < self.page_data.len;
    }
};

// ============================================================================
// Dictionary Helpers
// ============================================================================

const dictionary = @import("encoding/dictionary.zig");
const compress = @import("compress/mod.zig");

/// Container for all dictionary types that might be used when decoding a column.
/// This eliminates the need to declare 3-4 separate dictionary variables in each
/// column reading function.
pub const DictionarySet = struct {
    string_dict: ?dictionary.StringDictionary = null,
    int32_dict: ?dictionary.Int32Dictionary = null,
    int64_dict: ?dictionary.Int64Dictionary = null,
    float32_dict: ?dictionary.Float32Dictionary = null,
    float64_dict: ?dictionary.Float64Dictionary = null,
    int96_dict: ?dictionary.Int96Dictionary = null,
    fixed_byte_array_dict: ?dictionary.FixedByteArrayDictionary = null,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize an empty dictionary set
    pub fn init(allocator: std.mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Check if any dictionary is present
    pub fn hasDictionary(self: *const Self) bool {
        return self.string_dict != null or
            self.int32_dict != null or
            self.int64_dict != null or
            self.float32_dict != null or
            self.float64_dict != null or
            self.int96_dict != null or
            self.fixed_byte_array_dict != null;
    }

    /// Initialize dictionaries from a dictionary page.
    /// This parses the dictionary page and builds the appropriate dictionary
    /// based on the physical type.
    pub fn initFromPage(
        self: *Self,
        dict_page_body: []const u8,
        num_values: usize,
        physical_type: ?format.PhysicalType,
        type_length: ?i32,
        codec: format.CompressionCodec,
        uncompressed_size: usize,
    ) ReaderError!void {
        return self.initFromPageWithLimits(dict_page_body, num_values, physical_type, type_length, codec, uncompressed_size, null);
    }

    /// Initialize dictionaries from a dictionary page with allocation limits.
    /// This parses the dictionary page and builds the appropriate dictionary
    /// based on the physical type.
    pub fn initFromPageWithLimits(
        self: *Self,
        dict_page_body: []const u8,
        num_values: usize,
        physical_type: ?format.PhysicalType,
        type_length: ?i32,
        codec: format.CompressionCodec,
        uncompressed_size: usize,
        limits: ?AllocationLimits,
    ) ReaderError!void {
        // Check allocation limits
        if (limits) |lim| {
            try lim.checkDictionarySize(uncompressed_size);
        }

        // Decompress dictionary page if needed
        const dict_data = if (codec != .uncompressed) blk: {
            break :blk decompressData(self.allocator, dict_page_body, codec, uncompressed_size) catch
                return error.DecompressionError;
        } else dict_page_body;
        defer if (codec != .uncompressed) self.allocator.free(dict_data);

        // Build appropriate dictionary based on physical type
        if (physical_type) |pt| {
            switch (pt) {
                .byte_array => {
                    self.string_dict = dictionary.StringDictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                .fixed_len_byte_array => {
                    const fixed_len = safe.cast(type_length orelse 0) catch return error.IntegerOverflow;
                    if (fixed_len > 0) {
                        self.fixed_byte_array_dict = dictionary.FixedByteArrayDictionary.fromPlain(
                            self.allocator,
                            dict_data,
                            num_values,
                            fixed_len,
                        ) catch return error.OutOfMemory;
                    }
                },
                .int32 => {
                    self.int32_dict = dictionary.Int32Dictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                .int64 => {
                    self.int64_dict = dictionary.Int64Dictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                .float => {
                    self.float32_dict = dictionary.Float32Dictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                .double => {
                    self.float64_dict = dictionary.Float64Dictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                .int96 => {
                    self.int96_dict = dictionary.Int96Dictionary.fromPlain(
                        self.allocator,
                        dict_data,
                        num_values,
                    ) catch return error.OutOfMemory;
                },
                else => {},
            }
        }
    }

    /// Free all dictionaries
    pub fn deinit(self: *Self) void {
        if (self.string_dict) |*d| d.deinit();
        if (self.int32_dict) |*d| d.deinit();
        if (self.int64_dict) |*d| d.deinit();
        if (self.float32_dict) |*d| d.deinit();
        if (self.float64_dict) |*d| d.deinit();
        if (self.int96_dict) |*d| d.deinit();
        if (self.fixed_byte_array_dict) |*d| d.deinit();
    }
};

/// Decompress data using the specified codec
fn decompressData(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    codec: format.CompressionCodec,
    uncompressed_size: usize,
) ![]u8 {
    if (codec == .uncompressed) return error.InvalidCompressionState;
    return compress.decompress(allocator, compressed, codec, uncompressed_size);
}
