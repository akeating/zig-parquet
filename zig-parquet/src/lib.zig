//! Zig Parquet - A pure Zig implementation of Apache Parquet
//!
//! This library provides reading and writing of Parquet files.
//!
//! ## Reading Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! var reader = try parquet.openFile(allocator, file);
//! defer reader.deinit();
//!
//! const values = try reader.readColumn(0, i32);
//! defer allocator.free(values);
//! ```
//!
//! ## Writing Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! var writer = try parquet.writeToFile(allocator, file, &.{
//!     .{ .name = "id", .type_ = .int64, .optional = true },
//! });
//! defer writer.deinit();
//!
//! try writer.writeColumnOptional(i64, 0, &[_]parquet.Optional(i64){
//!     .{ .value = 1 }, .{ .value = 2 }, .{ .null_value = {} },
//! });
//! try writer.close();
//! ```

const std = @import("std");
const builtin = @import("builtin");
const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

// ============================================================================
// Primary API — Start here
// ============================================================================

// Convenience constructors: the simplest way to read/write Parquet.
//
//   Reading:
//     parquet.openFile(allocator, file)                        -> Reader
//     parquet.openBuffer(allocator, data)                      -> Reader
//     parquet.openFileRowReader(T, allocator, file, opts)      -> RowReader(T)
//     parquet.openBufferRowReader(T, allocator, data, opts)    -> RowReader(T)
//     parquet.openFileDynamic(allocator, file, opts)           -> DynamicReader
//     parquet.openBufferDynamic(allocator, data, opts)         -> DynamicReader
//
//   Writing:
//     parquet.writeToFile(allocator, file, columns)            -> Writer
//     parquet.writeToBuffer(allocator, columns)                -> Writer
//     parquet.writeToFileRows(T, allocator, file, opts)        -> RowWriter(T)
//     parquet.writeToBufferRows(T, allocator, opts)            -> RowWriter(T)
//
// Advanced — transport-neutral constructors on core types:
//     Reader.initFromSeekable(allocator, seekable)
//     Writer.initWithTarget(allocator, target, columns)
//     RowReader(T).initFromSeekable(allocator, seekable, opts)
//     RowWriter(T).initWithTarget(allocator, target, opts)
//     DynamicReader.initFromSeekable(allocator, seekable, opts)
//
// Internals — low-level codec/encoding/format access:
//     parquet.internals.*

// Core types
pub const types = @import("core/types.zig");
pub const Optional = types.Optional;
pub const ReaderError = types.ReaderError;
pub const WriterError = types.WriterError;

// Safety utilities (hardening against malformed input)
pub const safe = @import("core/safe.zig");

// Schema and Value types for nested composition
pub const schema = @import("core/schema.zig");
pub const SchemaNode = schema.SchemaNode;
pub const value = @import("core/value.zig");
pub const Value = value.Value;
pub const nested = @import("core/nested.zig");

// Arrow types for zero-copy interop
pub const arrow = @import("core/arrow.zig");
pub const ArrowSchema = arrow.ArrowSchema;
pub const ArrowArray = arrow.ArrowArray;
pub const ArrowColumn = arrow.ArrowColumn;
pub const ArrowArrayStream = arrow.ArrowArrayStream;

// Arrow batch API (runtime type dispatch for row group I/O)
pub const arrow_batch = @import("core/arrow_batch.zig");
pub const exportSchemaAsArrow = arrow_batch.exportSchemaAsArrow;
pub const importSchemaFromArrow = arrow_batch.importSchemaFromArrow;
pub const readRowGroupAsArrow = arrow_batch.readRowGroupAsArrow;
pub const writeRowGroupFromArrow = arrow_batch.writeRowGroupFromArrow;

// Reader (core type)
const reader_mod = @import("core/reader.zig");
pub const Reader = reader_mod.Reader;
pub const ListColumn = reader_mod.ListColumn;
pub const MapColumn = reader_mod.MapColumn;
/// Column-level reader MapEntry (uses Optional(V) for values).
/// For RowWriter/RowReader map fields, use the top-level MapEntry instead.
pub const ColumnMapEntry = reader_mod.MapEntry;
/// Map entry type for RowWriter/RowReader struct fields.
pub const MapEntry = types.MapEntry;

// Writer (core type)
const writer_mod = @import("core/writer.zig");
pub const Writer = writer_mod.Writer;
pub const ColumnDef = writer_mod.ColumnDef;
pub const StructField = writer_mod.StructField;

// Write target interface (core)
const write_target_mod = @import("core/write_target.zig");
pub const WriteTarget = write_target_mod.WriteTarget;
pub const WriteError = write_target_mod.WriteError;
pub const WriteTargetWriter = write_target_mod.WriteTargetWriter;

// Format types (Thrift-decoded Parquet structures)
pub const format = @import("core/format.zig");

// Row Writer / Row Reader (high-level row-oriented APIs)
const reader_module = @import("core/reader_mod.zig");
const writer_module = @import("core/writer_mod.zig");
pub const RowWriter = writer_module.RowWriter;
pub const RowWriterOptions = writer_module.RowWriterOptions;
pub const RowReader = reader_module.RowReader;
pub const RowReaderOptions = reader_module.RowReaderOptions;
pub const RowReaderError = reader_module.RowReaderError;

// Dynamic Reader (schema-agnostic row reading)
pub const DynamicReader = reader_module.DynamicReader;
pub const DynamicReaderError = reader_module.DynamicReaderError;
pub const DynamicReaderOptions = reader_module.DynamicReaderOptions;
pub const ChecksumOptions = reader_module.ChecksumOptions;
pub const Row = value.Row;

// SeekableReader interface (core)
pub const SeekableReader = reader_module.SeekableReader;
pub const BackendCleanup = reader_module.BackendCleanup;

// IO adapters (transport layer)
pub const io = struct {
    pub const FileReader = @import("io/file_reader.zig").FileReader;
    pub const BufferReader = @import("io/buffer_reader.zig").BufferReader;
    pub const CallbackReader = @import("io/callback_reader.zig").CallbackReader;
    pub const FileTarget = @import("io/file_target.zig").FileTarget;
    pub const BufferTarget = @import("io/buffer_target.zig").BufferTarget;
    pub const CallbackWriter = @import("io/callback_writer.zig").CallbackWriter;
};

// Convenience constructors (api/zig layer)
const api_reader = @import("api/zig/reader.zig");
const api_writer = @import("api/zig/writer.zig");

pub const openFile = api_reader.openFile;
pub const openBuffer = api_reader.openBuffer;
pub const openFileDynamic = api_reader.openFileDynamic;
pub const openBufferDynamic = api_reader.openBufferDynamic;
pub const openFileRowReader = api_reader.openFileRowReader;
pub const openBufferRowReader = api_reader.openBufferRowReader;
pub const writeToFile = api_writer.writeToFile;
pub const writeToBuffer = api_writer.writeToBuffer;
pub const writeToFileRows = api_writer.writeToFileRows;
pub const writeToBufferRows = api_writer.writeToBufferRows;

// Logical type wrappers for RowWriter/RowReader
pub const Timestamp = writer_module.row_writer.Timestamp;
pub const TimestampMicros = writer_module.row_writer.TimestampMicros;
pub const TimestampInt96 = writer_module.row_writer.TimestampInt96;
pub const Int96 = writer_module.row_writer.Int96;
pub const Date = writer_module.row_writer.Date;
pub const Uuid = writer_module.row_writer.Uuid;
pub const Interval = writer_module.row_writer.Interval;
pub const Decimal = types.Decimal;
pub const DecimalValue = types.DecimalValue;
pub const Time = writer_module.row_writer.Time;
pub const TimeMicros = writer_module.row_writer.TimeMicros;
pub const TimeUnit = writer_module.row_writer.TimeUnit;

// ============================================================================
// Internals — Low-level modules for advanced usage
// ============================================================================

pub const internals = struct {
    pub const thrift = @import("core/thrift/mod.zig");
    pub const compress = @import("core/compress/mod.zig");
    pub const reader = @import("core/reader_mod.zig");
    pub const column_decoder = reader.column_decoder;
    pub const encoding = struct {
        pub const plain = @import("core/encoding/plain.zig");
        pub const plain_encoder = @import("core/encoding/plain_encoder.zig");
        pub const rle = @import("core/encoding/rle.zig");
        pub const rle_encoder = @import("core/encoding/rle_encoder.zig");
        pub const dictionary = @import("core/encoding/dictionary.zig");
        pub const delta_binary_packed = @import("core/encoding/delta_binary_packed.zig");
        pub const delta_length_byte_array = @import("core/encoding/delta_length_byte_array.zig");
        pub const delta_byte_array = @import("core/encoding/delta_byte_array.zig");
        pub const byte_stream_split = @import("core/encoding/byte_stream_split.zig");
        pub const byte_stream_split_encoder = @import("core/encoding/byte_stream_split_encoder.zig");
        pub const delta_binary_packed_encoder = @import("core/encoding/delta_binary_packed_encoder.zig");
        pub const delta_length_byte_array_encoder = @import("core/encoding/delta_length_byte_array_encoder.zig");
        pub const delta_byte_array_encoder = @import("core/encoding/delta_byte_array_encoder.zig");
    };
    pub const writer = @import("core/writer_mod.zig");
    pub const geo = @import("core/geo/mod.zig");
};

// ============================================================================
// API surface layers (C ABI and WASM)
// ============================================================================

// C ABI surface -- comptime block ensures export fn symbols are compiled into
// shared libraries, since Zig's lazy analysis won't reach them otherwise.
// Skipped on WASM targets where callconv(.c) is not supported.
pub const c_api = if (!is_wasm) @import("api/c/mod.zig") else struct {
    pub const err = struct {};
    pub const handles = struct {};
    pub const reader = struct {};
    pub const writer = struct {};
    pub const row_reader = struct {};
    pub const row_writer = struct {};
    pub const introspect = struct {};
};
comptime {
    if (!is_wasm) {
        _ = &c_api.reader.zp_reader_open_memory;
        _ = &c_api.reader.zp_reader_open_callbacks;
        _ = &c_api.reader.zp_reader_open_file;
        _ = &c_api.reader.zp_reader_get_num_row_groups;
        _ = &c_api.reader.zp_reader_get_num_rows;
        _ = &c_api.reader.zp_reader_get_row_group_num_rows;
        _ = &c_api.reader.zp_reader_get_column_count;
        _ = &c_api.reader.zp_reader_get_schema;
        _ = &c_api.reader.zp_reader_read_row_group;
        _ = &c_api.reader.zp_reader_error_code;
        _ = &c_api.reader.zp_reader_error_message;
        _ = &c_api.reader.zp_reader_get_stream;
        _ = &c_api.reader.zp_reader_has_statistics;
        _ = &c_api.reader.zp_reader_get_null_count;
        _ = &c_api.reader.zp_reader_get_distinct_count;
        _ = &c_api.reader.zp_reader_get_min_value;
        _ = &c_api.reader.zp_reader_get_max_value;
        _ = &c_api.reader.zp_reader_get_kv_metadata_count;
        _ = &c_api.reader.zp_reader_get_kv_metadata_key;
        _ = &c_api.reader.zp_reader_get_kv_metadata_value;
        _ = &c_api.reader.zp_reader_close;
        _ = &c_api.writer.zp_writer_open_memory;
        _ = &c_api.writer.zp_writer_open_callbacks;
        _ = &c_api.writer.zp_writer_open_file;
        _ = &c_api.writer.zp_writer_set_schema;
        _ = &c_api.writer.zp_writer_set_column_codec;
        _ = &c_api.writer.zp_writer_set_row_group_size;
        _ = &c_api.writer.zp_writer_write_row_group;
        _ = &c_api.writer.zp_writer_get_buffer;
        _ = &c_api.writer.zp_writer_error_code;
        _ = &c_api.writer.zp_writer_error_message;
        _ = &c_api.writer.zp_writer_close;
        _ = &c_api.writer.zp_writer_free;
        _ = &c_api.row_reader.zp_row_reader_open_memory;
        _ = &c_api.row_reader.zp_row_reader_open_callbacks;
        _ = &c_api.row_reader.zp_row_reader_open_file;
        _ = &c_api.row_reader.zp_row_reader_get_num_row_groups;
        _ = &c_api.row_reader.zp_row_reader_get_num_rows;
        _ = &c_api.row_reader.zp_row_reader_get_column_count;
        _ = &c_api.row_reader.zp_row_reader_get_column_name;
        _ = &c_api.row_reader.zp_row_reader_read_row_group;
        _ = &c_api.row_reader.zp_row_reader_next;
        _ = &c_api.row_reader.zp_row_reader_get_type;
        _ = &c_api.row_reader.zp_row_reader_is_null;
        _ = &c_api.row_reader.zp_row_reader_get_int32;
        _ = &c_api.row_reader.zp_row_reader_get_int64;
        _ = &c_api.row_reader.zp_row_reader_get_float;
        _ = &c_api.row_reader.zp_row_reader_get_double;
        _ = &c_api.row_reader.zp_row_reader_get_bool;
        _ = &c_api.row_reader.zp_row_reader_get_bytes;
        _ = &c_api.row_reader.zp_row_reader_get_column_type;
        _ = &c_api.row_reader.zp_row_reader_get_decimal_precision;
        _ = &c_api.row_reader.zp_row_reader_get_decimal_scale;
        _ = &c_api.row_reader.zp_row_reader_get_kv_metadata_count;
        _ = &c_api.row_reader.zp_row_reader_get_kv_metadata_key;
        _ = &c_api.row_reader.zp_row_reader_get_kv_metadata_value;
        _ = &c_api.row_reader.zp_row_reader_set_checksum_validation;
        _ = &c_api.row_reader.zp_row_reader_get_value;
        _ = &c_api.row_reader.zp_value_get_type;
        _ = &c_api.row_reader.zp_value_is_null;
        _ = &c_api.row_reader.zp_value_get_int32;
        _ = &c_api.row_reader.zp_value_get_int64;
        _ = &c_api.row_reader.zp_value_get_float;
        _ = &c_api.row_reader.zp_value_get_double;
        _ = &c_api.row_reader.zp_value_get_bool;
        _ = &c_api.row_reader.zp_value_get_bytes;
        _ = &c_api.row_reader.zp_value_get_list_len;
        _ = &c_api.row_reader.zp_value_get_list_element;
        _ = &c_api.row_reader.zp_value_get_map_len;
        _ = &c_api.row_reader.zp_value_get_map_key;
        _ = &c_api.row_reader.zp_value_get_map_value;
        _ = &c_api.row_reader.zp_value_get_struct_field_count;
        _ = &c_api.row_reader.zp_value_get_struct_field_name;
        _ = &c_api.row_reader.zp_value_get_struct_field_value;
        _ = &c_api.row_reader.zp_row_reader_error_code;
        _ = &c_api.row_reader.zp_row_reader_error_message;
        _ = &c_api.row_reader.zp_row_reader_close;
        _ = &c_api.row_writer.zp_row_writer_open_memory;
        _ = &c_api.row_writer.zp_row_writer_open_callbacks;
        _ = &c_api.row_writer.zp_row_writer_open_file;
        _ = &c_api.row_writer.zp_row_writer_add_column;
        _ = &c_api.row_writer.zp_row_writer_add_column_decimal;
        _ = &c_api.row_writer.zp_row_writer_add_column_geometry;
        _ = &c_api.row_writer.zp_row_writer_add_column_geography;
        _ = &c_api.row_writer.zp_row_writer_set_compression;
        _ = &c_api.row_writer.zp_row_writer_begin;
        _ = &c_api.row_writer.zp_row_writer_set_null;
        _ = &c_api.row_writer.zp_row_writer_set_bool;
        _ = &c_api.row_writer.zp_row_writer_set_int32;
        _ = &c_api.row_writer.zp_row_writer_set_int64;
        _ = &c_api.row_writer.zp_row_writer_set_float;
        _ = &c_api.row_writer.zp_row_writer_set_double;
        _ = &c_api.row_writer.zp_row_writer_set_bytes;
        _ = &c_api.row_writer.zp_row_writer_add_row;
        _ = &c_api.row_writer.zp_row_writer_flush;
        _ = &c_api.row_writer.zp_row_writer_close;
        _ = &c_api.row_writer.zp_row_writer_get_buffer;
        _ = &c_api.row_writer.zp_row_writer_set_column_codec;
        _ = &c_api.row_writer.zp_row_writer_set_row_group_size;
        _ = &c_api.row_writer.zp_row_writer_set_kv_metadata;
        _ = &c_api.row_writer.zp_schema_primitive;
        _ = &c_api.row_writer.zp_schema_decimal;
        _ = &c_api.row_writer.zp_schema_optional;
        _ = &c_api.row_writer.zp_schema_list;
        _ = &c_api.row_writer.zp_schema_map;
        _ = &c_api.row_writer.zp_schema_struct;
        _ = &c_api.row_writer.zp_row_writer_add_column_nested;
        _ = &c_api.row_writer.zp_row_writer_begin_list;
        _ = &c_api.row_writer.zp_row_writer_end_list;
        _ = &c_api.row_writer.zp_row_writer_append_null;
        _ = &c_api.row_writer.zp_row_writer_append_bool;
        _ = &c_api.row_writer.zp_row_writer_append_int32;
        _ = &c_api.row_writer.zp_row_writer_append_int64;
        _ = &c_api.row_writer.zp_row_writer_append_float;
        _ = &c_api.row_writer.zp_row_writer_append_double;
        _ = &c_api.row_writer.zp_row_writer_append_bytes;
        _ = &c_api.row_writer.zp_row_writer_begin_struct;
        _ = &c_api.row_writer.zp_row_writer_set_field_null;
        _ = &c_api.row_writer.zp_row_writer_set_field_int32;
        _ = &c_api.row_writer.zp_row_writer_set_field_int64;
        _ = &c_api.row_writer.zp_row_writer_set_field_float;
        _ = &c_api.row_writer.zp_row_writer_set_field_double;
        _ = &c_api.row_writer.zp_row_writer_set_field_bool;
        _ = &c_api.row_writer.zp_row_writer_set_field_bytes;
        _ = &c_api.row_writer.zp_row_writer_end_struct;
        _ = &c_api.row_writer.zp_row_writer_begin_map;
        _ = &c_api.row_writer.zp_row_writer_begin_map_entry;
        _ = &c_api.row_writer.zp_row_writer_end_map_entry;
        _ = &c_api.row_writer.zp_row_writer_end_map;
        _ = &c_api.row_writer.zp_row_writer_error_code;
        _ = &c_api.row_writer.zp_row_writer_error_message;
        _ = &c_api.row_writer.zp_row_writer_free;
        _ = &c_api.introspect.getVersion;
        _ = &c_api.introspect.isCodecSupported;
    }
}

// WASM API surface -- only compiled on WASM targets.
// On WASM, export fn symbols replace the C ABI callconv(.c) exports.
pub const wasm_api = if (is_wasm) @import("api/wasm/mod.zig") else struct {
    pub const wasi = struct {};
    pub const freestanding = struct {};
    pub const handles = struct {};
    pub const err = struct {};
};
comptime {
    if (is_wasm) {
        const wasi = wasm_api.wasi;
        const freestanding = wasm_api.freestanding;

        // WASI exports (active on wasm32-wasi)
        if (@TypeOf(wasi) != type) {
            _ = &wasi.zp_reader_open_memory;
            _ = &wasi.zp_reader_open_callbacks;
            _ = &wasi.zp_reader_get_num_row_groups;
            _ = &wasi.zp_reader_get_num_rows;
            _ = &wasi.zp_reader_get_row_group_num_rows;
            _ = &wasi.zp_reader_get_column_count;
            _ = &wasi.zp_reader_get_schema;
            _ = &wasi.zp_reader_read_row_group;
            _ = &wasi.zp_reader_get_stream;
            _ = &wasi.zp_reader_error_code;
            _ = &wasi.zp_reader_error_message;
            _ = &wasi.zp_reader_has_statistics;
            _ = &wasi.zp_reader_get_null_count;
            _ = &wasi.zp_reader_get_distinct_count;
            _ = &wasi.zp_reader_get_min_value;
            _ = &wasi.zp_reader_get_max_value;
            _ = &wasi.zp_reader_get_kv_metadata_count;
            _ = &wasi.zp_reader_get_kv_metadata_key;
            _ = &wasi.zp_reader_get_kv_metadata_value;
            _ = &wasi.zp_reader_close;
            _ = &wasi.zp_writer_open_memory;
            _ = &wasi.zp_writer_open_callbacks;
            _ = &wasi.zp_writer_set_schema;
            _ = &wasi.zp_writer_set_column_codec;
            _ = &wasi.zp_writer_set_row_group_size;
            _ = &wasi.zp_writer_write_row_group;
            _ = &wasi.zp_writer_get_buffer;
            _ = &wasi.zp_writer_error_code;
            _ = &wasi.zp_writer_error_message;
            _ = &wasi.zp_writer_close;
            _ = &wasi.zp_writer_free;
            _ = &wasi.zp_version;
            _ = &wasi.zp_codec_supported;
            _ = &wasi.zp_row_reader_open_memory;
            _ = &wasi.zp_row_reader_open_callbacks;
            _ = &wasi.zp_row_reader_get_num_row_groups;
            _ = &wasi.zp_row_reader_get_num_rows;
            _ = &wasi.zp_row_reader_get_column_count;
            _ = &wasi.zp_row_reader_get_column_name;
            _ = &wasi.zp_row_reader_read_row_group;
            _ = &wasi.zp_row_reader_next;
            _ = &wasi.zp_row_reader_get_type;
            _ = &wasi.zp_row_reader_is_null;
            _ = &wasi.zp_row_reader_get_int32;
            _ = &wasi.zp_row_reader_get_int64;
            _ = &wasi.zp_row_reader_get_float;
            _ = &wasi.zp_row_reader_get_double;
            _ = &wasi.zp_row_reader_get_bool;
            _ = &wasi.zp_row_reader_get_bytes;
            _ = &wasi.zp_row_reader_get_column_type;
            _ = &wasi.zp_row_reader_get_decimal_precision;
            _ = &wasi.zp_row_reader_get_decimal_scale;
            _ = &wasi.zp_row_reader_get_kv_metadata_count;
            _ = &wasi.zp_row_reader_get_kv_metadata_key;
            _ = &wasi.zp_row_reader_get_kv_metadata_value;
            _ = &wasi.zp_row_reader_set_checksum_validation;
            _ = &wasi.zp_row_reader_error_code;
            _ = &wasi.zp_row_reader_error_message;
            _ = &wasi.zp_row_reader_get_value;
            _ = &wasi.zp_row_reader_close;
            _ = &wasi.zp_value_get_type;
            _ = &wasi.zp_value_is_null;
            _ = &wasi.zp_value_get_int32;
            _ = &wasi.zp_value_get_int64;
            _ = &wasi.zp_value_get_float;
            _ = &wasi.zp_value_get_double;
            _ = &wasi.zp_value_get_bool;
            _ = &wasi.zp_value_get_bytes;
            _ = &wasi.zp_value_get_list_len;
            _ = &wasi.zp_value_get_list_element;
            _ = &wasi.zp_value_get_map_len;
            _ = &wasi.zp_value_get_map_key;
            _ = &wasi.zp_value_get_map_value;
            _ = &wasi.zp_value_get_struct_field_count;
            _ = &wasi.zp_value_get_struct_field_name;
            _ = &wasi.zp_value_get_struct_field_value;
            _ = &wasi.zp_row_writer_open_memory;
            _ = &wasi.zp_row_writer_open_callbacks;
            _ = &wasi.zp_row_writer_add_column;
            _ = &wasi.zp_row_writer_add_column_decimal;
            _ = &wasi.zp_row_writer_add_column_geometry;
            _ = &wasi.zp_row_writer_add_column_geography;
            _ = &wasi.zp_row_writer_set_compression;
            _ = &wasi.zp_row_writer_set_column_codec;
            _ = &wasi.zp_row_writer_set_row_group_size;
            _ = &wasi.zp_row_writer_set_kv_metadata;
            _ = &wasi.zp_schema_primitive;
            _ = &wasi.zp_schema_decimal;
            _ = &wasi.zp_schema_optional;
            _ = &wasi.zp_schema_list;
            _ = &wasi.zp_schema_map;
            _ = &wasi.zp_schema_struct;
            _ = &wasi.zp_row_writer_add_column_nested;
            _ = &wasi.zp_row_writer_begin;
            _ = &wasi.zp_row_writer_set_null;
            _ = &wasi.zp_row_writer_set_bool;
            _ = &wasi.zp_row_writer_set_int32;
            _ = &wasi.zp_row_writer_set_int64;
            _ = &wasi.zp_row_writer_set_float;
            _ = &wasi.zp_row_writer_set_double;
            _ = &wasi.zp_row_writer_set_bytes;
            _ = &wasi.zp_row_writer_add_row;
            _ = &wasi.zp_row_writer_flush;
            _ = &wasi.zp_row_writer_close;
            _ = &wasi.zp_row_writer_get_buffer;
            _ = &wasi.zp_row_writer_begin_list;
            _ = &wasi.zp_row_writer_end_list;
            _ = &wasi.zp_row_writer_append_null;
            _ = &wasi.zp_row_writer_append_bool;
            _ = &wasi.zp_row_writer_append_int32;
            _ = &wasi.zp_row_writer_append_int64;
            _ = &wasi.zp_row_writer_append_float;
            _ = &wasi.zp_row_writer_append_double;
            _ = &wasi.zp_row_writer_append_bytes;
            _ = &wasi.zp_row_writer_begin_struct;
            _ = &wasi.zp_row_writer_set_field_null;
            _ = &wasi.zp_row_writer_set_field_int32;
            _ = &wasi.zp_row_writer_set_field_int64;
            _ = &wasi.zp_row_writer_set_field_float;
            _ = &wasi.zp_row_writer_set_field_double;
            _ = &wasi.zp_row_writer_set_field_bool;
            _ = &wasi.zp_row_writer_set_field_bytes;
            _ = &wasi.zp_row_writer_end_struct;
            _ = &wasi.zp_row_writer_begin_map;
            _ = &wasi.zp_row_writer_begin_map_entry;
            _ = &wasi.zp_row_writer_end_map_entry;
            _ = &wasi.zp_row_writer_end_map;
            _ = &wasi.zp_row_writer_error_code;
            _ = &wasi.zp_row_writer_error_message;
            _ = &wasi.zp_row_writer_free;
        }

        // Freestanding exports (active on wasm32-freestanding)
        if (@TypeOf(freestanding) != type) {
            _ = &freestanding.zp_alloc;
            _ = &freestanding.zp_free;
            _ = &freestanding.zp_reader_open_buffer;
            _ = &freestanding.zp_reader_open_host;
            _ = &freestanding.zp_reader_get_num_row_groups;
            _ = &freestanding.zp_reader_get_num_rows;
            _ = &freestanding.zp_reader_get_row_group_num_rows;
            _ = &freestanding.zp_reader_get_column_count;
            _ = &freestanding.zp_reader_get_schema;
            _ = &freestanding.zp_reader_read_row_group;
            _ = &freestanding.zp_reader_error_code;
            _ = &freestanding.zp_reader_error_message;
            _ = &freestanding.zp_reader_has_statistics;
            _ = &freestanding.zp_reader_get_null_count;
            _ = &freestanding.zp_reader_get_distinct_count;
            _ = &freestanding.zp_reader_get_min_value;
            _ = &freestanding.zp_reader_get_max_value;
            _ = &freestanding.zp_reader_get_kv_metadata_count;
            _ = &freestanding.zp_reader_get_kv_metadata_key;
            _ = &freestanding.zp_reader_get_kv_metadata_key_len;
            _ = &freestanding.zp_reader_get_kv_metadata_value;
            _ = &freestanding.zp_reader_get_kv_metadata_value_len;
            _ = &freestanding.zp_reader_close;
            _ = &freestanding.zp_writer_open_buffer;
            _ = &freestanding.zp_writer_open_host;
            _ = &freestanding.zp_writer_set_schema;
            _ = &freestanding.zp_writer_set_column_codec;
            _ = &freestanding.zp_writer_set_row_group_size;
            _ = &freestanding.zp_writer_write_row_group;
            _ = &freestanding.zp_writer_get_buffer;
            _ = &freestanding.zp_writer_error_code;
            _ = &freestanding.zp_writer_error_message;
            _ = &freestanding.zp_writer_close;
            _ = &freestanding.zp_writer_free;
            _ = &freestanding.zp_version;
            _ = &freestanding.zp_codec_supported;
            _ = &freestanding.zp_reader_stream_init;
            _ = &freestanding.zp_reader_stream_get_schema;
            _ = &freestanding.zp_reader_stream_get_next;
            _ = &freestanding.zp_reader_stream_close;
            _ = &freestanding.zp_arrow_array_get_length;
            _ = &freestanding.zp_arrow_array_get_null_count;
            _ = &freestanding.zp_arrow_array_get_n_children;
            _ = &freestanding.zp_arrow_array_get_n_buffers;
            _ = &freestanding.zp_arrow_array_get_child;
            _ = &freestanding.zp_arrow_array_get_buffer;
            _ = &freestanding.zp_arrow_schema_get_format;
            _ = &freestanding.zp_arrow_schema_get_name;
            _ = &freestanding.zp_arrow_schema_get_n_children;
            _ = &freestanding.zp_arrow_schema_get_child;
            _ = &freestanding.zp_row_reader_open_buffer;
            _ = &freestanding.zp_row_reader_open_host;
            _ = &freestanding.zp_row_reader_get_num_row_groups;
            _ = &freestanding.zp_row_reader_get_num_rows;
            _ = &freestanding.zp_row_reader_get_column_count;
            _ = &freestanding.zp_row_reader_get_column_name;
            _ = &freestanding.zp_row_reader_read_row_group;
            _ = &freestanding.zp_row_reader_next;
            _ = &freestanding.zp_row_reader_get_type;
            _ = &freestanding.zp_row_reader_is_null;
            _ = &freestanding.zp_row_reader_get_int32;
            _ = &freestanding.zp_row_reader_get_int64;
            _ = &freestanding.zp_row_reader_get_float;
            _ = &freestanding.zp_row_reader_get_double;
            _ = &freestanding.zp_row_reader_get_bool;
            _ = &freestanding.zp_row_reader_get_bytes_ptr;
            _ = &freestanding.zp_row_reader_get_bytes_len;
            _ = &freestanding.zp_row_reader_get_column_type;
            _ = &freestanding.zp_row_reader_get_decimal_precision;
            _ = &freestanding.zp_row_reader_get_decimal_scale;
            _ = &freestanding.zp_row_reader_get_kv_metadata_count;
            _ = &freestanding.zp_row_reader_get_kv_metadata_key;
            _ = &freestanding.zp_row_reader_get_kv_metadata_key_len;
            _ = &freestanding.zp_row_reader_get_kv_metadata_value;
            _ = &freestanding.zp_row_reader_get_kv_metadata_value_len;
            _ = &freestanding.zp_row_reader_set_checksum_validation;
            _ = &freestanding.zp_row_reader_error_code;
            _ = &freestanding.zp_row_reader_error_message;
            _ = &freestanding.zp_row_reader_get_value;
            _ = &freestanding.zp_row_reader_close;
            _ = &freestanding.zp_value_get_type;
            _ = &freestanding.zp_value_is_null;
            _ = &freestanding.zp_value_get_int32;
            _ = &freestanding.zp_value_get_int64;
            _ = &freestanding.zp_value_get_float;
            _ = &freestanding.zp_value_get_double;
            _ = &freestanding.zp_value_get_bool;
            _ = &freestanding.zp_value_get_bytes_ptr;
            _ = &freestanding.zp_value_get_bytes_len;
            _ = &freestanding.zp_value_get_list_len;
            _ = &freestanding.zp_value_get_list_element;
            _ = &freestanding.zp_value_get_map_len;
            _ = &freestanding.zp_value_get_map_key;
            _ = &freestanding.zp_value_get_map_value;
            _ = &freestanding.zp_value_get_struct_field_count;
            _ = &freestanding.zp_value_get_struct_field_name_ptr;
            _ = &freestanding.zp_value_get_struct_field_name_len;
            _ = &freestanding.zp_value_get_struct_field_value;
            _ = &freestanding.zp_row_writer_open_buffer;
            _ = &freestanding.zp_row_writer_open_host;
            _ = &freestanding.zp_row_writer_add_column;
            _ = &freestanding.zp_row_writer_add_column_decimal;
            _ = &freestanding.zp_row_writer_add_column_geometry;
            _ = &freestanding.zp_row_writer_add_column_geography;
            _ = &freestanding.zp_row_writer_set_compression;
            _ = &freestanding.zp_row_writer_set_column_codec;
            _ = &freestanding.zp_row_writer_set_row_group_size;
            _ = &freestanding.zp_row_writer_set_kv_metadata;
            _ = &freestanding.zp_schema_primitive;
            _ = &freestanding.zp_schema_decimal;
            _ = &freestanding.zp_schema_optional;
            _ = &freestanding.zp_schema_list;
            _ = &freestanding.zp_schema_map;
            _ = &freestanding.zp_schema_struct;
            _ = &freestanding.zp_row_writer_add_column_nested;
            _ = &freestanding.zp_row_writer_begin;
            _ = &freestanding.zp_row_writer_set_null;
            _ = &freestanding.zp_row_writer_set_bool;
            _ = &freestanding.zp_row_writer_set_int32;
            _ = &freestanding.zp_row_writer_set_int64;
            _ = &freestanding.zp_row_writer_set_float;
            _ = &freestanding.zp_row_writer_set_double;
            _ = &freestanding.zp_row_writer_set_bytes;
            _ = &freestanding.zp_row_writer_add_row;
            _ = &freestanding.zp_row_writer_flush;
            _ = &freestanding.zp_row_writer_close;
            _ = &freestanding.zp_row_writer_get_buffer;
            _ = &freestanding.zp_row_writer_begin_list;
            _ = &freestanding.zp_row_writer_end_list;
            _ = &freestanding.zp_row_writer_append_null;
            _ = &freestanding.zp_row_writer_append_bool;
            _ = &freestanding.zp_row_writer_append_int32;
            _ = &freestanding.zp_row_writer_append_int64;
            _ = &freestanding.zp_row_writer_append_float;
            _ = &freestanding.zp_row_writer_append_double;
            _ = &freestanding.zp_row_writer_append_bytes;
            _ = &freestanding.zp_row_writer_begin_struct;
            _ = &freestanding.zp_row_writer_set_field_null;
            _ = &freestanding.zp_row_writer_set_field_int32;
            _ = &freestanding.zp_row_writer_set_field_int64;
            _ = &freestanding.zp_row_writer_set_field_float;
            _ = &freestanding.zp_row_writer_set_field_double;
            _ = &freestanding.zp_row_writer_set_field_bool;
            _ = &freestanding.zp_row_writer_set_field_bytes;
            _ = &freestanding.zp_row_writer_end_struct;
            _ = &freestanding.zp_row_writer_begin_map;
            _ = &freestanding.zp_row_writer_begin_map_entry;
            _ = &freestanding.zp_row_writer_end_map_entry;
            _ = &freestanding.zp_row_writer_end_map;
            _ = &freestanding.zp_row_writer_error_code;
            _ = &freestanding.zp_row_writer_error_message;
            _ = &freestanding.zp_row_writer_free;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

test {
    _ = safe;
    _ = types;
    _ = internals.thrift;
    _ = format;
    _ = internals.compress;
    _ = internals.column_decoder;
    _ = internals.encoding.plain;
    _ = internals.encoding.plain_encoder;
    _ = internals.encoding.rle;
    _ = internals.encoding.rle_encoder;
    _ = internals.encoding.dictionary;
    _ = internals.encoding.delta_binary_packed;
    _ = internals.encoding.delta_length_byte_array;
    _ = internals.encoding.delta_byte_array;
    _ = internals.encoding.byte_stream_split;
    _ = internals.encoding.byte_stream_split_encoder;
    _ = internals.encoding.delta_binary_packed_encoder;
    _ = internals.encoding.delta_length_byte_array_encoder;
    _ = internals.encoding.delta_byte_array_encoder;
    _ = @import("core/reader.zig");
    _ = internals.reader;
    _ = internals.reader.seekable_reader;
    _ = @import("core/writer.zig");
    _ = internals.writer;
    _ = schema;
    _ = value;
    _ = nested;
    _ = arrow_batch;
    _ = internals.geo;
    _ = api_reader;
    _ = api_writer;
    if (!is_wasm) {
        _ = @import("api/c/mod.zig");
    }

    _ = @import("tests/mod.zig");
}
