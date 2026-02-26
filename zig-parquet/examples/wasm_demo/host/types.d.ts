/**
 * TypeScript type definitions for the zig-parquet WASI WASM module.
 *
 * The WASI API mirrors the C ABI but uses WASM-compatible calling conventions.
 * All functions use opaque pointer handles (`number` in JS, representing
 * pointers in WASM linear memory).
 */

// ---------------------------------------------------------------------------
// Error codes (stable across library versions)
// ---------------------------------------------------------------------------

export declare const ZP_OK = 0;
export declare const ZP_ERROR_INVALID_DATA = 1;
export declare const ZP_ERROR_NOT_PARQUET = 2;
export declare const ZP_ERROR_IO = 3;
export declare const ZP_ERROR_OUT_OF_MEMORY = 4;
export declare const ZP_ERROR_UNSUPPORTED = 5;
export declare const ZP_ERROR_INVALID_ARGUMENT = 6;
export declare const ZP_ERROR_INVALID_STATE = 7;
export declare const ZP_ERROR_CHECKSUM = 8;
export declare const ZP_ERROR_SCHEMA = 9;
export declare const ZP_ERROR_HANDLE_LIMIT = 10;
export declare const ZP_ROW_END = 11;

// Physical type constants (0-9)
export declare const ZP_TYPE_NULL = 0;
export declare const ZP_TYPE_BOOL = 1;
export declare const ZP_TYPE_INT32 = 2;
export declare const ZP_TYPE_INT64 = 3;
export declare const ZP_TYPE_FLOAT = 4;
export declare const ZP_TYPE_DOUBLE = 5;
export declare const ZP_TYPE_BYTES = 6;
export declare const ZP_TYPE_LIST = 7;
export declare const ZP_TYPE_MAP = 8;
export declare const ZP_TYPE_STRUCT = 9;

// Logical type constants (10+)
export declare const ZP_TYPE_STRING = 10;
export declare const ZP_TYPE_DATE = 11;
export declare const ZP_TYPE_TIMESTAMP_MILLIS = 12;
export declare const ZP_TYPE_TIMESTAMP_MICROS = 13;
export declare const ZP_TYPE_TIMESTAMP_NANOS = 14;
export declare const ZP_TYPE_TIME_MILLIS = 15;
export declare const ZP_TYPE_TIME_MICROS = 16;
export declare const ZP_TYPE_TIME_NANOS = 17;
export declare const ZP_TYPE_INT8 = 18;
export declare const ZP_TYPE_INT16 = 19;
export declare const ZP_TYPE_UINT8 = 20;
export declare const ZP_TYPE_UINT16 = 21;
export declare const ZP_TYPE_UINT32 = 22;
export declare const ZP_TYPE_UINT64 = 23;
export declare const ZP_TYPE_UUID = 24;
export declare const ZP_TYPE_JSON = 25;
export declare const ZP_TYPE_ENUM = 26;
export declare const ZP_TYPE_DECIMAL = 27;
export declare const ZP_TYPE_FLOAT16 = 28;
export declare const ZP_TYPE_BSON = 29;
export declare const ZP_TYPE_INTERVAL = 30;
export declare const ZP_TYPE_GEOMETRY = 31;
export declare const ZP_TYPE_GEOGRAPHY = 32;

// Compression codec IDs
export declare const ZP_CODEC_UNCOMPRESSED = 0;
export declare const ZP_CODEC_SNAPPY = 1;
export declare const ZP_CODEC_GZIP = 2;
export declare const ZP_CODEC_LZ4_RAW = 4;
export declare const ZP_CODEC_BROTLI = 6;
export declare const ZP_CODEC_ZSTD = 7;

// ---------------------------------------------------------------------------
// Module exports
// ---------------------------------------------------------------------------

/** Opaque handle pointer (a WASM linear memory address). */
type HandlePtr = number;

/** Status code. 0 = ZP_OK, positive = error. */
type StatusCode = number;

export interface ParquetWasiExports {
  /** WASM linear memory. */
  memory: WebAssembly.Memory;

  // -- Row Reader (cursor-based, no Arrow required) --

  /**
   * Open a row reader from a buffer in WASM linear memory.
   * `handle_out_ptr` points to a pointer-sized location that receives the handle.
   */
  zp_row_reader_open_memory(
    data_ptr: number,
    len: number,
    handle_out_ptr: number,
  ): StatusCode;

  /**
   * Open a row reader using caller-provided random-access callbacks.
   * Callback function pointers must point to WASM table entries.
   */
  zp_row_reader_open_callbacks(
    ctx: HandlePtr,
    read_at_fn: number,
    size_fn: number,
    handle_out_ptr: number,
  ): StatusCode;

  /** Get number of row groups. Writes i32 to `count_out_ptr`. */
  zp_row_reader_get_num_row_groups(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;

  /** Get total row count. Writes i64 to `count_out_ptr`. */
  zp_row_reader_get_num_rows(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;

  /** Get top-level column count. Writes i32 to `count_out_ptr`. */
  zp_row_reader_get_column_count(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;

  /** Get pointer to null-terminated column name, or null. */
  zp_row_reader_get_column_name(
    handle: HandlePtr,
    col_index: number,
  ): number;

  /**
   * Load all rows for a row group into memory.
   * Must be called before next()/get_*.
   */
  zp_row_reader_read_row_group(
    handle: HandlePtr,
    rg_index: number,
  ): StatusCode;

  /**
   * Advance the cursor to the next row.
   * Returns ZP_OK (0) if a row is available, ZP_ROW_END (11) when exhausted.
   */
  zp_row_reader_next(handle: HandlePtr): StatusCode;

  /** Get the value type for a column in the current row (ZP_TYPE_* constant). */
  zp_row_reader_get_type(handle: HandlePtr, col_index: number): number;

  /** Returns 1 if the column value is null, 0 otherwise. */
  zp_row_reader_is_null(handle: HandlePtr, col_index: number): number;

  /** Get an int32 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_int32(handle: HandlePtr, col_index: number): number;

  /** Get an int64 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_int64(handle: HandlePtr, col_index: number): bigint;

  /** Get a float32 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_float(handle: HandlePtr, col_index: number): number;

  /** Get a float64 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_double(handle: HandlePtr, col_index: number): number;

  /** Get a boolean value as 0/1. Returns 0 on type mismatch or null. */
  zp_row_reader_get_bool(handle: HandlePtr, col_index: number): number;

  /**
   * Get bytes/string data. Writes data pointer and length to out-params.
   * Returns ZP_OK on success, ZP_ERROR_INVALID_STATE if value is null.
   */
  zp_row_reader_get_bytes(
    handle: HandlePtr,
    col_index: number,
    data_out_ptr: number,
    len_out_ptr: number,
  ): StatusCode;

  /** Get the schema-level type for a column (available before iterating rows). */
  zp_row_reader_get_column_type(handle: HandlePtr, col_index: number): number;

  /** Get decimal precision for a DECIMAL column. Returns 0 for non-decimal. */
  zp_row_reader_get_decimal_precision(handle: HandlePtr, col_index: number): number;

  /** Get decimal scale for a DECIMAL column. Returns 0 for non-decimal. */
  zp_row_reader_get_decimal_scale(handle: HandlePtr, col_index: number): number;

  /** Get most recent error code for this row reader. */
  zp_row_reader_error_code(handle: HandlePtr): StatusCode;

  /** Get pointer to null-terminated error message. */
  zp_row_reader_error_message(handle: HandlePtr): number;

  /** Close row reader and free all resources. */
  zp_row_reader_close(handle: HandlePtr): void;

  // -- Arrow Reader (advanced) --

  zp_reader_open_memory(
    data_ptr: number,
    len: number,
    handle_out_ptr: number,
  ): StatusCode;
  zp_reader_open_callbacks(
    ctx: HandlePtr,
    read_at_fn: number,
    size_fn: number,
    handle_out_ptr: number,
  ): StatusCode;
  zp_reader_get_num_row_groups(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_num_rows(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_row_group_num_rows(
    handle: HandlePtr,
    rg_index: number,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_column_count(
    handle: HandlePtr,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_schema(
    handle: HandlePtr,
    schema_out_ptr: number,
  ): StatusCode;
  zp_reader_read_row_group(
    handle: HandlePtr,
    rg_index: number,
    col_indices_ptr: number,
    num_cols: number,
    arrays_out_ptr: number,
    schema_out_ptr: number,
  ): StatusCode;
  zp_reader_get_stream(
    handle: HandlePtr,
    stream_out_ptr: number,
  ): StatusCode;
  zp_reader_error_code(handle: HandlePtr): StatusCode;
  zp_reader_error_message(handle: HandlePtr): number;
  zp_reader_close(handle: HandlePtr): void;

  // -- Writer --

  zp_writer_open_memory(handle_out_ptr: number): StatusCode;
  zp_writer_open_callbacks(
    ctx: HandlePtr,
    write_fn: number,
    close_fn: number,
    handle_out_ptr: number,
  ): StatusCode;
  zp_writer_set_schema(
    handle: HandlePtr,
    schema_ptr: number,
  ): StatusCode;
  zp_writer_set_column_codec(
    handle: HandlePtr,
    col_index: number,
    codec: number,
  ): StatusCode;
  zp_writer_set_row_group_size(
    handle: HandlePtr,
    size_bytes: bigint,
  ): StatusCode;
  zp_writer_write_row_group(
    handle: HandlePtr,
    batch_ptr: number,
    schema_ptr: number,
  ): StatusCode;
  zp_writer_get_buffer(
    handle: HandlePtr,
    data_out_ptr: number,
    len_out_ptr: number,
  ): StatusCode;
  zp_writer_error_code(handle: HandlePtr): StatusCode;
  zp_writer_error_message(handle: HandlePtr): number;
  zp_writer_close(handle: HandlePtr): StatusCode;
  zp_writer_free(handle: HandlePtr): void;

  // -- Row Writer (cursor-based, no Arrow required) --

  zp_row_writer_open_memory(handle_out_ptr: number): StatusCode;
  zp_row_writer_open_callbacks(
    ctx: HandlePtr,
    write_fn: number,
    close_fn: number,
    handle_out_ptr: number,
  ): StatusCode;
  zp_row_writer_add_column(handle: HandlePtr, name_ptr: number, col_type: number): StatusCode;
  zp_row_writer_add_column_decimal(handle: HandlePtr, name_ptr: number, precision: number, scale: number): StatusCode;
  zp_row_writer_add_column_geometry(handle: HandlePtr, name_ptr: number, crs_ptr: number, crs_len: number): StatusCode;
  zp_row_writer_add_column_geography(handle: HandlePtr, name_ptr: number, crs_ptr: number, crs_len: number, algorithm: number): StatusCode;
  zp_row_writer_set_compression(handle: HandlePtr, codec: number): StatusCode;
  zp_row_writer_begin(handle: HandlePtr): StatusCode;
  zp_row_writer_set_null(handle: HandlePtr, col_index: number): StatusCode;
  zp_row_writer_set_bool(handle: HandlePtr, col_index: number, value: number): StatusCode;
  zp_row_writer_set_int32(handle: HandlePtr, col_index: number, value: number): StatusCode;
  zp_row_writer_set_int64(handle: HandlePtr, col_index: number, value: bigint): StatusCode;
  zp_row_writer_set_float(handle: HandlePtr, col_index: number, value: number): StatusCode;
  zp_row_writer_set_double(handle: HandlePtr, col_index: number, value: number): StatusCode;
  zp_row_writer_set_bytes(handle: HandlePtr, col_index: number, data_ptr: number, len: number): StatusCode;
  zp_row_writer_add_row(handle: HandlePtr): StatusCode;
  zp_row_writer_flush(handle: HandlePtr): StatusCode;
  zp_row_writer_close(handle: HandlePtr): StatusCode;
  zp_row_writer_get_buffer(handle: HandlePtr, data_out_ptr: number, len_out_ptr: number): StatusCode;
  zp_row_writer_error_code(handle: HandlePtr): StatusCode;
  zp_row_writer_error_message(handle: HandlePtr): number;
  zp_row_writer_free(handle: HandlePtr): void;

  // -- Introspection --

  zp_version(): number;
  zp_codec_supported(codec: number): number;
}
