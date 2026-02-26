/**
 * TypeScript type definitions for the zig-parquet freestanding WASM module.
 *
 * The freestanding API uses integer handle IDs (not opaque pointers) and
 * requires four host-imported IO functions.
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

// Compression codec IDs for zp_codec_supported / zp_writer_set_column_codec
export declare const ZP_CODEC_UNCOMPRESSED = 0;
export declare const ZP_CODEC_SNAPPY = 1;
export declare const ZP_CODEC_GZIP = 2;
export declare const ZP_CODEC_LZ4_RAW = 4;
export declare const ZP_CODEC_BROTLI = 6;
export declare const ZP_CODEC_ZSTD = 7;

// ---------------------------------------------------------------------------
// Host imports — the host must provide these in the "env" namespace
// ---------------------------------------------------------------------------

/**
 * Functions the host must supply when instantiating the WASM module.
 *
 * `ctx` is an opaque u32 context ID chosen by the host and passed back
 * unchanged. The host uses it to identify which data source/sink to operate on.
 */
export interface HostImports {
  /**
   * Read bytes from position `offset` (split into lo/hi u32) into WASM
   * linear memory at `buf_ptr` (up to `buf_len` bytes).
   * Returns the number of bytes read (>= 0) or a negative value on error.
   */
  host_read_at(
    ctx: number,
    offset_lo: number,
    offset_hi: number,
    buf_ptr: number,
    buf_len: number,
  ): number;

  /** Return the total byte size of the data source identified by `ctx`. */
  host_size(ctx: number): bigint;

  /**
   * Write `data_len` bytes from WASM linear memory starting at `data_ptr`.
   * Returns 0 on success, non-zero on error.
   */
  host_write(ctx: number, data_ptr: number, data_len: number): number;

  /**
   * Close the output sink identified by `ctx`.
   * Returns 0 on success, non-zero on error.
   */
  host_close(ctx: number): number;
}

// ---------------------------------------------------------------------------
// Module exports
// ---------------------------------------------------------------------------

/** Handle ID returned by open functions. >= 0 on success, negative on error. */
type HandleId = number;

/** Status code. 0 = ZP_OK, positive = error. */
type StatusCode = number;

export interface ParquetFreestandingExports {
  /** WASM linear memory. */
  memory: WebAssembly.Memory;

  // -- Memory management --

  /** Allocate `len` bytes in WASM memory. Returns pointer or 0 on failure. */
  zp_alloc(len: number): number;

  /** Free a previously allocated region. */
  zp_free(ptr: number, len: number): void;

  // -- Row Reader (cursor-based, no Arrow required) --

  /**
   * Open a row reader from a buffer already in WASM linear memory.
   * Returns handle ID (>= 0) on success, or negative error code.
   */
  zp_row_reader_open_buffer(data_ptr: number, len: number): HandleId;

  /**
   * Open a row reader using host-provided IO callbacks.
   * `ctx` is passed to host_read_at / host_size.
   * Returns handle ID (>= 0) on success, or negative error code.
   */
  zp_row_reader_open_host(ctx: number): HandleId;

  /** Get number of row groups. Returns count or negative on error. */
  zp_row_reader_get_num_row_groups(handle_id: HandleId): number;

  /** Write total row count to `count_out_ptr` (i64 LE). Returns status. */
  zp_row_reader_get_num_rows(
    handle_id: HandleId,
    count_out_ptr: number,
  ): StatusCode;

  /** Write top-level column count to `count_out_ptr` (i32 LE). Returns status. */
  zp_row_reader_get_column_count(
    handle_id: HandleId,
    count_out_ptr: number,
  ): StatusCode;

  /** Get pointer to null-terminated column name, or null. */
  zp_row_reader_get_column_name(
    handle_id: HandleId,
    col_index: number,
  ): number;

  /**
   * Load all rows for a row group into memory.
   * Must be called before next()/get_*.
   */
  zp_row_reader_read_row_group(
    handle_id: HandleId,
    rg_index: number,
  ): StatusCode;

  /**
   * Advance the cursor to the next row.
   * Returns ZP_OK (0) if a row is available, ZP_ROW_END (11) when exhausted.
   */
  zp_row_reader_next(handle_id: HandleId): StatusCode;

  /** Get the value type for a column in the current row (ZP_TYPE_* constant). */
  zp_row_reader_get_type(handle_id: HandleId, col_index: number): number;

  /** Returns 1 if the column value is null, 0 otherwise. */
  zp_row_reader_is_null(handle_id: HandleId, col_index: number): number;

  /** Get an int32 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_int32(handle_id: HandleId, col_index: number): number;

  /** Get an int64 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_int64(handle_id: HandleId, col_index: number): bigint;

  /** Get a float32 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_float(handle_id: HandleId, col_index: number): number;

  /** Get a float64 value. Returns 0 on type mismatch or null. */
  zp_row_reader_get_double(handle_id: HandleId, col_index: number): number;

  /** Get a boolean value as 0/1. Returns 0 on type mismatch or null. */
  zp_row_reader_get_bool(handle_id: HandleId, col_index: number): number;

  /** Get pointer to bytes/string data. Returns null if not bytes or null. */
  zp_row_reader_get_bytes_ptr(
    handle_id: HandleId,
    col_index: number,
  ): number;

  /** Get length of bytes/string data. Returns 0 if not bytes or null. */
  zp_row_reader_get_bytes_len(
    handle_id: HandleId,
    col_index: number,
  ): number;

  /** Get the schema-level type for a column (available before iterating rows). */
  zp_row_reader_get_column_type(handle_id: HandleId, col_index: number): number;

  /** Get decimal precision for a DECIMAL column. Returns 0 for non-decimal. */
  zp_row_reader_get_decimal_precision(handle_id: HandleId, col_index: number): number;

  /** Get decimal scale for a DECIMAL column. Returns 0 for non-decimal. */
  zp_row_reader_get_decimal_scale(handle_id: HandleId, col_index: number): number;

  /** Get most recent error code. */
  zp_row_reader_error_code(handle_id: HandleId): StatusCode;

  /** Get pointer to null-terminated error message. */
  zp_row_reader_error_message(handle_id: HandleId): number;

  /** Close the row reader and free all resources. */
  zp_row_reader_close(handle_id: HandleId): void;

  // -- Row Writer (cursor-based, no Arrow required) --

  /** Open a row writer to an in-memory buffer. Returns handle ID. */
  zp_row_writer_open_buffer(): HandleId;

  /** Open a row writer using host-provided IO callbacks. Returns handle ID. */
  zp_row_writer_open_host(ctx: number): HandleId;

  /** Add a column to the schema (before begin). col_type is any ZP_TYPE_* constant. */
  zp_row_writer_add_column(handle_id: HandleId, name_ptr: number, col_type: number): StatusCode;

  /** Add a DECIMAL column to the schema (before begin). */
  zp_row_writer_add_column_decimal(handle_id: HandleId, name_ptr: number, precision: number, scale: number): StatusCode;

  /** Add GEOMETRY column. crs_ptr=0 for default OGC:CRS84. */
  zp_row_writer_add_column_geometry(handle_id: HandleId, name_ptr: number, crs_ptr: number, crs_len: number): StatusCode;

  /** Add GEOGRAPHY column. algorithm: 0=spherical, 1=vincenty, 2=thomas, 3=andoyer, 4=karney. */
  zp_row_writer_add_column_geography(handle_id: HandleId, name_ptr: number, crs_ptr: number, crs_len: number, algorithm: number): StatusCode;

  /** Set default compression codec (before begin). */
  zp_row_writer_set_compression(handle_id: HandleId, codec: number): StatusCode;

  /** Finalize schema and begin writing. */
  zp_row_writer_begin(handle_id: HandleId): StatusCode;

  /** Set a column value to null in the current row. */
  zp_row_writer_set_null(handle_id: HandleId, col_index: number): StatusCode;

  /** Set a boolean column value (0 = false, non-zero = true). */
  zp_row_writer_set_bool(handle_id: HandleId, col_index: number, value: number): StatusCode;

  /** Set an int32 column value. */
  zp_row_writer_set_int32(handle_id: HandleId, col_index: number, value: number): StatusCode;

  /** Set an int64 column value. */
  zp_row_writer_set_int64(handle_id: HandleId, col_index: number, value: bigint): StatusCode;

  /** Set a float32 column value. */
  zp_row_writer_set_float(handle_id: HandleId, col_index: number, value: number): StatusCode;

  /** Set a float64 column value. */
  zp_row_writer_set_double(handle_id: HandleId, col_index: number, value: number): StatusCode;

  /** Set a bytes/string column value. Data is copied internally. */
  zp_row_writer_set_bytes(handle_id: HandleId, col_index: number, data_ptr: number, len: number): StatusCode;

  /** Commit the current row. Unset columns default to null. */
  zp_row_writer_add_row(handle_id: HandleId): StatusCode;

  /** Write buffered rows as a row group. */
  zp_row_writer_flush(handle_id: HandleId): StatusCode;

  /** Flush remaining rows, write footer, and close output. */
  zp_row_writer_close(handle_id: HandleId): StatusCode;

  /** Get buffer pointer and length (memory backend only). */
  zp_row_writer_get_buffer(handle_id: HandleId, data_out_ptr: number, len_out_ptr: number): StatusCode;

  /** Get most recent error code. */
  zp_row_writer_error_code(handle_id: HandleId): StatusCode;

  /** Get pointer to null-terminated error message. */
  zp_row_writer_error_message(handle_id: HandleId): number;

  /** Free all resources associated with the row writer. */
  zp_row_writer_free(handle_id: HandleId): void;

  // -- Arrow Reader (advanced) --

  zp_reader_open_buffer(data_ptr: number, len: number): HandleId;
  zp_reader_open_host(ctx: number): HandleId;
  zp_reader_get_num_row_groups(handle_id: HandleId): number;
  zp_reader_get_num_rows(
    handle_id: HandleId,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_row_group_num_rows(
    handle_id: HandleId,
    rg_index: number,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_column_count(
    handle_id: HandleId,
    count_out_ptr: number,
  ): StatusCode;
  zp_reader_get_schema(
    handle_id: HandleId,
    schema_out_ptr: number,
  ): StatusCode;
  zp_reader_read_row_group(
    handle_id: HandleId,
    rg_index: number,
    col_indices_ptr: number,
    num_cols: number,
    arrays_out_ptr: number,
    schema_out_ptr: number,
  ): StatusCode;
  zp_reader_error_code(handle_id: HandleId): StatusCode;
  zp_reader_error_message(handle_id: HandleId): number;
  zp_reader_close(handle_id: HandleId): void;

  // -- Writer --

  zp_writer_open_buffer(): HandleId;
  zp_writer_open_host(ctx: number): HandleId;
  zp_writer_set_schema(
    handle_id: HandleId,
    schema_ptr: number,
  ): StatusCode;
  zp_writer_set_column_codec(
    handle_id: HandleId,
    col_index: number,
    codec: number,
  ): StatusCode;
  zp_writer_set_row_group_size(
    handle_id: HandleId,
    size_bytes: bigint,
  ): StatusCode;
  zp_writer_write_row_group(
    handle_id: HandleId,
    batch_ptr: number,
    schema_ptr: number,
  ): StatusCode;
  zp_writer_get_buffer(
    handle_id: HandleId,
    data_out_ptr: number,
    len_out_ptr: number,
  ): StatusCode;
  zp_writer_error_code(handle_id: HandleId): StatusCode;
  zp_writer_error_message(handle_id: HandleId): number;
  zp_writer_close(handle_id: HandleId): StatusCode;
  zp_writer_free(handle_id: HandleId): void;

  // -- Introspection --

  zp_version(): number;
  zp_codec_supported(codec: number): number;

  // -- Arrow struct accessors --

  zp_arrow_array_get_length(arr_ptr: number): bigint;
  zp_arrow_array_get_null_count(arr_ptr: number): bigint;
  zp_arrow_array_get_n_children(arr_ptr: number): bigint;
  zp_arrow_array_get_n_buffers(arr_ptr: number): bigint;
  zp_arrow_array_get_child(arr_ptr: number, index: number): number;
  zp_arrow_array_get_buffer(arr_ptr: number, index: number): number;
  zp_arrow_schema_get_format(schema_ptr: number): number;
  zp_arrow_schema_get_name(schema_ptr: number): number;
  zp_arrow_schema_get_n_children(schema_ptr: number): bigint;
  zp_arrow_schema_get_child(schema_ptr: number, index: number): number;
}
