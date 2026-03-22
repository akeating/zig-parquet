/**
 * libparquet - Zig Parquet library C API
 *
 * Arrow C Data Interface is used for data exchange. The caller passes
 * ArrowSchema/ArrowArray structs to read/write row groups. Both are
 * defined below following the Arrow specification.
 *
 * All functions returning int use ZP_OK (0) for success and one of the
 * ZP_ERROR_* codes for failure. Per-handle error details are available
 * via zp_reader_error_message / zp_writer_error_message.
 *
 * Thread safety: no internal synchronization. Each handle must be used
 * from a single thread at a time. Separate handles are independent.
 */

#ifndef LIBPARQUET_H
#define LIBPARQUET_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * Error codes (stable across library versions)
 * ======================================================================== */

#define ZP_OK                    0
#define ZP_ERROR_INVALID_DATA    1   /* Malformed Parquet structure */
#define ZP_ERROR_NOT_PARQUET     2   /* Not a Parquet file (bad magic) */
#define ZP_ERROR_IO              3   /* Transport-level I/O failure */
#define ZP_ERROR_OUT_OF_MEMORY   4   /* Allocation failure */
#define ZP_ERROR_UNSUPPORTED     5   /* Unsupported codec or encoding */
#define ZP_ERROR_INVALID_ARGUMENT 6  /* Bad argument (null ptr, bad index) */
#define ZP_ERROR_INVALID_STATE   7   /* Wrong lifecycle state */
#define ZP_ERROR_CHECKSUM        8   /* Page checksum mismatch */
#define ZP_ERROR_SCHEMA          9   /* Schema-level error */
#define ZP_ERROR_HANDLE_LIMIT   10  /* Handle table full (WASM freestanding) */

/* ========================================================================
 * Compression codec values (matches Parquet CompressionCodec enum)
 * ======================================================================== */

#define ZP_CODEC_UNCOMPRESSED  0
#define ZP_CODEC_SNAPPY        1
#define ZP_CODEC_GZIP          2
#define ZP_CODEC_BROTLI        4
#define ZP_CODEC_LZ4_RAW       7
#define ZP_CODEC_ZSTD          6

/* ========================================================================
 * Arrow C Data Interface
 * https://arrow.apache.org/docs/format/CDataInterface.html
 * ======================================================================== */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char*    format;
    const char*    name;
    const char*    metadata;
    int64_t        flags;
    int64_t        n_children;
    struct ArrowSchema** children;
    struct ArrowSchema*  dictionary;
    void (*release)(struct ArrowSchema*);
    void*          private_data;
};

struct ArrowArray {
    int64_t  length;
    int64_t  null_count;
    int64_t  offset;
    int64_t  n_buffers;
    int64_t  n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray*  dictionary;
    void (*release)(struct ArrowArray*);
    void*    private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* ========================================================================
 * Callback typedefs
 * ======================================================================== */

/**
 * Random-access read callback.
 * Read up to `buf_len` bytes starting at `offset` into `buf`.
 * Set `*bytes_read` to the number of bytes actually read.
 * Return 0 on success, non-zero on failure.
 */
typedef int (*zp_read_at_fn)(void* ctx, uint64_t offset,
                             uint8_t* buf, size_t buf_len,
                             size_t* bytes_read);

/** Return the total size of the data source in bytes. */
typedef uint64_t (*zp_size_fn)(void* ctx);

/**
 * Sequential write callback.
 * Write exactly `len` bytes from `data`.
 * Return 0 on success, non-zero on failure.
 */
typedef int (*zp_write_fn)(void* ctx, const uint8_t* data, size_t len);

/**
 * Close/finalize callback (optional for writers).
 * Return 0 on success, non-zero on failure.
 */
typedef int (*zp_close_fn)(void* ctx);

/* ========================================================================
 * Opaque handle types
 *
 * Reader and writer handles are distinct types so the compiler catches
 * accidental misuse (e.g. passing a writer to a reader function).
 * ======================================================================== */

typedef struct zp_reader_s* zp_reader_t;
typedef struct zp_writer_s* zp_writer_t;

/* ========================================================================
 * Reader API
 * ======================================================================== */

/**
 * Open a reader from an in-memory buffer.
 * The buffer must remain valid until zp_reader_close().
 */
int zp_reader_open_memory(const uint8_t* data, size_t len,
                          zp_reader_t* handle_out);

/**
 * Open a reader using random-access callbacks.
 * The context and callbacks must remain valid until zp_reader_close().
 */
int zp_reader_open_callbacks(void* ctx,
                             zp_read_at_fn read_at,
                             zp_size_fn size,
                             zp_reader_t* handle_out);

/**
 * Open a reader from a file path.
 * The library opens and owns the file; it is closed on zp_reader_close().
 */
int zp_reader_open_file(const char* path,
                        zp_reader_t* handle_out);

/**
 * Get the number of row groups.
 * On success, *count_out is set and ZP_OK is returned.
 */
int zp_reader_get_num_row_groups(zp_reader_t handle, int* count_out);

/** Get the total number of rows across all row groups. */
int zp_reader_get_num_rows(zp_reader_t handle, int64_t* count_out);

/** Get the number of rows in a specific row group. */
int zp_reader_get_row_group_num_rows(zp_reader_t handle,
                                     int rg_index,
                                     int64_t* count_out);

/** Get the number of leaf (physical) columns. */
int zp_reader_get_column_count(zp_reader_t handle, int* count_out);

/**
 * Export the file schema as an ArrowSchema.
 * The caller owns the output and must call schema_out->release().
 */
int zp_reader_get_schema(zp_reader_t handle,
                         struct ArrowSchema* schema_out);

/**
 * Read a row group as Arrow arrays.
 *
 * @param handle       Reader handle.
 * @param rg_index     Row group index (0-based).
 * @param col_indices  Column indices to read, or NULL for all columns.
 * @param num_cols     Number of entries in col_indices (ignored if NULL).
 * @param arrays_out   Output: struct ArrowArray whose children are columns.
 * @param schema_out   Output: corresponding ArrowSchema.
 *
 * Both outputs must be released by the caller.
 * All-or-nothing: on error, no partial output is written.
 */
int zp_reader_read_row_group(zp_reader_t handle,
                             int rg_index,
                             const int* col_indices,
                             int num_cols,
                             struct ArrowArray* arrays_out,
                             struct ArrowSchema* schema_out);

/* ---- Column Statistics ---- */

/**
 * Check if statistics exist for a column in a row group.
 * Returns 1 if statistics exist, 0 if not.
 */
int zp_reader_has_statistics(zp_reader_t handle, int col_index, int rg_index);

/**
 * Get the null count from column statistics.
 * Returns ZP_OK on success, error if statistics or null count unavailable.
 */
int zp_reader_get_null_count(zp_reader_t handle, int col_index, int rg_index,
                             int64_t* count_out);

/** Get the distinct count from column statistics. */
int zp_reader_get_distinct_count(zp_reader_t handle, int col_index,
                                 int rg_index, int64_t* count_out);

/**
 * Get the minimum value from column statistics (raw bytes).
 * The caller interprets the bytes based on the column's physical type.
 * Pointer is borrowed from parsed metadata; valid until reader close.
 */
int zp_reader_get_min_value(zp_reader_t handle, int col_index, int rg_index,
                            const uint8_t** data_out, size_t* len_out);

/** Get the maximum value from column statistics (raw bytes). */
int zp_reader_get_max_value(zp_reader_t handle, int col_index, int rg_index,
                            const uint8_t** data_out, size_t* len_out);

/* ---- Key-Value Metadata (Reader) ---- */

/** Get the number of key-value metadata entries in the file. */
int zp_reader_get_kv_metadata_count(zp_reader_t handle, int* count_out);

/**
 * Get a key-value metadata key by index.
 * Pointer is borrowed from parsed metadata; valid until reader close.
 */
int zp_reader_get_kv_metadata_key(zp_reader_t handle, int index,
                                  const char** key_out, size_t* len_out);

/**
 * Get a key-value metadata value by index.
 * Pointer is borrowed from parsed metadata; valid until reader close.
 * Returns empty string (len=0) if the value is null.
 */
int zp_reader_get_kv_metadata_value(zp_reader_t handle, int index,
                                    const char** val_out, size_t* len_out);

/** Most recent error code for this reader handle. */
int zp_reader_error_code(zp_reader_t handle);

/** Most recent error message (valid until next call on this handle). */
const char* zp_reader_error_message(zp_reader_t handle);

/**
 * Get an Arrow C Stream Interface that iterates over row groups.
 *
 * Each call to get_next() on the stream returns one row group as a
 * struct ArrowArray (children are column arrays). Returns a released
 * array (release == NULL) when all row groups have been consumed.
 *
 * The stream borrows the reader handle; the reader must remain open
 * for the lifetime of the stream. Call stream->release() when done.
 *
 * @param handle      Reader handle (must stay open while stream is active).
 * @param stream_out  Output: initialized ArrowArrayStream.
 */
int zp_reader_get_stream(zp_reader_t handle,
                         struct ArrowArrayStream* stream_out);

/** Close the reader and release all resources. */
void zp_reader_close(zp_reader_t handle);

/* ========================================================================
 * Writer API
 *
 * Lifecycle: open -> set_schema -> [set_column_codec] -> write_row_group
 *            -> close -> [get_buffer] -> free.
 *
 * zp_writer_close() finalizes the Parquet footer. For memory-backed
 * writers, call zp_writer_get_buffer() after close to retrieve the
 * complete file contents. zp_writer_free() releases the handle and
 * all internal memory — the buffer pointer from get_buffer becomes
 * invalid after free.
 * ======================================================================== */

/** Open a writer that writes to an in-memory buffer. */
int zp_writer_open_memory(zp_writer_t* handle_out);

/**
 * Open a writer using write callbacks.
 * close_fn may be NULL if no finalization is needed.
 */
int zp_writer_open_callbacks(void* ctx,
                             zp_write_fn write,
                             zp_close_fn close_fn,
                             zp_writer_t* handle_out);

/**
 * Open a writer to a file path.
 * The library creates and owns the file; it is closed on zp_writer_close().
 */
int zp_writer_open_file(const char* path,
                        zp_writer_t* handle_out);

/**
 * Set the output schema from an ArrowSchema.
 * Must be called exactly once, before writing any row groups.
 * The schema is read but not retained.
 */
int zp_writer_set_schema(zp_writer_t handle,
                         const struct ArrowSchema* schema);

/**
 * Set compression codec for a column (call after set_schema).
 * See ZP_CODEC_* constants for valid values.
 */
int zp_writer_set_column_codec(zp_writer_t handle,
                               int col_index,
                               int codec);

/**
 * Set the target row group size in bytes.
 * This is advisory; the library may write slightly larger row groups.
 * Must be called before writing any row groups.
 * Default: library-chosen (typically 128 MB).
 */
int zp_writer_set_row_group_size(zp_writer_t handle,
                                 int64_t size_bytes);

/**
 * Write a row group from a struct ArrowArray.
 *
 * @param handle  Writer handle.
 * @param batch   Struct ArrowArray whose children are column arrays.
 * @param schema  ArrowSchema describing the batch.
 *
 * Data errors are detected before writing (handle remains valid).
 * Transport errors may corrupt the output (handle becomes unusable).
 */
int zp_writer_write_row_group(zp_writer_t handle,
                              const struct ArrowArray* batch,
                              const struct ArrowSchema* schema);

/**
 * Retrieve the written buffer (memory backend only).
 * Returns a pointer into the internal buffer; valid until zp_writer_free().
 * Returns ZP_ERROR_INVALID_STATE for callback- or file-backed writers.
 */
int zp_writer_get_buffer(zp_writer_t handle,
                         const uint8_t** data_out,
                         size_t* len_out);

/** Most recent error code for this writer handle. */
int zp_writer_error_code(zp_writer_t handle);

/** Most recent error message (valid until next call on this handle). */
const char* zp_writer_error_message(zp_writer_t handle);

/**
 * Finalize the Parquet footer and close the internal writer.
 * After this, zp_writer_get_buffer() returns the complete file.
 * The handle must still be freed with zp_writer_free().
 */
int zp_writer_close(zp_writer_t handle);

/**
 * Free all resources associated with the writer handle.
 * Must be called after zp_writer_close(). For memory writers,
 * retrieve the buffer before calling this.
 */
void zp_writer_free(zp_writer_t handle);

/* ========================================================================
 * Row Reader API (non-Arrow, cursor-based)
 *
 * Row-oriented reader that yields one row at a time with typed getters.
 * No Arrow knowledge required. Backed by a dynamic reader internally.
 *
 * Lifecycle: open -> [get metadata] -> read_row_group -> next/get_* loop
 *            -> close.
 *
 * Column values from the current row are borrowed from the handle and
 * are valid until the next read_row_group or close call.
 * ======================================================================== */

#define ZP_ROW_END          11  /* Returned by next() when no more rows */

/* Physical type constants (0-9) */
#define ZP_TYPE_NULL         0
#define ZP_TYPE_BOOL         1
#define ZP_TYPE_INT32        2
#define ZP_TYPE_INT64        3
#define ZP_TYPE_FLOAT        4
#define ZP_TYPE_DOUBLE       5
#define ZP_TYPE_BYTES        6  /* Raw binary / fixed-length byte arrays */
#define ZP_TYPE_LIST         7
#define ZP_TYPE_MAP          8
#define ZP_TYPE_STRUCT       9

/* Logical type constants (10+)
 *
 * Returned by get_type / get_column_type when the column has a logical
 * type annotation. Use the corresponding physical getter:
 *
 *   STRING, JSON, ENUM, BSON, UUID, FLOAT16  --> get_bytes
 *   DATE, TIME_MILLIS, INT8..UINT32          --> get_int32
 *   TIMESTAMP_*, TIME_MICROS/NANOS, UINT64   --> get_int64
 *   DECIMAL  --> get_int32 (prec<=9), get_int64 (prec<=18), get_bytes (prec>18)
 */
#define ZP_TYPE_STRING              10
#define ZP_TYPE_DATE                11  /* INT32: days since Unix epoch */
#define ZP_TYPE_TIMESTAMP_MILLIS    12  /* INT64: millis since epoch (UTC) */
#define ZP_TYPE_TIMESTAMP_MICROS    13  /* INT64: micros since epoch (UTC) */
#define ZP_TYPE_TIMESTAMP_NANOS     14  /* INT64: nanos since epoch (UTC) */
#define ZP_TYPE_TIME_MILLIS         15  /* INT32: millis since midnight */
#define ZP_TYPE_TIME_MICROS         16  /* INT64: micros since midnight */
#define ZP_TYPE_TIME_NANOS          17  /* INT64: nanos since midnight */
#define ZP_TYPE_INT8                18  /* INT32 with INT(8,signed) */
#define ZP_TYPE_INT16               19  /* INT32 with INT(16,signed) */
#define ZP_TYPE_UINT8               20  /* INT32 with INT(8,unsigned) */
#define ZP_TYPE_UINT16              21  /* INT32 with INT(16,unsigned) */
#define ZP_TYPE_UINT32              22  /* INT32 with INT(32,unsigned) */
#define ZP_TYPE_UINT64              23  /* INT64 with INT(64,unsigned) */
#define ZP_TYPE_UUID                24  /* FLBA(16): RFC 4122 UUID */
#define ZP_TYPE_JSON                25  /* BYTE_ARRAY: JSON string */
#define ZP_TYPE_ENUM                26  /* BYTE_ARRAY: enum member name */
#define ZP_TYPE_DECIMAL             27  /* INT32/INT64/FLBA + precision,scale */
#define ZP_TYPE_FLOAT16             28  /* FLBA(2): IEEE 754 half-precision */
#define ZP_TYPE_BSON                29  /* BYTE_ARRAY: BSON document */
#define ZP_TYPE_INTERVAL            30  /* FLBA(12): months/days/millis */
#define ZP_TYPE_GEOMETRY            31  /* BYTE_ARRAY: WKB geometry */
#define ZP_TYPE_GEOGRAPHY           32  /* BYTE_ARRAY: WKB geography */

typedef struct zp_row_reader_s* zp_row_reader_t;

/** Opaque value handle for nested type traversal. */
typedef void* zp_value_t;

/** Opaque schema handle for nested type definitions. */
typedef const void* zp_schema_t;

/** Open a row reader from an in-memory buffer. */
int zp_row_reader_open_memory(const uint8_t* data, size_t len,
                              zp_row_reader_t* handle_out);

/** Open a row reader using random-access callbacks. */
int zp_row_reader_open_callbacks(void* ctx,
                                 zp_read_at_fn read_at,
                                 zp_size_fn size,
                                 zp_row_reader_t* handle_out);

/** Open a row reader from a file path (non-WASM only). */
int zp_row_reader_open_file(const char* path,
                            zp_row_reader_t* handle_out);

/** Get the number of row groups. */
int zp_row_reader_get_num_row_groups(zp_row_reader_t handle, int* count_out);

/** Get the total number of rows across all row groups. */
int zp_row_reader_get_num_rows(zp_row_reader_t handle, int64_t* count_out);

/** Get the number of top-level columns. */
int zp_row_reader_get_column_count(zp_row_reader_t handle, int* count_out);

/**
 * Get the name of a top-level column. Returns NULL for invalid index.
 * The returned string is valid for the lifetime of the handle.
 */
const char* zp_row_reader_get_column_name(zp_row_reader_t handle,
                                          int col_index);

/**
 * Load a row group for row-by-row iteration.
 * Frees any previously loaded rows. Cursor resets.
 */
int zp_row_reader_read_row_group(zp_row_reader_t handle, int rg_index);

/**
 * Load a row group with column projection.
 * Only the specified top-level columns are read.
 * If col_indices is NULL, all columns are read (same as zp_row_reader_read_row_group).
 * Returned rows contain values in dense order matching the projection list.
 */
int zp_row_reader_read_row_group_projected(zp_row_reader_t handle,
                                           int rg_index,
                                           const int* col_indices,
                                           int num_cols);

/**
 * Advance to the next row.
 * Returns ZP_OK when a row is available, ZP_ROW_END when done.
 */
int zp_row_reader_next(zp_row_reader_t handle);

/**
 * Get the value type at a column in the current row.
 * Returns a logical type constant (ZP_TYPE_STRING, ZP_TYPE_DATE, etc.)
 * when the column has a logical type annotation, otherwise returns the
 * physical type (ZP_TYPE_INT32, ZP_TYPE_BYTES, etc.).
 * Returns ZP_TYPE_NULL for null values.
 */
int zp_row_reader_get_type(zp_row_reader_t handle, int col_index);

/** Returns 1 if the column value is null, 0 otherwise. */
int zp_row_reader_is_null(zp_row_reader_t handle, int col_index);

/** Get an int32 value. Returns 0 if null or type mismatch. */
int32_t zp_row_reader_get_int32(zp_row_reader_t handle, int col_index);

/** Get an int64 value. Returns 0 if null or type mismatch. */
int64_t zp_row_reader_get_int64(zp_row_reader_t handle, int col_index);

/** Get a float value. Returns 0 if null or type mismatch. */
float zp_row_reader_get_float(zp_row_reader_t handle, int col_index);

/** Get a double value. Returns 0 if null or type mismatch. */
double zp_row_reader_get_double(zp_row_reader_t handle, int col_index);

/** Get a boolean value. Returns 0 if null or type mismatch. */
int zp_row_reader_get_bool(zp_row_reader_t handle, int col_index);

/**
 * Get byte/string data from the current row.
 * On success, *data_out and *len_out are set. The data is valid until
 * the next read_row_group or close call.
 * Returns ZP_OK on success. On type mismatch, outputs are set to empty.
 */
int zp_row_reader_get_bytes(zp_row_reader_t handle, int col_index,
                            const uint8_t** data_out, size_t* len_out);

/**
 * Get the schema-level type for a column (available before iterating rows).
 * Returns a ZP_TYPE_* constant reflecting the logical type when present.
 * Returns ZP_TYPE_NULL for invalid column index.
 */
int zp_row_reader_get_column_type(zp_row_reader_t handle, int col_index);

/**
 * Get decimal precision for a DECIMAL column.
 * Returns 0 for non-decimal columns or invalid index.
 */
int zp_row_reader_get_decimal_precision(zp_row_reader_t handle, int col_index);

/**
 * Get decimal scale for a DECIMAL column.
 * Returns 0 for non-decimal columns or invalid index.
 */
int zp_row_reader_get_decimal_scale(zp_row_reader_t handle, int col_index);

/* ---- Key-Value Metadata (Row Reader) ---- */

/** Get the number of key-value metadata entries. */
int zp_row_reader_get_kv_metadata_count(zp_row_reader_t handle,
                                        int* count_out);

/** Get a key-value metadata key by index. */
int zp_row_reader_get_kv_metadata_key(zp_row_reader_t handle, int index,
                                      const char** key_out, size_t* len_out);

/** Get a key-value metadata value by index. */
int zp_row_reader_get_kv_metadata_value(zp_row_reader_t handle, int index,
                                        const char** val_out, size_t* len_out);

/**
 * Enable or disable page checksum validation for subsequent reads.
 * @param validate  Non-zero to enable CRC32 validation.
 * @param strict    Non-zero to fail on missing checksums (only if validate).
 */
int zp_row_reader_set_checksum_validation(zp_row_reader_t handle,
                                          int validate, int strict);

/* ---- Row Reader Column Statistics ---- */

/** Check if statistics exist for a column in a row group. Returns 1 or 0. */
int zp_row_reader_has_statistics(zp_row_reader_t handle, int col_index,
                                 int rg_index);

/** Get the null count from column statistics. */
int zp_row_reader_get_null_count(zp_row_reader_t handle, int col_index,
                                 int rg_index, int64_t* count_out);

/** Get the distinct count from column statistics. */
int zp_row_reader_get_distinct_count(zp_row_reader_t handle, int col_index,
                                     int rg_index, int64_t* count_out);

/**
 * Get the minimum value from column statistics (raw bytes).
 * Pointer is borrowed from parsed metadata; valid until reader close.
 */
int zp_row_reader_get_min_value(zp_row_reader_t handle, int col_index,
                                int rg_index, const uint8_t** data_out,
                                size_t* len_out);

/** Get the maximum value from column statistics (raw bytes). */
int zp_row_reader_get_max_value(zp_row_reader_t handle, int col_index,
                                int rg_index, const uint8_t** data_out,
                                size_t* len_out);

/** Most recent error code for this row reader. */
int zp_row_reader_error_code(zp_row_reader_t handle);

/** Most recent error message (valid until next call on this handle). */
const char* zp_row_reader_error_message(zp_row_reader_t handle);

/** Close the row reader and release all resources. */
void zp_row_reader_close(zp_row_reader_t handle);

/* ========================================================================
 * Value Handle API (nested type traversal)
 *
 * Retrieve structured values from the current row. For columns with
 * nested types (LIST, MAP, STRUCT), use zp_row_reader_get_value() to
 * obtain an opaque value handle, then traverse it with the accessors
 * below. Value handles are borrowed and valid until the next
 * read_row_group or close call.
 * ======================================================================== */

/** Get an opaque value handle for a column in the current row. */
const void* zp_row_reader_get_value(zp_row_reader_t handle, int col_index);

/** Get the type tag of a value (ZP_TYPE_* constant). */
int zp_value_get_type(const void* value);

/** Returns 1 if the value is null, 0 otherwise. */
int zp_value_is_null(const void* value);

/** Get an int32 from a value. Returns 0 if null or type mismatch. */
int32_t zp_value_get_int32(const void* value);

/** Get an int64 from a value. Returns 0 if null or type mismatch. */
int64_t zp_value_get_int64(const void* value);

/** Get a float from a value. Returns 0 if null or type mismatch. */
float zp_value_get_float(const void* value);

/** Get a double from a value. Returns 0 if null or type mismatch. */
double zp_value_get_double(const void* value);

/** Get a boolean from a value. Returns 0 if null or type mismatch. */
int zp_value_get_bool(const void* value);

/**
 * Get byte/string data from a value.
 * On success, *data_out and *len_out are set.
 * Returns ZP_OK on success. On type mismatch, outputs are set to empty.
 */
int zp_value_get_bytes(const void* value,
                       const uint8_t** data_out, size_t* len_out);

/* ---- List traversal ---- */

/** Get the number of elements in a LIST value. Returns 0 if null or type mismatch. */
int zp_value_get_list_len(const void* value);

/** Get a list element by index. Returns NULL on error. */
const void* zp_value_get_list_element(const void* value, int index);

/* ---- Map traversal ---- */

/** Get the number of entries in a MAP value. Returns 0 if null or type mismatch. */
int zp_value_get_map_len(const void* value);

/** Get the key of a map entry by index. Returns NULL on error. */
const void* zp_value_get_map_key(const void* value, int entry_index);

/** Get the value of a map entry by index. Returns NULL on error. */
const void* zp_value_get_map_value(const void* value, int entry_index);

/* ---- Struct traversal ---- */

/** Get the number of fields in a STRUCT value. Returns 0 if null or type mismatch. */
int zp_value_get_struct_field_count(const void* value);

/**
 * Get the name of a struct field by index.
 * On success, *name_out and *len_out are set.
 */
int zp_value_get_struct_field_name(const void* value, int field_index,
                                   const uint8_t** name_out, size_t* len_out);

/** Get the value of a struct field by index. Returns NULL on error. */
const void* zp_value_get_struct_field_value(const void* value,
                                            int field_index);

/* ========================================================================
 * Row Writer API (non-Arrow, cursor-based)
 *
 * Row-oriented writer for creating Parquet files without Arrow knowledge.
 * Supports multiple row groups via explicit flush() calls.
 *
 * Lifecycle: open -> add_column* -> [set_compression] -> begin
 *            -> [set_*/add_row]* -> [flush] -> close -> [get_buffer] -> free.
 *
 * Each flush() writes buffered rows as a new row group.
 * close() flushes remaining rows and writes the Parquet footer.
 * ======================================================================== */

typedef struct zp_row_writer_s* zp_row_writer_t;

/** Open a row writer that writes to an in-memory buffer. */
int zp_row_writer_open_memory(zp_row_writer_t* handle_out);

/**
 * Open a row writer using write callbacks.
 * close_fn may be NULL if no finalization is needed.
 */
int zp_row_writer_open_callbacks(void* ctx,
                                 zp_write_fn write,
                                 zp_close_fn close_fn,
                                 zp_row_writer_t* handle_out);

/** Open a row writer to a file path (non-WASM only). */
int zp_row_writer_open_file(const char* path,
                            zp_row_writer_t* handle_out);

/**
 * Add a column to the schema (must be called before begin).
 * col_type is any ZP_TYPE_* constant -- physical (BOOL, INT32, INT64,
 * FLOAT, DOUBLE, BYTES) or logical (STRING, DATE, TIMESTAMP_MICROS,
 * UUID, INT8, UINT32, JSON, etc.). Logical types set the appropriate
 * physical type and annotation automatically.
 *
 * For DECIMAL columns, use zp_row_writer_add_column_decimal() instead.
 */
int zp_row_writer_add_column(zp_row_writer_t handle,
                             const char* name,
                             int col_type);

/**
 * Add a DECIMAL column to the schema (must be called before begin).
 * Physical type is chosen automatically: INT32 for precision <= 9,
 * INT64 for precision <= 18, FIXED_LEN_BYTE_ARRAY for larger.
 */
int zp_row_writer_add_column_decimal(zp_row_writer_t handle,
                                     const char* name,
                                     int precision, int scale);

/**
 * Add a GEOMETRY column. CRS is optional (NULL = default OGC:CRS84).
 */
int zp_row_writer_add_column_geometry(zp_row_writer_t handle,
                                      const char* name,
                                      const char* crs, int crs_len);

/**
 * Add a GEOGRAPHY column. CRS is optional (NULL = default OGC:CRS84).
 * algorithm: 0=spherical (default), 1=vincenty, 2=thomas, 3=andoyer, 4=karney
 */
int zp_row_writer_add_column_geography(zp_row_writer_t handle,
                                       const char* name,
                                       const char* crs, int crs_len,
                                       int algorithm);

/* ---- Schema builder for nested types ---- */

/** Create a schema node for a primitive type. */
const void* zp_schema_primitive(zp_row_writer_t handle, int zp_type);

/** Create a DECIMAL schema node with given precision (1-38) and scale (0..precision). */
const void* zp_schema_decimal(zp_row_writer_t handle, int precision, int scale);

/** Wrap a schema node to make it optional (nullable). */
const void* zp_schema_optional(zp_row_writer_t handle, const void* inner);

/** Create a LIST schema with the given element schema. */
const void* zp_schema_list(zp_row_writer_t handle, const void* element);

/** Create a MAP schema with the given key and value schemas. */
const void* zp_schema_map(zp_row_writer_t handle,
                          const void* key_schema,
                          const void* value_schema);

/** Create a STRUCT schema from parallel arrays of field names and schemas. */
const void* zp_schema_struct(zp_row_writer_t handle,
                             const char** field_names,
                             const void** field_schemas,
                             int field_count);

/**
 * Add a column with a nested type schema (must be called before begin).
 * The schema is built using zp_schema_* functions above.
 */
int zp_row_writer_add_column_nested(zp_row_writer_t handle,
                                    const char* name,
                                    const void* schema);

/**
 * Set default compression codec for all columns.
 * Must be called before begin(). See ZP_CODEC_* constants.
 */
int zp_row_writer_set_compression(zp_row_writer_t handle, int codec);

/**
 * Set compression codec for a specific column.
 * Must be called after add_column and before begin().
 */
int zp_row_writer_set_column_codec(zp_row_writer_t handle,
                                   int col_index, int codec);

/**
 * Set the maximum number of rows per row group.
 * When reached, rows are automatically flushed as a new row group.
 * Can be called at any time before close.
 */
int zp_row_writer_set_row_group_size(zp_row_writer_t handle,
                                     int64_t max_rows);

/**
 * Set a key-value metadata entry on the file.
 * If the key already exists, the value is updated.
 * Can be called at any time before close.
 * The key and value are copied internally.
 */
int zp_row_writer_set_kv_metadata(zp_row_writer_t handle,
                                  const char* key, size_t key_len,
                                  const char* value, size_t value_len);

/**
 * Finalize the schema and begin writing.
 * At least one column must have been added.
 */
int zp_row_writer_begin(zp_row_writer_t handle);

/** Set a column value to null in the current row. */
int zp_row_writer_set_null(zp_row_writer_t handle, int col_index);

/** Set a boolean column value in the current row. */
int zp_row_writer_set_bool(zp_row_writer_t handle, int col_index, int value);

/** Set an int32 column value in the current row. */
int zp_row_writer_set_int32(zp_row_writer_t handle, int col_index, int32_t value);

/** Set an int64 column value in the current row. */
int zp_row_writer_set_int64(zp_row_writer_t handle, int col_index, int64_t value);

/** Set a float column value in the current row. */
int zp_row_writer_set_float(zp_row_writer_t handle, int col_index, float value);

/** Set a double column value in the current row. */
int zp_row_writer_set_double(zp_row_writer_t handle, int col_index, double value);

/**
 * Set a bytes/string column value in the current row.
 * The data is copied internally; the caller may free it after this call.
 */
int zp_row_writer_set_bytes(zp_row_writer_t handle, int col_index,
                            const uint8_t* data, size_t len);

/* ---- Nested value builders ---- */

/** Begin building a list value for a column. */
int zp_row_writer_begin_list(zp_row_writer_t handle, int col_index);

/** End a list value (commits the accumulated elements). */
int zp_row_writer_end_list(zp_row_writer_t handle, int col_index);

/** Append a null element to the current list/map value. */
int zp_row_writer_append_null(zp_row_writer_t handle, int col_index);

/** Append a boolean element to the current list value. */
int zp_row_writer_append_bool(zp_row_writer_t handle, int col_index, int value);

/** Append an int32 element to the current list value. */
int zp_row_writer_append_int32(zp_row_writer_t handle, int col_index, int32_t value);

/** Append an int64 element to the current list value. */
int zp_row_writer_append_int64(zp_row_writer_t handle, int col_index, int64_t value);

/** Append a float element to the current list value. */
int zp_row_writer_append_float(zp_row_writer_t handle, int col_index, float value);

/** Append a double element to the current list value. */
int zp_row_writer_append_double(zp_row_writer_t handle, int col_index, double value);

/** Append a bytes/string element to the current list value. */
int zp_row_writer_append_bytes(zp_row_writer_t handle, int col_index,
                               const uint8_t* data, size_t len);

/** Begin building a struct value for a column. */
int zp_row_writer_begin_struct(zp_row_writer_t handle, int col_index);

/** Set a struct field to null. */
int zp_row_writer_set_field_null(zp_row_writer_t handle, int col_index,
                                 int field_index);

/** Set a struct field to an int32 value. */
int zp_row_writer_set_field_int32(zp_row_writer_t handle, int col_index,
                                  int field_index, int32_t value);

/** Set a struct field to an int64 value. */
int zp_row_writer_set_field_int64(zp_row_writer_t handle, int col_index,
                                  int field_index, int64_t value);

/** Set a struct field to a float value. */
int zp_row_writer_set_field_float(zp_row_writer_t handle, int col_index,
                                  int field_index, float value);

/** Set a struct field to a double value. */
int zp_row_writer_set_field_double(zp_row_writer_t handle, int col_index,
                                   int field_index, double value);

/** Set a struct field to a boolean value. */
int zp_row_writer_set_field_bool(zp_row_writer_t handle, int col_index,
                                  int field_index, int value);

/** Set a struct field to a bytes/string value. */
int zp_row_writer_set_field_bytes(zp_row_writer_t handle, int col_index,
                                  int field_index,
                                  const uint8_t* data, size_t len);

/** End a struct value (commits the accumulated fields). */
int zp_row_writer_end_struct(zp_row_writer_t handle, int col_index);

/** Begin building a map value for a column. */
int zp_row_writer_begin_map(zp_row_writer_t handle, int col_index);

/** Begin a key-value entry within the current map. */
int zp_row_writer_begin_map_entry(zp_row_writer_t handle, int col_index);

/** End a key-value entry within the current map. */
int zp_row_writer_end_map_entry(zp_row_writer_t handle, int col_index);

/** End a map value (commits the accumulated entries). */
int zp_row_writer_end_map(zp_row_writer_t handle, int col_index);

/**
 * Commit the current row to the buffer. Unset columns default to null.
 * Must be called after begin().
 */
int zp_row_writer_add_row(zp_row_writer_t handle);

/**
 * Write buffered rows as a row group.
 * Does nothing if no rows are buffered.
 */
int zp_row_writer_flush(zp_row_writer_t handle);

/**
 * Flush remaining rows, write the Parquet footer, and close the output.
 * After this, get_buffer() returns the complete file (memory backend).
 * The handle must still be freed with zp_row_writer_free().
 */
int zp_row_writer_close(zp_row_writer_t handle);

/**
 * Retrieve the written buffer (memory backend only).
 * Returns a pointer into the internal buffer; valid until zp_row_writer_free().
 */
int zp_row_writer_get_buffer(zp_row_writer_t handle,
                             const uint8_t** data_out,
                             size_t* len_out);

/** Most recent error code for this row writer. */
int zp_row_writer_error_code(zp_row_writer_t handle);

/** Most recent error message (valid until next call on this handle). */
const char* zp_row_writer_error_message(zp_row_writer_t handle);

/** Free all resources associated with the row writer handle. */
void zp_row_writer_free(zp_row_writer_t handle);

/* ========================================================================
 * Library introspection
 * ======================================================================== */

/** Return the library version string (e.g. "0.1.0"). */
const char* zp_version(void);

/**
 * Check if a compression codec is supported in this build.
 * Returns 1 if supported, 0 if not. See ZP_CODEC_* constants.
 */
int zp_codec_supported(int codec);

#ifdef __cplusplus
}
#endif

#endif /* LIBPARQUET_H */
