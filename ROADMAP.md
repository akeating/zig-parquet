# Roadmap

## Phase 1: Parquet Reader + `pq` CLI

- [x] Generate canonical test files (PyArrow)

### P0 — Read a Simple File ✅
Build together as one unit (can't test independently):
- [x] Thrift Compact Protocol decoder
- [x] Footer parsing (magic bytes, FileMetaData)
- [x] PLAIN encoding (all primitive types)
- [x] Flat schemas only
- [x] Validate: read `basic_types_plain_uncompressed.parquet`

### P1 — Encodings + Compression ✅
- [x] DICTIONARY encoding
- [x] zstd decompression (libzstd C library)
- [x] gzip decompression (zlib C library)
- [x] Multiple row group iteration
- [x] Validate: read all P1 test files

### P2 — `pq` CLI ✅
- [x] `pq schema <file>`
- [x] `pq head <file> -n 5`
- [x] `pq cat <file> --json`
- [x] `pq stats <file>`

### P3 — Additional Compression (Read) ✅
- [x] Snappy decompression (C interop)
- [x] LZ4_RAW decompression (C interop)
- [x] Brotli decompression (C interop)

---

## Phase 2: Parquet Writer

### P0 — Basic Writer ✅
- [x] PLAIN encoding writer
- [x] Flat schemas only
- [x] `pq convert data.csv out.parquet`

### P1 — Logical Types (Core) ✅
- [x] STRING (BYTE_ARRAY + UTF8 annotation)
- [x] DATE (INT32, days since epoch)
- [x] TIMESTAMP (INT64, millis/micros/nanos)
- [x] TIME (INT32/INT64, millis/micros/nanos)
- [x] Schema serialization of LogicalType

### P2 — Logical Types (Extended) ✅
- [x] DECIMAL (INT32/INT64/FIXED with precision/scale)
- [x] UUID (FIXED_LEN_BYTE_ARRAY(16))
- [x] INT8/INT16/UINT8/UINT16/UINT32/UINT64
- [x] FLOAT16 (FIXED_LEN_BYTE_ARRAY(2), IEEE 754 half-precision)
- [x] ENUM (BYTE_ARRAY with enum annotation)
- [x] JSON (BYTE_ARRAY with json annotation)

### P3 — Writer Compression Infrastructure ✅
- [x] Column-level codec selection in ColumnDef
- [x] Compression API in compress module
- [x] Threading codec through writer pipeline

### P4 — Writer Compression (C Interop) ✅
- [x] zstd compression (libzstd v1.5.7)
- [x] gzip compression (zlib v1.3.1)
- [x] Snappy compression (C++ lib with -fno-exceptions for WASM)
- [x] LZ4_RAW compression (lz4 C lib)
- [x] Brotli compression (brotli C lib)

### P5 — Validation ✅
- [x] `pq validate <file>`
- [x] Round-trip testing with compression (write → read → compare)

---

## Phase 3: Nested Types

Extends both reader and writer to support complex nested structures.

### P0 — Lists ✅
- [x] Repeated fields (1-level nesting)
- [x] Multi-level definition/repetition levels
- [x] Read support
- [x] Write support

### P1 — Structs ✅
- [x] Nested groups
- [x] Read support
- [x] Write support

### P2 — Maps ✅
- [x] Key-value groups
- [x] Read support
- [x] Write support

### P3 — Nested Composition ✅
- [x] SchemaNode tagged union (recursive type tree)
- [x] Value tagged union (dynamic nested values)
- [x] ColumnDef.fromNode() for complex nested types
- [x] Arbitrary nesting depth (recursive level computation per leaf)
- [x] Flatten/assemble for nested Value types (in-memory)
- [x] Schema generation from SchemaNode
- [x] Writer.writeNestedColumn() — complete file I/O
- [x] Reader typed APIs work with nested columns
- [x] Round-trip tests with full data verification:
  - list<int32>
  - list<struct<i64, f64>>
  - struct<byte_array, int32>
  - struct<byte_array, list<int32>>
  - map<string, int32>
  - map<int32, int64>

### P4 — Logical Types ✅
- [x] SchemaNode supports logical type annotations on primitives
- [x] Convenience constructors: string(), date(), timestamp(), decimal(), uuid(), json()
- [x] Schema generation emits logical_type in footer
- [x] Reader preserves logical_type when inferring SchemaNode
- [x] Round-trip tests verify logical types (STRING, DATE)

---

## Phase 4: Row-Based API

Row-oriented API for natural data ingestion. User controls flushing.

### P0 — RowWriter Core ✅
- [x] `RowWriter(T)` comptime generic over struct type
- [x] Schema inference from Zig struct fields
- [x] Column buffers generated at compile time
- [x] `writeRow(row: T)` — append single row
- [x] `writeRows(rows: []const T)` — append batch to buffers
- [x] `flush()` — write buffered data as row group, clear buffers
- [x] `close()` — final flush + write footer
- [x] Multiple row groups in single file
- [x] Round-trip tests with verification

### P1 — Extended Type Support ✅
- [x] Additional integer sizes: i8, i16, u8, u16, u32, u64 (with INT logical type)
- [x] Logical type wrappers: `Timestamp`, `Date`, `Time`, `Uuid`
- [x] Round-trip tests for all new types

### P2 — Nested Struct Support ✅
- [x] Nested structs → flattened columns with dot-path names (e.g., `address.city`)
- [x] Recursive field extraction at comptime
- [x] Deep nesting support (3+ levels)
- [x] Optional fields → nullable columns (already works)

### P3 — Slice/List Field Support ✅
- [x] `[]const T` — required list of required elements
- [x] `?[]const T` — optional list of required elements
- [x] `[]const ?T` — required list of optional elements
- [x] `?[]const ?T` — optional list of optional elements
- [x] 3-level list schema generation
- [x] Integration with list_encoder for def/rep levels
- [x] All primitive list element types: i32, i64, f32, f64, bool, []const u8
- [x] Small integers in lists: i8, i16, u8, u16, u32, u64 (widened to i32/i64)
- [x] Logical type wrappers in lists: Timestamp, Date, Time
- [x] `[]const Uuid` — UUID lists with fixed byte array encoding
- [x] `[]const []const T` — Nested lists with 5-level schema and def/rep levels

### P4 — Proper Nested Struct Schema (Spec Compliance) ✅
- [x] Generate GROUP schema elements for nested structs
- [x] Proper `num_children` counts at each level
- [x] Update column paths to use schema hierarchy (`["address", "city"]` not `["address.city"]`)
- [x] Tests verify schema structure and path_in_schema
- [x] `[]const NestedStruct` — Lists of structs (multi-column encoding)
- [x] Nested structs within list element structs (e.g., `[]const Outer` where `Outer` contains `Inner` struct)

### P5 — RowReader (Symmetric API) ✅
- [x] `RowReader(T)` for reading into structs
- [x] Iterator interface over rows (`next()`, `hasNext()`, `reset()`)
- [x] Schema validation against file metadata
- [x] Nested struct reconstruction from flattened columns
- [x] `readAll()` for batch reading
- [x] Memory management (`freeRow`, `freeRows`)

### P6 — Large File Support ✅
- [x] Multi-page reader: handle column chunks with multiple data pages
- [x] Multi-page writer: add `max_page_size` option to split large columns into multiple pages
- [x] Dictionary encoding for integer columns (i32, i64) via `use_dictionary` option
  - [x] Flat columns
  - [x] List element columns
  - [x] Full roundtrip with RowWriter/RowReader
  - [x] Set reasonable defaults (enabled by default, 1MB size-based fallback like Arrow/parquet-go)
  - [x] Extend to string columns (dictionary encoding with size-based fallback)
  - [x] Multi-page dictionary encoding (write multiple data pages referencing single dictionary)

### P7 — Dynamic Row API ✅
Schema-agnostic reading without comptime types. Inspired by parquet-go's Row/Value API.

- [x] `Value` tagged union (holds any parquet primitive)
  - boolean, int32, int64, float32, float64, byte_array, fixed_len_byte_array, null
- [x] `Row` type (slice of Values ordered by column index)
- [x] `DynamicReader` — read rows without knowing schema at compile time
  - `readAllRows(row_group_idx) ![]Row`
  - `getSchema() []SchemaElement`
- [x] `Row.getColumn(col_idx)` — get values for specific column
- [x] `Row.range(fn)` — iterate over columns
- [x] `decodeColumnDynamic` — runtime type dispatch for column decoding
- [x] Round-trip tests: write with RowWriter(T), read with DynamicReader
- [x] Large file test with diverse types, validated via DynamicReader
- [x] `pq validate` refactored to use DynamicReader (handles all types including lists)

### P8 — Format Compatibility ✅
Support for additional Parquet format variants found in real-world files.

- [x] DataPageV2 support
  - [x] `DataPageHeaderV2` struct with `num_values`, `num_nulls`, `num_rows`, `encoding`, level byte lengths
  - [x] Parse `data_page_header_v2` (Thrift field ID 8) in `PageHeader`
  - [x] Handle V2 page layout: uncompressed levels before compressed values
  - [x] `decodeColumnDynamicV2` for V2-specific decoding path
  - [x] Encoding validation (return `UnsupportedEncoding` for unimplemented delta encodings)
- [x] FixedByteArrayDictionary support
  - [x] `FixedByteArrayDictionary` type for dictionary-encoded fixed-length byte arrays (float16, UUID, etc.)
  - [x] Dictionary detection based on physical type, not just type parameter
  - [x] `decodeDictFixedByteArrayWithDefLevels` for typed reader
  - [x] Both typed reader (`reader.zig`) and dynamic reader (`dynamic_reader.zig`) support
- [x] Validate: float16_*.parquet files pass with both `pq validate` and `pq cat`

---

## Phase 5: Writer Refactoring

Internal cleanup to reduce duplication and improve maintainability. No API changes.

### P0 — Extract ColumnDef Module ✅
Move `ColumnDef` and related types from `writer.zig` to dedicated file.

- [x] Create `writer/column_def.zig`
  - Move `ColumnDef` struct (~70 lines)
  - Move `StructField` struct
  - Move factory methods: `list()`, `listInt32()`, `listInt64()`, `listString()`
  - Move factory methods: `map()`, `mapStringInt32()`, `mapStringInt64()`, `mapStringString()`
  - Move factory methods: `string()`, `date()`, `timestamp()`, `time()`, `decimal()`, `uuid()`
  - Move factory methods: `int8()`, `int16()`, `uint8()`, `uint16()`, `uint32()`, `uint64()`
  - Move factory methods: `float16()`, `enum_()`, `json()`
  - Move `fromNode()`, `fromSchemaNode()`, `fromSchemaNodeRecursive()`, `getPhysicalType()`
- [x] Update `writer.zig` to re-export from `column_def.zig`
- [x] Verify all tests pass

### P1 — Consolidate column_writer.zig Core Functions ✅
Removed column_name-based generic column writing functions, keeping only path array versions.

- [x] Removed `writeColumnChunk`, `writeColumnChunkMultiPage`, `writeColumnChunkMultiPageImpl` (column_name versions)
- [x] Removed `writeColumnChunkNullable`, `writeColumnChunkNullableMultiPage`, `writeColumnChunkNullableMultiPageImpl` (column_name versions)
- [x] Updated writer.zig to use `writeColumnChunkWithPathArray` with `&.{col_def.name}`
- [x] Updated tests to use path array versions

Removed ~310 lines of duplicated code from column_writer.zig.

### P2 — Consolidate column_writer.zig Byte Array Functions ✅
Removed column_name-based byte array functions, keeping only path array versions.

- [x] Removed `writeColumnChunkByteArray`, `writeColumnChunkByteArrayNullable` (column_name versions)
- [x] Removed `writeColumnChunkFixedByteArray`, `writeColumnChunkFixedByteArrayNullable` (column_name versions)
- [x] Updated writer.zig to use `WithPathArray` versions with `&.{col_def.name}`
- [x] All tests pass

Removed ~90 lines from column_writer.zig.

### P3 — Extract Shared Schema Builder (Skipped)
Skipped: RowWriter was removed in Phase 16, eliminating the original comptime/runtime unification goal. DynamicWriter still imports `Writer.countSchemaElements` and `Writer.generateSchemaFromNodeStatic` — extracting these into a shared module remains a future cleanup opportunity.

### P4 — Split column_writer.zig by Domain ✅
Split column_writer.zig (2829→1701 lines) into domain-specific modules.

- [x] Created `writer/column_write_list.zig` (~620 lines)
  - All list writing functions including dict-encoded and nested lists
- [x] Created `writer/column_write_struct.zig` (~180 lines)
  - Struct field writing functions
- [x] Created `writer/column_write_map.zig` (~220 lines)
  - Map key/value writing functions
- [x] Updated `column_writer.zig` to re-export from domain modules
- [x] Updated `writer/mod.zig` to include new modules
- [x] All tests pass

---

## Phase 6: Column Statistics

Standard Parquet feature for query optimization. Required for production-quality files.

### P0 — Write Statistics ✅
- [x] Track min/max/null_count during column writes
- [x] Write statistics in ColumnMetaData
- [x] Support for all physical types

**Implementation notes:**
- Created `writer/statistics.zig` with `StatisticsBuilder(T)` and `ByteArrayStatisticsBuilder`
- Statistics computed using PLAIN encoding (little-endian for numbers, raw for byte arrays)
- Both deprecated (`min`/`max`) and current (`min_value`/`max_value`) fields populated
- Integrated into all column writer functions (flat, nullable, dictionary, list, struct, map)
- Added 3 new tests verifying statistics correctness

### P1 — Read Statistics ✅
- [x] Parse statistics from ColumnMetaData (already existed)
- [x] `reader.getColumnStatistics(col_idx, row_group_idx)` API
- [x] Typed decode methods (`minAsI32`, `maxAsI64`, etc.) on Statistics struct
- [x] Expose via `pq stats` command with column-level min/max/nulls display

**Implementation notes:**
- Added `getColumnStatistics()`, `getColumnType()`, `getColumnMetaData()` to Reader
- Added typed accessors to Statistics: `minAsI32`, `maxAsI64`, `minAsF64`, etc.
- Enhanced `pq stats` to display per-column statistics with smart formatting
- Added 4 round-trip tests verifying statistics for i32, i64 with nulls, byte array, and double

### P2 — Round-trip Validation ✅
- [x] Write → read → verify statistics match data
- [x] Test with nulls, edge values, all types

**Implementation notes:**
- Added 6 new round-trip tests covering all remaining types:
  - `round-trip statistics boolean`
  - `round-trip statistics float` (f32)
  - `round-trip statistics fixed byte array`
  - `round-trip statistics edge values i32` (min/max int values)
  - `round-trip statistics edge values i64` (min/max int values)
  - `round-trip statistics all nulls` (verifies null_count when all values null)
- Total statistics tests: 10 (4 from P1 + 6 from P2)

---

## Phase 7: Delta Encodings ✅

Advanced Parquet encodings for improved compression of sorted or incrementally changing data.

### P0 — Test Infrastructure ✅
- [x] Generate test files with delta encodings (PyArrow)
- [x] Reference: `parquet-format/Encodings.md` for spec, `parquet-go/encoding/delta/` for implementation

### P1 — DELTA_BINARY_PACKED ✅
Efficient encoding for integer columns with incremental values.

- [x] Block structure: header (block size, miniblock count, value count, first value)
- [x] Delta computation between consecutive values
- [x] Variable bit-width packing per miniblock
- [x] Signed vs unsigned handling (zigzag encoding)
- [x] Reader support for int32 and int64
- [x] Writer support (for sorted columns)

### P2 — DELTA_LENGTH_BYTE_ARRAY ✅
For byte arrays with similar lengths.

- [x] Length deltas encoded with DELTA_BINARY_PACKED
- [x] Raw byte data concatenated after lengths
- [x] Reader support
- [x] Writer support

### P3 — DELTA_BYTE_ARRAY ✅
For byte arrays with common prefixes (sorted strings).

- [x] Prefix length + suffix length deltas
- [x] Only suffix bytes stored
- [x] Reader support
- [x] Writer support (for sorted string columns)

### P4 — BYTE_STREAM_SPLIT ✅
For floating-point columns with better compression.

- [x] Transpose bytes across values for better compression
- [x] Reader support for float and double
- [x] Writer support

**Implementation notes:**
- Created `encoding/delta_binary_packed.zig` with `Decoder(T)` supporting i32/i64
- Created `encoding/delta_length_byte_array.zig` for variable-length byte arrays
- Created `encoding/delta_byte_array.zig` for prefix-compressed strings
- Created `encoding/byte_stream_split.zig` for float/double byte transposition
- Integrated into both typed Reader and DynamicReader
- All 9 PyArrow-generated test files verified (including null handling)

---

## Phase 8: Stream Abstraction

Enables buffer-based reading for testing and WASM. See `docs/api_exploration.md`.

### P0 — SeekableReader Interface ✅
- [x] Define `SeekableReader` (vtable-based: `readAt`, `size`)
  - Reading requires random access: read footer from end, then seek to column chunks
  - `std.Io.Reader` doesn't guarantee seeking; `std.fs.File.Reader` is concrete, not an interface
- [x] `BufferReader` backend — read from `[]const u8` (for testing, WASM)
- [x] `FileReader` backend — read from `std.fs.File` (current behavior)

### P1 — Reader Refactoring ✅
- [x] Refactor Reader to use SeekableReader internally
- [x] Refactor DynamicReader to use SeekableReader
- [x] Refactor RowReader to use SeekableReader
- [x] Add convenience: `Reader.initFromBuffer()`, `Reader.initFromFile()`

**Implementation notes:**
- Created `reader/seekable_reader.zig` with vtable-based `SeekableReader` interface
- Created `reader/parquet_reader.zig` with shared `parseFooter()` and `readColumnChunkData()`
- Refactored all three readers to use `SourceBackend` union and `getSource()` method
- `getSource()` computes SeekableReader dynamically to avoid dangling pointers after struct move
- Backward compatible: `init(allocator, file)` calls `initFromFile()` internally
- Added buffer-based integration tests verifying identical behavior for file vs buffer reading

### P2 — Writer Buffer Support ✅
- [x] Use `std.Io.Writer.Allocating` for buffer-backed writing (already in std)
  - Writing is sequential, no custom interface needed
- [x] Add convenience: `Writer.initToBuffer()` returning owned `[]u8`
- [x] Add convenience: `RowWriter.initToBuffer()`
- [x] Unified `WriteTarget` interface for backend abstraction

**Implementation notes:**
- Created `writer/write_target.zig` with vtable-based `WriteTarget` interface
- `FileTarget` backend — writes to `std.fs.File` with buffered I/O
- `BufferTarget` backend — writes to in-memory buffer (for WASM/testing)
- Both `Writer` and `RowWriter` use `WriteBackend` union with `file`, `buffer`, and `external` variants
- Unified `close()` method works for all backends (no separate `finish()` needed)
- `toOwnedSlice()` retrieves buffer data after close (buffer mode only)
- `initWithTarget()` for custom backends (S3, HTTP, etc.)
- Backward compatible: `init(allocator, file, ...)` still works via `initToFile()`
- Round-trip tests verify buffer-based writing with Reader/RowReader

---

## Phase 9: Code Quality

Critical fixes and refactoring identified in code review.

### P0 — Reader Critical Bugs ✅
- [x] Fix `decodeMapColumn` multi-page support (was only reading first page)
  - Added loop with accumulators (`all_values`, `all_def_levels`, `all_rep_levels`)
  - Now iterates through all data pages in the column chunk
- [x] Fix empty row group return (was `&.{}` — use-after-free risk)
  - Changed to `try self.allocator.alloc(Optional(T), 0)` for proper allocated empty slice
- [x] Add bounds checks before slicing page data
  - Added `if (pos + compressed_size > page_data.len) return error.InvalidPageData` at all slice points
- [x] Fix list definition level encoding causing reader crash on empty lists
  - `maxDefLevel()` in `struct_utils.zig` returned 1 for `[]const i32`, making empty list (def=1) collide with present value (def=1)
  - Fix: Always use OPTIONAL repetition for list elements in schema, ensuring max_def >= 2

### P1 — Writer Critical Bugs ✅
- [x] Fix row count mismatch — return `error.RowCountMismatch` instead of silent ignore
  - Added `RowCountMismatch` to `WriterError` in `types.zig`
  - Changed 3 silent ignores in `writer.zig` to return the error
- [x] Fix `initToBuffer` memory leak — add errdefer for buffer_target
  - Added `errdefer allocator.destroy(buffer_target)` in both `Writer` and `RowWriter`
  - Added `errdefer allocator.free(columns_written)` in `Writer`
- [x] Fix `getWriter()`/`close()` panic on external target — handle gracefully
  - Changed `getWriter()` to return error instead of panic in both `Writer` and `RowWriter`
  - Refactored `close()` to handle external targets separately via `writeFooterToTarget()`
- [x] Fix empty nested column leaves undefined column_chunks
  - `writeNestedColumn()` now creates proper empty column chunks instead of leaving them null
  - Added validation in footer builders that checks both `columns_written` and `column_chunks`

### P2 — Reader Deduplication ✅
- [x] Share `SourceBackend` union across Reader, DynamicReader, RowReader
  - Moved to `parquet_reader.zig` with `getSourceFromBackend()` helper
  - All three readers now import the shared type
- [x] Extract `PageIterator` helper for shared page iteration logic
  - `PageIterator` struct in `parquet_reader.zig` handles Thrift parsing, page header extraction
  - Returns `PageInfo` with header, body, and page type flags
  - Includes `freePageHeader()` helper with proper statistics cleanup
- [x] Extract dictionary initialization helper
  - `DictionarySet` struct holds all dictionary types (string, int32, int64, fixed_byte_array)
  - `initFromPage()` handles decompression and dictionary building by physical type
  - Eliminates 3-4 separate dictionary variable declarations per function
- [x] Refactor `readColumnFromRowGroup` and `readListColumnFromRowGroup` to use new helpers
  - Uses `PageIterator` for page iteration instead of manual position tracking
  - Uses `DictionarySet` for dictionary management
  - Added `decodeFlatValuesWithDictSet()` helper for list value decoding

### P3 — Writer Deduplication ✅
- [x] Consolidate per-type write methods with comptime generics (`writeColumn(T, ...)`)
- [x] Consolidate struct field writers (`writeStructField(T, ...)`)
- [x] Merge `generateSchemaFromNode` and `generateSchemaFromNodeStatic`
- [x] Share `PARQUET_MAGIC` constant between reader and writer

**Implementation notes:**
- Added `format.PARQUET_MAGIC` as single source of truth (used by reader and both writers)
- Deleted `generateSchemaFromNode` method (identical to `generateSchemaFromNodeStatic` with unused `self`)
- Replaced 12 type-specific `writeColumn*` methods with 2 generic methods:
  - `writeColumn(comptime T: type, column_index, values)` for non-nullable
  - `writeColumnNullable(comptime T: type, column_index, values)` for nullable
- Replaced 3 `writeStructField*` methods with single generic `writeStructField(comptime T: type, ...)`
- Added `physicalTypeFor(comptime T: type)` helper for comptime type→PhysicalType mapping
- Net reduction: ~430 lines (543 deleted, 116 inserted across affected files)

### P4 — Spec Compliance ✅
- [x] Add `converted_type = 3` (LIST) to list schema generation (already present)
- [x] Multi-page support for nested types (list/map/struct)

**Implementation notes:**
- Created `*MultiPage` variants in `column_write_list.zig`, `column_write_struct.zig`, `column_write_map.zig`
- Slot-based pagination for nested types (splits on level entries, not raw values)
- Original functions delegate to `*MultiPage` with `max_page_size = null` (backward compatible)
- `RowWriter` threads `max_page_size` through to all nested column writers

### P5 — Multi-page Tests ✅
Comprehensive test coverage for multi-page writing and reading.

- [x] Add `multipage_test.zig` with tests for:
  - Plain byte array columns
  - Plain list columns (including empty and mixed empty/non-empty)
  - Nullable list columns  
  - Nested list columns
  - Compression with multi-page (Zstd, Snappy)
  - Edge cases (single value per page, all nulls)
  - Large dataset (10K rows)

### P6 — Type Safety ✅
- [x] Removed dead code: `inferSchemaNode`, `freeSchemaNode`, `freeSchemaNodeRecursive`, `schemaElementToNode`
- [x] Rename `freePageHeader` → `freePageHeaderContents` for clarity

**Implementation notes:**
- `inferSchemaNode` was unused dead code (no call sites). Removed ~155 lines.
- SchemaNode itself is still used extensively by writer, nested module, and column_def
- Renamed `freePageHeader` → `freePageHeaderContents` across all 5 reader files to clarify it frees inner allocations (Statistics), not the header struct itself

### P7 — Additional Cleanup ✅
From code review feedback.

- [x] Extract `buildSerializedFooter()` to reduce footer writing duplication
  - Added to both Writer and RowWriter
  - `writeFooterToWriter()` and `writeFooterToTarget()` now call shared method
- [x] Add `deinitArrayList(T, allocator, list)` helper for string cleanup
  - Added to `parquet_reader.zig`
  - Also added `deinitOptionalArrayList()` for Optional(T) values
  - Updated `reader.zig` to use helpers in 3 locations
- [x] Add named constants: `format.ConvertedType.LIST`, `format.ConvertedType.MAP`
  - Added full `ConvertedType` struct with all Parquet converted types
  - Replaced magic numbers in `writer.zig` and `row_writer.zig`
- [x] Add allocation limits for untrusted input
  - Added `AllocationLimits` struct with `max_dictionary_size`, `max_page_size`, `max_string_length`
  - Added `AllocationLimitExceeded` error
  - Added `initFromPageWithLimits()` to `DictionarySet`
  - Updated `RowReaderOptions` to include limits
- [x] Remove deprecated `finish()` method
  - Removed from Writer and RowWriter
  - Updated 4 tests to use `close()` + `toOwnedSlice()`

### P8 — Documentation ✅
- [x] Document `getSource()` recomputation behavior (intentional for struct move safety)
  - Expanded `getSourceFromBackend` doc comment in `parquet_reader.zig`
  - Updated brief comments in all reader files to reference main docs

### P9 — List-of-Struct Round-Trip Fixes ✅
Critical fixes for list-of-struct and map column round-trip (RowWriter → RowReader).

- [x] Fix writer def_level calculation for list-of-struct columns
  - Writer was computing `max_def = 2` as base, but Parquet spec says required list with required elements = 1
  - Changed from `var max_def: u8 = 2` to `var max_def: u8 = 1` (only repeated level contributes)
- [x] Fix column_decoder to skip level data for nested required fields
  - `decodeColumn` only checked `is_optional` to parse level data
  - For nested columns, even required leaf fields have level data due to parent's repetition level
  - Changed condition from `if (is_optional)` to `if (is_optional or ctx.max_repetition_level > 0)`
- [x] Fix RowReader element counting threshold for list-of-struct
  - Used hardcoded `dl[i] >= 2` threshold which didn't work for all schema configurations
  - Changed to dynamic threshold: `min(2, max_def)` for standard 3-level lists/maps
- [x] Add list-of-struct round-trip test in `multipage_test.zig`
  - Verifies RowWriter → RowReader works correctly for `[]const Point` fields

### P10 — Documentation Cleanup ✅
- [x] Document errdefer ownership patterns in page loops
  - Added 5-line ownership model comment block in `reader.zig`
- [x] Fix misleading "peek" comment
  - Changed to "Check if first page is a dictionary page (consumes header if so)"

---

## Phase 10: Writer Options & Metadata

Enhanced writer configuration and file metadata support.

### P0 — Encoding Options ✅
Added to `RowWriterOptions`:
- [x] `int_encoding: format.Encoding` — encoding for integer columns (default: `.plain`)
  - `.delta_binary_packed` optimal for monotonic/sequential data (timestamps, IDs, counters)
- [x] `float_encoding: format.Encoding` — encoding for float columns (default: `.plain`)
  - `.byte_stream_split` optimal for continuous measurements (sensors, GPS)
- [x] Non-nullable column support
- [x] Nullable column encoding support via `writeColumnChunkOptionalWithEncoding` (Phase 11 style unification)
- [x] Per-column encoding overrides via `column_encodings: []const ColumnEncoding` in `RowWriterOptions`

### P1 — Key-Value Metadata ✅
File-level metadata stored in footer, written at close.

- [x] Add `metadata: ?[]const format.KeyValue` to `RowWriterOptions`
- [x] Store metadata in writer struct (`std.ArrayListUnmanaged(format.KeyValue)`)
- [x] Include in `FileMetaData` during footer serialization
- [x] `setKeyValueMetadata(key, value)` method for dynamic/computed metadata
  - Caller computes hash/checksum and calls before `close()` — no special hooks needed
- [x] `pq stats` display of key-value metadata
- [x] Round-trip tests: options, setKeyValueMetadata, key update, combined

**Use cases:**
- Data provenance (source device, pipeline version)
- Integrity verification (content hash, row count checksum)
- Application-specific metadata (sample rates, coordinate systems)

---

## Phase 11: API Unification ✅

Unified nullable/non-nullable function variants using `types.Optional(T)` consistently.

**Problem solved:** Reader returns `[]Optional(T)` (custom tagged union), but Writer accepted `[]const ?T` (Zig built-in optional). This created ~16 duplicate function pairs and prevented direct read→write roundtrip.

**Results:**
- ~20 deprecated functions removed from `page_writer.zig` and `column_writer.zig`
- ~600 LOC reduction in core writer modules
- Direct read→write roundtrip now possible with `Optional(T)`
- Schema-aware definition level handling via `is_optional` parameter
- Backward-compatible wrappers in `writer.zig` for existing code

**Approach:** Bottom-up refactoring. Each layer depends on the one below, so we started at the lowest level and worked up. Added `Optional(T)` versions, made old functions thin wrappers, then removed internal deprecated functions.

### P0 — Statistics Layer ✅
Lowest level, isolated, easy to test.

- [x] Add `Optional(T)` import to `writer/statistics.zig`
- [x] `updateOptional([]const Optional(T))` added to `StatisticsBuilder(T)`
- [x] `updateOptional([]const Optional([]const u8))` added to `ByteArrayStatisticsBuilder`
- [x] Old `update`/`updateNullable` kept as deprecated wrappers (still used by list/map/struct writers)
- [x] Tests for new `updateOptional` methods

### P1 — Page Writer Layer ✅
Uses statistics, writes individual pages.

- [x] `writeDataPageOptional` added (delegates to encoding version with `.plain`)
- [x] `writeDataPageByteArrayOptional` added
- [x] `writeDataPageFixedByteArrayOptional` added
- [x] `writeDataPageWithEncoding` / `writeDataPageNullableWithEncoding` → unified as `writeDataPageOptionalWithEncoding`
- [x] `writeDataPageByteArrayWithEncoding` / `writeDataPageByteArrayNullableWithEncoding` → unified as `writeDataPageByteArrayOptionalWithEncoding`
- [x] Old functions (`writeDataPage`, `writeDataPageNullable`, etc.) marked deprecated (removed in P4)
- [x] Tests for new Optional page writer functions (6 tests)

### P2 — Column Writer Layer ✅
Uses page_writer, writes column chunks.

- [x] `writeColumnChunkOptionalWithPathArray` added (delegates to encoding version)
- [x] `writeColumnChunkOptionalWithPathArrayMultiPage` added
- [x] `writeColumnChunkDictOptionalWithPathArray` added (full implementation)
- [x] `writeColumnChunkByteArrayOptionalWithPathArray` added (delegates to encoding version)
- [x] `writeColumnChunkByteArrayOptionalWithPathArrayMultiPage` added
- [x] `writeColumnChunkByteArrayDictOptionalWithPathArray` added (full implementation)
- [x] `writeColumnChunkFixedByteArrayOptionalWithPathArray` added
- [x] `writeColumnChunkOptionalWithEncoding` (from Phase 10)
- [x] `writeColumnChunkByteArrayOptionalWithEncoding` (from Phase 10)
- [x] 14 old functions marked deprecated (removed in P4)

### P3 — Public Writer API ✅
Top-level public API, breaking change for users.

- [x] `writeColumnOptional([]const Optional(T))` added to Writer
- [x] `writeColumnFixedByteArrayOptional([]const Optional([]const u8))` added to Writer
- [x] 4 old methods marked deprecated: `writeColumn`, `writeColumnNullable`, `writeColumnFixedByteArray`, `writeColumnFixedByteArrayNullable`
- [x] Updated `writer.zig` doc comments and examples
- [x] Updated `lib.zig` doc comment example

### P4 — Call Sites & Cleanup ✅
Internal cleanup and architectural fix for definition level handling.

**Key architectural change:**
- [x] Added `is_optional: bool` parameter throughout Optional functions (page_writer → column_writer → writer)
- [x] Definition levels now controlled by schema (`col_def.optional`), not by data content
- [x] `Optional(T)` can now be used for both required and optional columns correctly

**Internal cleanup completed:**
- [x] `page_writer.zig`: Removed 6 deprecated functions:
  - `writeDataPage`, `writeDataPageNullable`
  - `writeDataPageByteArray`, `writeDataPageByteArrayNullable`
  - `writeDataPageFixedByteArray`, `writeDataPageFixedByteArrayNullable`
- [x] `column_writer.zig`: Removed 14 deprecated functions:
  - `writeColumnChunkWithPathArray`, `writeColumnChunkNullableWithPathArray`
  - `writeColumnChunkWithPathArrayMultiPage`, `writeColumnChunkNullableWithPathArrayMultiPage`
  - `writeColumnChunkDictWithPathArray`, `writeColumnChunkDictNullableWithPathArray`
  - `writeColumnChunkByteArrayWithPathArray`, `writeColumnChunkByteArrayNullableWithPathArray`
  - `writeColumnChunkByteArrayWithPathArrayMultiPage`, `writeColumnChunkByteArrayNullableWithPathArrayMultiPage`
  - `writeColumnChunkByteArrayDictWithPathArray`, `writeColumnChunkByteArrayDictNullableWithPathArray`
  - `writeColumnChunkFixedByteArrayWithPathArray`, `writeColumnChunkFixedByteArrayNullableWithPathArray`
- [x] `row_writer.zig`: Migrated to Optional APIs with proper `is_optional` parameter

**Backward-compatible wrappers kept:**
- [x] `writer.zig`: 4 deprecated methods now delegate to Optional APIs (kept for backward compatibility):
  - `writeColumn` → converts to `Optional(T)` and calls `writeColumnOptional`
  - `writeColumnNullable` → converts `?T` to `Optional(T)` and calls `writeColumnOptional`
  - `writeColumnFixedByteArray` → converts to `Optional` and calls `writeColumnFixedByteArrayOptional`
  - `writeColumnFixedByteArrayNullable` → converts `?T` to `Optional` and calls `writeColumnFixedByteArrayOptional`
- [x] `statistics.zig`: 2 deprecated methods kept (still used by list/map/struct writers):
  - `update`, `updateNullable` — will be removed when list/map/struct writers are migrated

### P5 — Optional Helper Methods ✅
Add unified `from()` method to `Optional(T)` for cleaner construction.

- [x] Add `Optional(T).from(val: anytype) Self` — unified method accepting both T and ?T
  - Uses `@typeInfo` to detect optional types at comptime
  - Handles comptime values (e.g., integer literals) correctly
- [x] Update ~22 production call sites in `writer.zig`, `row_writer.zig`, `column_decoder.zig`
- [x] Test sites left unchanged — `.{ .value = x }` already concise in array literals

**Implementation:** Single unified `from()` method with comptime type detection:
```zig
pub fn from(val: anytype) Self {
    const val_info = @typeInfo(@TypeOf(val));
    if (val_info == .optional) {
        return if (val) |v| .{ .value = v } else .null_value;
    } else {
        return .{ .value = val };
    }
}
```

**Usage:**
```zig
Optional(i32).from(42)           // from value
Optional(i32).from(maybe_null)   // from ?i32
```

---

## Phase 12: Extended Types

New logical types and precision support for broader compatibility.

### P0 — Nanosecond Precision (Write Support) ✅
Read support exists. Write support needed for Spark/Arrow compatibility (they default to nanos).

- [x] Parameterized `Timestamp(unit)` type returning struct with `.value: i64`
- [x] Parameterized `Time(unit)` type returning struct with `.value: i64`
- [x] Backward-compat aliases: `TimestampMicros = Timestamp(.micros)`, `TimeMicros = Time(.micros)`
- [x] Convenience constructors: `.from()`, `.fromNanos()`, `.fromMicros()`, `.fromMillis()`, `.fromSeconds()`
- [x] Centralized `isTimestampType()` / `isTimeType()` helpers in `struct_utils.zig`
- [x] Updated `RowWriter` schema inference for parameterized types
- [x] Updated `RowReader` wrapper handling for parameterized types
- [x] Round-trip tests for all precisions (nanos, micros, millis)

### P1 — INT96 Timestamp (Legacy Compatibility) ✅
Deprecated but widely used by Impala, older Spark, and many existing files.

- [x] `Int96` struct with 12-byte storage and Julian day conversion
- [x] `toNanos()` / `fromNanos()` for Unix epoch timestamp conversion
- [x] INT96 read support in column_decoder (`decodeDynamicInt96`)
- [x] INT96 write support in page_writer and column_writer
- [x] `TimestampInt96` wrapper type for RowWriter schema inference
- [x] RowReader INT96 handling with comptime type detection
- [x] Round-trip tests (basic, nullable, RowReader integration)
- [x] Validates `int96_from_spark.parquet` from test-files-wild

### P2 — BYTE_STREAM_SPLIT Additional Types (Parquet 2024) ✅
Extended BYTE_STREAM_SPLIT beyond FLOAT/DOUBLE per Parquet 2024 spec.

- [x] INT32 encoding/decoding
- [x] INT64 encoding/decoding
- [x] FIXED_LEN_BYTE_ARRAY encoding/decoding
- [x] FLOAT16 encoding/decoding
- [x] Update `RowWriterOptions.int_encoding` to allow `.byte_stream_split`
- [x] Test files from Arrow for each new type (`generate.py`)
- [x] Validates `test-files-wild/parquet-testing/data/byte_stream_split_extended.gzip.parquet`
- [x] Round-trip tests for i32/i64 with BSS encoding

### P2.5 — RLE Boolean Encoding ✅
RLE encoding for boolean columns (more efficient than PLAIN for runs of same value).

- [x] RLE boolean decoding in column_decoder
- [x] RLE boolean encoding in page_writer (via `rle_encoder.encodeBooleans`)
- [x] `bool_encoding` option in `RowWriterOptions` (default: `.plain`, option: `.rle`)
- [x] Validate `test-files-wild/parquet-testing/data/rle_boolean_encoding.parquet`

### P2.6 — Format Edge Cases ✅
Handle edge cases in compression and page formats found in real-world files.

See `test-files-wild/README.md` for full validation status (250/260 passing, 98.8% adjusted).

- [x] Concatenated gzip members (multiple gzip streams in single page)
  - File: `concatenated_gzip_members.parquet`
  - Fixed: Iterative zlib decompression until all data consumed
- [x] DataPageV2 edge cases
  - File: `datapage_v2_empty_datapage.snappy.parquet` — empty data pages
  - Files: `alltypes_tiny_pages*.parquet` — V2 pages with many small pages
  - File: `dms_test_table_LOAD00000001.parquet` — similar structure
  - Fixed: Boolean decoding, float/double dictionary support, INT96 dictionary support
- [x] Statistics memory management
  - Fixed: Added `deinit()` to Statistics, PageHeader to prevent memory leaks

**Additional edge cases fixed:**
- [x] `dict-page-offset-zero.parquet` — dictionary_page_offset=0 treated as "no dictionary"
  - Fix: Treat offset=0 as null (0 is before PAR1 magic, invalid file position)
- [x] `yellow_tripdata_2023-01.parquet` — dictionary fallback to PLAIN encoding
  - Fix: Check page encoding before using dictionary decoding (handles fallback pages)

**Additional edges cases identified as malformed:**
- [x] `fixed_length_byte_array.parquet` — malformed REQUIRED column with nulls
  - Status: Expected failure (matches canonical behavior)
  - File declares REQUIRED but has 105 nulls; PyArrow also fails: `OSError: Unexpected end of stream`
  - V2 pages: handled correctly (header provides explicit level byte counts to skip)
  - V1 pages: no reliable detection possible, all implementations fail similarly

### P3 — Semi-Structured Data ✅
JSON and BSON logical types for semi-structured data.

- [x] JSON logical type (BYTE_ARRAY with JSON annotation)
- [x] BSON logical type (BYTE_ARRAY with BSON annotation)
- [x] DynamicReader logical type API (`getColumnLogicalType()`, `isColumnType()`)

**Implementation notes:**
- JSON support was implemented in Phase 2 P2 (writer) with full round-trip support
- BSON mirrors JSON: `LogicalType.bson` (field 13), `ColumnDef.bson()`, `SchemaNode.bson()`
- DynamicReader can now identify column logical types for JSON/BSON/other types
- VARIANT moved to Future section (complex binary encoding, low ecosystem adoption)

### P4 — INTERVAL Type ✅
Duration/interval representation (legacy ConvertedType).

- [x] `Interval` struct with months, days, millis fields
- [x] `fromBytes()` / `toBytes()` for 12-byte little-endian encoding
- [x] `ColumnDef.interval()` factory method
- [x] Schema generation with `converted_type = INTERVAL`
- [x] Statistics skipped (sort order undefined per Parquet spec)
- [x] RowWriter/RowReader integration
- [x] Round-trip tests (required and optional)

**Implementation notes:**
- INTERVAL is a legacy `ConvertedType` (not in modern `LogicalType`)
- Stored as `FIXED_LEN_BYTE_ARRAY(12)`: 3 little-endian u32 values
- Order: months, days, milliseconds (matches Parquet spec)
- Convenience constructors: `fromMonths()`, `fromDays()`, `fromMillis()`, etc.

### P5 — Geospatial Types (Arrow GeoParquet) ✅
WKB-encoded geometry types for GIS applications.

- [x] GEOMETRY — WKB-encoded geometry (points, lines, polygons)
- [x] GEOGRAPHY — WKB-encoded geography (spherical coordinates)
- [x] Bounding box metadata in column statistics
- [x] GeoParquet metadata specification compliance
- [x] CRS (Coordinate Reference System) metadata

**Implementation notes:**
- Created `geo/` module with WKB parser, bounding box builder, and GeoParquet metadata generator
- `GeospatialStatistics` with `BoundingBox` and `geospatial_types` in column metadata
- GeoParquet 1.1 compliant JSON metadata in file footer ("geo" key)
- CRS stored as PROJJSON string in logical type and GeoParquet metadata
- Edge interpolation algorithm for GEOGRAPHY type (spherical by default)
- Validates geospatial test files from test-files-wild

### P6 — Page Checksums ✅
Data integrity validation.

- [x] CRC32 page checksums (write)
- [x] CRC32 page checksums (read validation)
- [x] Optional strict mode to reject pages with invalid checksums

**Implementation notes:**
- Added `computePageCrc()` helper using `std.hash.crc.Crc32`
- Writer: `write_page_checksum: bool = true` in `RowWriterOptions` (enabled by default)
- Reader: `ChecksumOptions` with `validate_page_checksum` and `strict_checksum` flags
- Error types: `PageChecksumMismatch`, `MissingPageChecksum`
- All 23 page header creation sites updated across column_writer, column_write_list/map/struct

### P7 — Smart Dictionary Encoding ✅
Cardinality-based dictionary encoding decisions (like Arrow/parquet-cpp).

- [x] Track unique value count during encoding
- [x] Calculate cardinality ratio (unique / total)
- [x] Auto-fallback to PLAIN when ratio > threshold (e.g., 0.5)
- [x] Add `dictionary_cardinality_threshold` option to RowWriterOptions

---

## Phase 13: Hardening

Eliminate runtime panics and segfaults when processing malformed, corrupted, or edge-case files. Return context-aware errors instead of crashing.

See `zig-parquet/HARDENING.md` for full implementation details.

### Philosophy

Zig's `@intCast` and array indexing are "promises" that values are valid. In debug builds, violations panic; in release builds, they're undefined behavior. This is fine for trusted data but dangerous when parsing untrusted external files or user-provided input.

**Both reading and writing handle untrusted data:**
- **Reading:** Files may be corrupted, maliciously crafted, or use unsupported features
- **Writing:** User code may provide out-of-range values, oversized strings, or invalid schemas

### P0 — Reader Panic Elimination ✅
- [x] Boolean decoding bounds check (`plain.zig:decodeBool`)
- [x] Level decoding bounds checks (`column_decoder.zig:decodeLevelsForDynamicWithEncoding`)
- [x] Fixed-length byte array bounds check (`column_decoder.zig:decodeRequiredColumn`)
- [x] Decompression size validation (`gzip.zig:decompress`)
- [x] Page size underflow check (`dynamic_reader.zig`)
- [x] Safe casting helpers (`extractBitWidth`, `safePageSize`, `safeRowCount`)
- [x] Thrift length validation (`compact.zig`)
- [x] RLE shift validation (`rle.zig:readVarInt`)
- [x] Delta encoding header validation (`delta_binary_packed.zig`)

**Results:** 11 panics → 0 panics across test-files-wild validation

### P1 — Safe Casting Infrastructure ✅
Created `src/safe.zig` module with safe casting helpers:
- [x] `safe.cast(val)` — returns `error.IntegerOverflow` if cast fails
- [x] `safe.castTo(TargetType, val)` — for non-usize targets
- [x] `safe.slice(data, offset, len)` — bounds-checked slicing
- [x] `safe.get(T, data, index)` — bounds-checked indexing

### P2 — Reader Casting Audit ✅
All reader code now uses `safe.cast()` instead of `@intCast`:
- [x] `reader.zig` (28 → 0 `@intCast`)
- [x] `reader/column_decoder.zig` (6 → 0)
- [x] `reader/parquet_reader.zig` (5 → 0)
- [x] `reader/dynamic_reader.zig` (3 → 0)
- [x] `reader/row_reader.zig` (4 → 0)
- [x] `reader/seekable_reader.zig` (1 → 0)

### P3 — Writer Casting Audit ✅
- [x] `writer.zig` (19 → 0 `@intCast`)
- [x] `writer/column_writer.zig` (0 usages remaining)
- [x] `writer/column_write_list.zig` (0 usages remaining)
- [x] `writer/column_write_struct.zig` (0 usages remaining)
- [x] `writer/column_write_map.zig` (0 usages remaining)
- [x] `writer/row_writer.zig` (0 usages remaining)

### P4 — Encoding/Thrift/Format Casting Audit ✅
- [x] `encoding/*.zig` (0 usages remaining)
- [x] `thrift/*.zig` (0 usages remaining)
- [x] `format/*.zig` (0 usages remaining)
- [x] `compress/*.zig` (0 usages remaining)
- [x] `types.zig` and `arrow.zig` (0 usages remaining)
- [x] Other files (0 usages remaining)

**Summary:** 320 total `@intCast` in src/ (including 78 in tests, 2 in safe.zig)
- Reader: ✅ Complete (0 remaining)
- Writer: ✅ Complete (0 remaining)
- Encoding/Format: ✅ Complete (0 remaining)
- Other: ✅ Complete (0 remaining)

### P5 — Slice & Bounds Validation Audit ✅
Convert all raw slicing and bounds-unchecked reads to use safe alternatives:
- [x] `encoding/plain.zig`: Update all decoders (`decodeU32`, `decodeByteArray`, etc.) to check lengths and return `error{EndOfData}!T`.
- [x] Reader level decoding: Fix missing bounds checks before slicing `rep_levels_data` and `def_levels_data` in `column_decoder.zig`.
- [x] Replace all raw slice operations (`\[.*\.\.\]\[0\.\..*\]`) with `safe.slice()` across the codebase (`geo/wkb.zig`, `encoding/delta_length_byte_array.zig`, etc.).
- [x] Add length validation before all `std.mem.readInt` calls (e.g., in `types.zig`, `format/statistics.zig`).

### P6 — Error Handling Refinement ✅
Eliminate remaining raw panics triggered by malformed external data:
- [x] `reader/parquet_reader.zig`: Replace `unreachable` with explicit error (e.g., `error.InvalidCompressionState`) for `.uncompressed` in `decompressPage`.
- [x] Audit remaining `unreachable` usages to ensure they cannot be triggered by external data.

### P7 — Writer Input Validation & Overflow Protection ✅
Eliminate panics when the writer receives out-of-bounds or massive user data.
- [x] Add `error.ValueTooLarge` and `error.TooManyRows` to `types.zig`.
- [x] Add `value.len > maxInt` checks for strings/byte arrays in `writer/column_writer.zig` and encodings before Thrift encoding.
- [x] Add `num_rows > maxInt` checks.
- [x] Replace `catch unreachable` in `format/*.zig` and `encoding/*.zig` with proper errors (`error.IntegerOverflow` or similar) when writing out dynamically-sized metadata.
- [x] Audit writer arithmetic (`+`) for tracking offsets and page sizes, substituting `std.math.add` where overflow could panic.
- [x] Add `.external` target support in `writer.zig:getWriter` or document why it relies on `InvalidState`.

---

## Phase 14: Comptime Type Coverage

Zig's lazy comptime evaluation means untested generic instantiations can hide compile errors and logic bugs. This phase systematically exercises all type-x-context combinations in RowWriter/RowReader.

### P0 — List-of-Struct Wrapper Type Fixes
Writer `writeListOfStructColumn` collects wrapper values (e.g. `types.Date`) but passes them to `writeColumnChunkWithLevelsAndFullPath` expecting storage types (e.g. `i32`). Compile error for any non-FLBA wrapper type.

- [x] Fix `writeListOfStructColumn` to convert Date/Timestamp/Time values to storage types before writing
- [x] Round-trip tests: list-of-struct with Date, Timestamp, Time leaf fields

### P1 — Int96 in Lists and List-of-Struct
Int96 flat columns work but `listElementStorageType` doesn't handle Int96, and the list writer has no Int96 path.

- [x] Add Int96 to `listElementStorageType` and `widenListElement` in `struct_utils.zig`
- [x] Add Int96 to allowed list element types in RowWriter validation
- [x] Add Int96 list write path (convert to 12-byte representation)
- [x] Fix reader Int96 detection for list contexts (`isInt96TimestampType` returns false for slices)
- [x] Fix `writeListOfStructColumn` for Int96 leaves (currently writes INT64 data with INT96 schema)
- [x] Round-trip tests: `[]TimestampInt96`, list-of-struct with Int96 leaf

### P2 — Nested Lists with FLBA Types
Nested lists (e.g. `[][]Uuid`) are rejected at comptime because `[16]u8` and `[12]u8` are not in the nested list allow-list, and the nested list writer doesn't handle FLBA types.

- [x] Add `[16]u8` and `[12]u8` to nested list element validation
- [x] Wire FLBA writer into nested list column path
- [x] Fix `writeNestedListColumn` hardcoded `max_def=3` — now computed from schema optionality
- [x] Fix `flattenNestedList` to use dynamic def levels instead of hardcoded 0/1/2/3
- [x] Fix reader `readNestedListField` inner-list-exists threshold for variable schema depth
- [x] Fix `freeFieldValue` memory leak for nested list elements
- [x] Round-trip tests: `[][]Uuid`, `[][]Interval`

### P3 — Optional Variant Tests ✅
Code audit confirms these paths should compile, but zero test coverage leaves them vulnerable to regressions.

- [x] `[]?Date`, `[]?Uuid`, `[]?Interval` — list of optional wrapper elements
- [x] `?[]Date`, `?[]Uuid` — optional list of wrapper elements
- [x] `?Date`, `?Uuid` — flat optional wrapper columns
- [x] Fix `flattenList` hardcoded def levels (0/1/2) — now computed dynamically from `max_def_level`

### P4 — RowWriter Map Support ✅
Maps are supported in the low-level Writer/Reader API but not in RowWriter/RowReader. Requires design for how maps are represented as Zig struct fields (e.g. `[]const MapEntry(K,V)` or similar).

- [x] Design Zig struct representation for map fields
- [x] Add map field detection in `struct_utils.zig` flattening
- [x] Add map write path in `row_writer.zig`
- [x] Add map read path in `row_reader.zig`
- [x] Round-trip tests for string→i32, string→string maps

**Implementation notes:**
- `MapEntry(K, V)` type in `types.zig` with `is_parquet_map_entry` comptime marker, `key: K`, `value: ?V`
- `struct_utils.zig`: `isMapEntryType()` detection, `flattenMapInImpl()` uses `"key_value"` path (not `"list"/"element"`), `element_optional=false`
- Map schema: `container(MAP) → key_value(REPEATED) → key(REQUIRED) + value(OPTIONAL)` — 4 elements vs 5 for list-of-struct
- Reuses existing `writeListOfStructColumn` and `readListOfStructField` — correct def/rep levels because `element_optional=false` matches map semantics
- No reader changes needed: existing list-of-struct path already handles `key_value` container names

### P5 — RowWriter Decimal Support ✅
Decimal is supported in RowReader and the low-level Writer, but not in RowWriter. Challenge: precision and scale must be known at comptime for schema inference.

- [x] Design parameterized `Decimal(precision, scale)` type (like `Timestamp(unit)`)
- [x] Add `zigTypeToParquet`, `BufferElementType`, `zigTypeToTypeLength` for Decimal
- [x] Add Decimal write path (FIXED_LEN_BYTE_ARRAY for all precisions, matching RowReader)
- [x] Fix RowReader for INT32/INT64-backed decimal columns (currently assumes FLBA)
- [x] Round-trip tests for Decimal with various precisions

**Implementation notes:**
- Existing runtime `Decimal` renamed to `DecimalValue` (for RowReader with runtime precision/scale from schema)
- New `Decimal(precision, scale)` comptime function returns a struct with `bytes: [N]u8` where N = `decimalByteLength(precision)`
- `decimalByteLength()` lookup table implements Parquet spec formula `ceil((P * log2(10) + 1) / 8)` for precisions 1-38
- `isDecimalType(T)` detection via `@hasDecl(T, "decimal_precision")` + logical type check
- Generalized FLBA write dispatch: replaced hardcoded `[16]u8`/`[12]u8` branches with `isFixedByteArray(T)` across all write paths (flat, optional, list, nested list, list-of-struct)
- INT32/INT64 decimal reader fix: `decodeDecimalInt()` in `column_decoder.zig` converts little-endian integers to big-endian byte arrays when schema logical type is DECIMAL
- Interop: files written with `Decimal(p,s)` can be read back as `DecimalValue` and vice versa

### P6 — Low-Level Map Type Gaps ✅
The typed map API (`column_write_map.zig`) only supports i32/i64/string keys and values.

- [x] Add f32, f64, bool physical type support to `column_write_map.zig` (key and value writers)
- [x] Add generic `writeMapColumn(K, V)` to `Writer`, keep `writeMapColumnStringInt32` as convenience wrapper
- [x] Add `ColumnDef` helpers: `mapStringFloat32`, `mapStringFloat64`, `mapStringBool`, `mapInt32String`, `mapInt64String`
- [x] Add bool support to reader's `decodeFlatValuesWithDictSet` (was missing for map reading)
- [x] Round-trip tests for string→f64, string→bool, i32→string, string→i64, string→f32 maps

**Implementation notes:**
- `column_write_map.zig`: extended physical type mapping and `bytes_per_slot` for both key and value writers
- `writer.zig`: new generic `writeMapColumn(K, V)` with `zigTypeToPhysicalType` helper; old `writeMapColumnStringInt32` delegates to it
- `reader.zig`: added bit-packed boolean decoding branch in `decodeFlatValuesWithDictSet`
- FLBA map support (UUID, Interval, Decimal values) intentionally omitted — users should use RowWriter which already handles them via list-of-struct

### P7 — Comptime Type Test Coverage Matrix

Systematic audit of which type-x-context combinations have round-trip test coverage. Each unchecked item represents a comptime instantiation path that may hide compile errors or logic bugs.

**Coverage matrix (Y = tested, — = no dedicated test):**

Note: Most "—" entries share physical-level code paths with tested types (e.g., `i8` uses the same INT32 write/read path as `i32`; `Timestamp(ms)` uses the same INT64 path as `Timestamp(us)`). These are unlikely to hide bugs. True risk lives in gaps where the physical type or encoding path differs from any tested combination.

| Type | Flat | `?T` | `[]T` | `?[]T` | `[]?T` | `[][]T` | Struct | Stats | Map K | Map V |
|------|:----:|:----:|:-----:|:------:|:------:|:-------:|:-----:|:-----:|:-----:|:-----:|
| `bool` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `i8` | Y | Y | Y | — | — | — | Y | Y | — | — |
| `i16` | Y | Y | Y | — | — | — | Y | Y | — | — |
| `u8` | Y | Y | — | — | — | — | Y | Y | — | — |
| `u16` | Y | Y | Y | — | — | — | Y | Y | — | — |
| `i32` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `u32` | Y | Y | Y | — | — | — | Y | Y | — | — |
| `i64` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `u64` | Y | Y | Y | — | — | — | Y | Y | — | — |
| `f32` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `f64` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `[]const u8` | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| `Date` | Y | Y | Y | Y | Y | Y | Y | Y | — | Y |
| `Timestamp(us)` | Y | Y | Y | Y | Y | Y | Y | Y | — | Y |
| `Timestamp(ms)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `Timestamp(ns)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `TimestampInt96` | Y | Y | Y | — | — | — | Y | — | — | — |
| `Time(us)` | Y | Y | Y | Y | Y | Y | Y | Y | — | Y |
| `Time(ms)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `Time(ns)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `Uuid` | Y | Y | Y | Y | Y | Y | Y | Y | — | Y |
| `Decimal(9,2)` | Y | Y | Y | Y | Y | Y | Y | Y | — | Y |
| `Decimal(18,4)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `Decimal(38,10)` | Y | Y | Y | — | — | — | — | Y | — | — |
| `Interval` | Y | Y | Y | Y | Y | Y | Y | Y* | — | Y |
| `f16` | Y | Y | Y | — | — | — | Y | Y | — | — |

\* Interval statistics test verifies they are intentionally *not* written.

**Encoding coverage:**

| Type | Plain | Delta BP | Byte Stream Split | RLE | Dictionary |
|------|:-----:|:--------:|:-----------------:|:---:|:----------:|
| `bool` | Y | n/a | n/a | Y | n/a |
| `i32` | Y | Y | Y | n/a | Y |
| `i64` | Y | Y | Y | n/a | Y |
| `f32` | Y | n/a | Y | n/a | Y |
| `f64` | Y | n/a | Y | n/a | Y |
| `[]const u8` | Y | n/a | n/a | n/a | Y |
| `Uuid` (FLBA) | Y | n/a | Y | n/a | — |
| `Interval` (FLBA) | Y | n/a | Y | n/a | — |
| `Date` (i32) | Y | Y | — | n/a | — |
| `Timestamp` (i64) | Y | Y | — | n/a | — |
| `Time` (i64) | Y | Y | — | n/a | — |
| `Decimal` (FLBA) | Y | n/a | Y | n/a | — |
| `f16` | Y | n/a | Y | n/a | — |

**PyArrow interop coverage:**

| Type | Arrow→Zig | Zig→Arrow | Writer Logical RT |
|------|:---------:|:---------:|:-----------------:|
| `bool` | Y | Y | Y |
| `i32` | Y | Y | Y |
| `i64` | Y | Y | Y |
| `f32` | Y | Y | Y |
| `f64` | Y | Y | Y |
| `[]const u8` (STRING) | Y | Y | Y |
| binary | Y | Y | Y |
| `Date` | Y | Y | Y |
| `Timestamp` | Y | Y | Y |
| `Time` | Y | Y | Y |
| `Decimal` | Y | Y | Y |
| INT types (i8/i16/u8/u16/u32/u64) | Y | Y | Y |
| `f16` | Y | Y | Y |
| JSON | Y | — | Y |
| ENUM | Y | Y | Y |
| BSON | n/a | Y | Y |
| UUID | Y | Y | Y |
| `Interval` | n/a | Y | Y |
| `TimestampInt96` | Y | Y | Y |
| Geometry | n/a | Y | Y |
| Geography | n/a | Y | Y |
| List (i32) | Y | Y | Y |
| List (i64) | Y | — | Y |
| List (f32) | Y | — | Y |
| Struct | Y | Y | Y |
| Map (string→i32) | Y | — | Y |
| Map (string→string) | Y | — | Y |
| Map (i32→i32) | Y | — | Y |
| Delta encodings | Y | — | Y |
| Byte Stream Split | Y | — | Y |

Arrow→Zig = read PyArrow-generated files in Zig tests.
Zig→Arrow = zig-written files verified by `test_interop.py`.
n/a = PyArrow cannot write this logical type.

**Gaps — Nullable flat columns (`?T`):**

- [x] `?bool`
- [x] `?i8`
- [x] `?i16`
- [x] `?u8`
- [x] `?u16`
- [x] `?u32`
- [x] `?u64`
- [x] `?f16`
- [x] `?Timestamp(micros)`
- [x] `?Timestamp(millis)`
- [x] `?Timestamp(nanos)`
- [x] `?Time(micros)`
- [x] `?Time(millis)`
- [x] `?Time(nanos)`
- [x] `?Decimal(18,4)`
- [x] `?Decimal(38,10)`

**Gaps — List columns (`[]T`):**

- [x] `[]i16`
- [x] `[]u16`
- [x] `[]f32`
- [x] `[]f16`
- [x] `[]Timestamp(millis)`
- [x] `[]Timestamp(nanos)`
- [x] `[]Time(micros)`
- [x] `[]Time(millis)`
- [x] `[]Time(nanos)`
- [x] `[]Decimal(18,4)`
- [x] `[]Decimal(38,10)`

**Gaps — Optional list (`?[]T`) and list-of-optional (`[]?T`):**

- [x] `?[]i32`, `?[]i64`, `?[]f32`, `?[]f64`, `?[]bool`
- [x] `?[]Interval`, `?[]Timestamp(*)`, `?[]Time(*)`
- [x] `?[]Decimal(9,2)`
- [x] `[]?i32`, `[]?i64`, `[]?f32`, `[]?f64`, `[]?bool`
- [x] `[]?Timestamp(*)`, `[]?Time(*)`
- [x] `[]?Decimal(9,2)`

**Gaps — Nested lists (`[][]T`):**

- [x] `[][]i32`, `[][]i64`, `[][]f32`, `[][]f64`, `[][]bool`
- [x] `[][][]const u8`
- [x] `[][]Date`, `[][]Timestamp(*)`, `[][]Time(*)`
- [x] `[][]Decimal(9,2)`

**Gaps — Struct field types:**

- [x] Struct with `bool` field
- [x] Struct with `f32` field
- [x] Struct with small int fields (`i8`, `i16`, `u8`, `u16`, `u32`, `u64`)
- [x] Struct with `Decimal` field
- [x] Struct with `f16` field

**Gaps — Statistics:**

- [x] Statistics for `Date`
- [x] Statistics for `Timestamp(*)`
- [x] Statistics for `Time(*)`
- [x] Statistics for `Uuid`
- [x] Statistics for `Decimal(*)`
- [x] Statistics for small int types (`i8`, `i16`, `u8`, `u16`, `u32`, `u64`)
- [x] Statistics for `f16`

**Gaps — Map type combinations:**

- [x] Map with `bool` key
- [x] Map with `f32` key, `f64` key
- [x] Map with `i64` key
- [x] Map with `Date` value, `Timestamp` value, `Time` value
- [x] Map with `Uuid` value, `Decimal` value, `Interval` value

**Gaps — Encoding variety (non-Plain):**

- [x] Dictionary encoding for `f32`, `f64`
- [x] Delta Binary Packed for `Date` (i32-backed), `Timestamp` (i64-backed), `Time` (i64-backed)
- [x] Byte Stream Split for `Interval` (FLBA), `Decimal` (FLBA), `f16`

**Gaps — PyArrow interop:**

- [x] PyArrow interop for `ENUM`
- [x] PyArrow interop for `BSON`
- [x] PyArrow interop for `UUID`
- [x] PyArrow interop for `Interval`
- [x] PyArrow interop for `TimestampInt96`
- [x] PyArrow interop for `Geometry`/`Geography`
- [x] Arrow→Zig: read PyArrow-generated delta encoding files (DBP int32/int64, DLBA, DBA, with nulls)
- [x] Arrow→Zig: read PyArrow-generated Byte Stream Split files (float/double/int32/int64/f16/flba, with nulls)
- [x] Zig→Arrow: PyArrow verification for broad type surface (primitives, temporal, decimal, f16, INT annotations, nested)
- [x] Zig→Arrow: PyArrow verification for `Geography`

---

## Phase 15: Release Readiness

Superseded by `zig-parquet/API_DESIGN.md`, which defined a six-stage implementation plan. All stages are complete.

### P0 — Architecture (API_DESIGN.md Stage 1–2) ✅
- [x] Fix WriteTarget gap (external targets work end-to-end)
- [x] `no_compression` build flag
- [x] Normalize constructors (file/buffer delegate to transport-neutral path)
- [x] Callback transport adapters (CallbackReader, CallbackWriter)

### P1 — Module Reorganization + Arrow Batch (API_DESIGN.md Stage 3) ✅
- [x] `core/`, `io/`, `api/` directory structure with correct dependency direction
- [x] Error set consolidation (`core/errors.zig` with categorized sets)
- [x] Arrow batch API (`readRowGroupAsArrow`, `writeRowGroupFromArrow`, schema conversion)

### P2 — C ABI (API_DESIGN.md Stage 4) ✅
- [x] Reader entry points (`zp_reader_open_memory`, `zp_reader_open_callbacks`, etc.)
- [x] Writer entry points (`zp_writer_open_memory`, `zp_writer_open_callbacks`, etc.)
- [x] Error/handle infrastructure (stable codes 0–9, per-handle error context)
- [x] C header (`libparquet.h`)
- [x] C ABI tests (round-trip, error codes, handle recovery, callback transport)

### P3 — WASM Portability (API_DESIGN.md Stage 5) ✅
- [x] `wasm32-wasi` wrapper surface
- [x] `wasm32-freestanding` wrapper surface (integer handles, host-imported IO)
- [x] Freestanding example (`examples/wasm_freestanding/`)

### P4 — API Polish ✅

User-facing API review across C, Zig, and WASM surfaces. Focuses on usability gaps, consistency issues, and missing functionality that would affect adopters.

#### C API

- [x] Add `zp_reader_open_file(path, handle_out)` / `zp_writer_open_file(path, handle_out)` convenience functions (non-WASM targets only)
- [x] Change `zp_reader_get_num_row_groups` to output-parameter pattern (`int zp_reader_get_num_row_groups(handle, int* count_out)`) for consistency with other functions
- [x] Use distinct opaque struct pointers (`typedef struct zp_reader* zp_reader_t`, `typedef struct zp_writer* zp_writer_t`) instead of `void*` for compile-time type safety
- [x] Add metadata query functions: `zp_reader_get_num_rows(handle, int64_t* out)`, `zp_reader_get_row_group_num_rows(handle, rg_index, int64_t* out)`, `zp_reader_get_column_count(handle, int* out)`
- [x] Add `zp_version()` and `zp_codec_supported(int codec)` introspection functions
- [x] Document close/free asymmetry between reader (single `close`) and writer (`close` + `free`) in header comments

#### Zig API

- [x] Reduce `lib.zig` public surface: move internal modules (`thrift`, `compress`, `encoding`, `column_decoder`, `format`, `geo`) behind a `pub const internals` namespace
- [x] Remove deprecated `writeColumn` (non-nullable) from public API (moved to `internals.writer`)
- [x] Resolve `MapEntry` vs `ReaderMapEntry` naming confusion (renamed `ReaderMapEntry` to `ColumnMapEntry`)

#### WASM API

- [x] Add Arrow accessor exports to freestanding (`zp_arrow_array_get_length`, `zp_arrow_array_get_child`, `zp_arrow_schema_get_format`, etc.)
- [x] Document and return distinct error code (`ZP_ERROR_HANDLE_LIMIT`) when 64-handle limit is exhausted in freestanding
- [x] Add `zp_writer_set_column_codec` to freestanding API
- [x] Add callback-based open functions to WASI surface (`zp_reader_open_callbacks`, `zp_writer_open_callbacks`)

#### Cross-Cutting

- [x] Add Arrow C Stream Interface support (`zp_reader_get_stream` — iterator over row groups as Arrow batches)
- [x] Add row group size / writer options (`zp_writer_set_row_group_size`) to C and WASM writer APIs

### P5 — Hardening: API & Arrow Batch ✅

Eliminated all `@intCast` from production code in the API layer and Arrow batch module, which were added after the initial hardening pass and missed the audit.

#### Arrow Batch (`arrow_batch.zig`) — 31 fixes

- [x] Replace negative i32/i64 Arrow offset casts to usize with `safe.cast()` — prevents panic on negative offsets from C callers
- [x] Replace u64→i64 cast with `safe.castTo` — prevents overflow on large unsigned values from Arrow arrays
- [x] Replace usize→i32 offset casts with `safe.castTo` — prevents overflow when element count exceeds 2B
- [x] Replace safe widening casts (i8/u8/i16/u16→i32, u32→i64) with `safe.castTo` + `catch unreachable` with invariant comments
- [x] Replace usize→i64 ArrowArray field casts with `safe.castTo` + `catch unreachable`
- [x] Replace decimal precision casts with `safe.castTo` + `catch unreachable` referencing branch guards

#### C API — 5 fixes

- [x] `writer.zig`: Replace codec `@intCast` with `safe.castTo` for untrusted C caller input
- [x] `row_writer.zig`: Replace guarded `crs_len` casts with `safe.castTo` + `catch unreachable`
- [x] `row_writer.zig`: Replace `toUsize` helper with `safe.castTo`
- [x] `row_writer.zig`: Replace internal `type_length` cast with `safe.castTo` + `catch unreachable`

#### WASM API — 10 fixes

- [x] `wasi.zig`: Mirror all C API fixes (codec cast, crs_len, toUsize helper, type_length)
- [x] `freestanding.zig`: Mirror all C API fixes

#### Documentation

- [x] Update HARDENING.md `@intCast` elimination table with Arrow Batch, C API, and WASM API rows
- [x] Update test count from ~80 to ~96 (including arrow_batch.zig test blocks)

### P6 — Nested Type Gap Analysis & Fixes ✅

Systematic audit of nested type support (LIST, MAP, STRUCT) across all API layers — Row, Column, DynamicReader, C ABI — identifying and fixing bugs and test coverage gaps.

#### Bug Fixes — 5 fixes

- [x] Fix `list_encoder.flattenNestedList` conflating definition levels for null elements vs empty inner lists in `[][]?T` patterns
  - Added `leaf_element_optional` parameter to distinguish null element (def=max-1) from empty inner list (def=max-1-delta)
- [x] Fix `row_reader.buildInnerList` miscounting elements in optional-element nested lists
  - Changed to dynamic `elem_threshold = max_def - 1` for optional elements
- [x] Fix `nested.assembleRecursive` map assembly with incorrect `def_threshold` for empty maps under `optional` wrapper
  - Changed null map check from `def <= def_threshold` to `def < def_threshold`
- [x] Fix `nested.assembleRecursive` ignoring `rep_threshold` in list/map assembly loops
  - Now uses `current_rep <= rep_threshold` as break condition and propagates to recursive calls
- [x] Fix `handles.getStructFields` not navigating through `list`/`map` nodes for C ABI `list<struct>` writing
  - Extended to unwrap `list` and `map` schema nodes when resolving struct fields

#### Missing Tests Added

- [x] `[][]?i32` and `?[][]i32` round-trip tests in `roundtrip_test.zig`
- [x] DynamicReader `map<string, i32>` round-trip test in `nested_test.zig`
- [x] Multi-page map column test in `multipage_test.zig`
- [x] Multi-page struct-with-list field test in `multipage_test.zig`
- [x] C ABI reader: nested list (`list<list<int32>>`) and list-of-struct tests in `c_abi_test.zig`
- [x] C ABI writer: nested list and list-of-struct round-trip tests in `c_abi_test.zig`

#### PyArrow Interop Files Added

- [x] `list_int64.parquet` — `pa.list_(pa.int64())` with boundary values
- [x] `list_f32.parquet` — `pa.list_(pa.float32())` with special values (inf, -inf)
- [x] `map_string_string.parquet` — `pa.map_(pa.string(), pa.string())` with unicode
- [x] `map_int_int.parquet` — `pa.map_(pa.int32(), pa.int32())`
- [x] Zig read-back tests for all four new files in `row_reader_test.zig`

### P7 — Nested Type Test Coverage & CI Fix ✅

Follow-up to P6. Fixed CI pipeline, removed 55 force-tracked generated files from git, and added comprehensive DynamicReader and deep-composition round-trip tests.

#### CI Fix

- [x] Add `setup-uv` + `uv run python generate.py` steps to `test.yml` and `release.yml` (generate PyArrow test files in CI)
- [x] Update `test-files-arrow/.gitignore` with `!interop/*.parquet` negation (keep Zig-generated interop files tracked)
- [x] Remove 55 force-tracked PyArrow-generated `.parquet` files and `manifest.json` from git (`git rm --cached`)

#### DynamicReader Gap Tests (4 tests in `nested_test.zig`)

- [x] `list<list<i32>>` — nested list with empty inner/outer lists
- [x] `optional<list<i32>>` — nullable list with null rows
- [x] `list<optional<i32>>` — list with null elements
- [x] `map<string, i32>` with null rows — maps are inherently OPTIONAL in generated Parquet schema

#### Deep Composition Round-Trip Tests (4 tests in `nested_test.zig`)

- [x] `map<string, list<i32>>` — map with list values
- [x] `list<map<string, i32>>` — list of maps
- [x] `struct<id:i32, meta:map<string, i32>>` — struct with map field
- [x] `map<string, struct<x:i32, y:i32>>` — map with struct values

#### C ABI Composition Test (1 test in `c_abi_test.zig`)

- [x] `struct<id:i32, tags:list<i32>>` — write with RowWriter, read with C ABI reader

### P8 — Nested Type Bug Fixes ✅

Four correctness and hardening fixes for nested type support.

#### FLBA Nested Writer Fix (`column_writer.zig`)

- [x] Add `fixed_byte_array` variant to `ValuePhysicalType` enum
- [x] Route `fixed_bytes_val` to `writeDataPageWithLevelsFixedByteArray` (no length prefixes) instead of `writeDataPageWithLevelsByteArray`
- [x] UUID round-trip test now verifies read-back data (was previously skipped due to this bug)

#### 2-Level List Schema Reconstruction (`schema.zig`)

- [x] Fix `buildListNode` to detect 2-level nested LIST/MAP encoding (repeated child with `converted_type=LIST/MAP`)
- [x] Follows Arrow C++ approach: if the repeated child is itself a LIST or MAP, treat it as the element rather than a 3-level wrapper
- [x] Add schema test for legacy `list<list<int32>>` encoding
- [x] Add file-based test reading `old_list_structure.parquet` from parquet-testing

#### `optional<map>` Def-Level Mismatch (`writer.zig`, `schema.zig`, `nested.zig`)

- [x] Map container now uses `rep_type` parameter instead of hardcoded `.optional` in `generateSchemaFromNodeStatic`
- [x] `computeLeafLevelsRecursive` `.map` branch reduced from +2/+3 to +1/+2 (key_value group only; container optionality comes from `.optional` wrapper)
- [x] `flattenRecursive` `.map` branch reduced from +2 to +1 for non-empty maps; empty maps no longer add extra def
- [x] All bare `SchemaNode.map` usages in tests wrapped in `.optional` to match Parquet convention
- [x] New `optional<map>` round-trip test verifying null, empty, and populated maps are distinguishable

#### Recursion Depth Limit (`nested.zig`)

- [x] Add `max_nesting_depth = 64` constant
- [x] `flattenRecursive` and `assembleRecursive` now accept a `depth` parameter and return `error.NestingTooDeep` when exceeded
- [x] Prevents stack overflow on pathologically deep schemas

---

## Phase 16: API Simplification ✅

Replaced comptime `RowWriter(T)` / `RowReader(T)` with runtime `DynamicWriter` / `DynamicReader`. The comptime row API could not handle deeply nested types (`list<list<T>>`, `struct<list>`, `list<map>`, etc.) due to Zig comptime limitations. The runtime API supports all types and arbitrary nesting depth with zero limitations.

### P0 — Create DynamicWriter Core ✅

Extracted `DynamicWriter` from C ABI `RowWriterHandle` into `zig-parquet/src/core/dynamic_writer.zig`:

- [x] Accept user-provided `Allocator` (not hardcoded `page_allocator`)
- [x] Accept `WriteTarget` directly (transport-neutral)
- [x] Zig-idiomatic method signatures: `setBytes(col, slice)` not `setBytes(col, ptr, len)`
- [x] `TypeInfo` named constants (`TypeInfo.string`, `TypeInfo.int32`, etc.) and builders (`TypeInfo.forDecimal`)
- [x] All nested builders: `beginList/endList`, `beginStruct/endStruct`, `beginMap/endMap`
- [x] Convenience constructors: `createFileDynamic`, `createBufferDynamic`

### P1 — Wire Up and Delete ✅

- [x] Refactored C ABI `RowWriterHandle` to delegate to `DynamicWriter`
- [x] Refactored WASM API `RowWriterHandle` to delegate to `DynamicWriter`
- [x] Updated `lib.zig` exports: removed `RowWriter`, `RowReader`, logical type wrappers; added `DynamicWriter`, `TypeInfo`, `ColumnType`
- [x] Deleted `row_writer.zig` (2,076 lines), `row_reader.zig` (1,685 lines), `struct_utils.zig` (736 lines)
- [x] Deleted `row_writer_test.zig` (2,013 lines), `row_reader_test.zig` (3,800 lines)

### P2 — Update Consumers ✅

- [x] Rewrote 8 test files to use `DynamicWriter` / `DynamicReader`
- [x] Rewrote 7 examples to use `DynamicWriter` / `DynamicReader`
- [x] Updated README.md Quick Start, features list, nested types section
- [x] Updated ROADMAP.md with Phase 16

**Net result:** ~10,300 lines of comptime code removed, ~400 lines of `DynamicWriter` added. All 597 tests pass.

### P3 — Per-Column and Per-Leaf Properties ✅

Added `ColumnProperties` struct and `setPathProperties` for fine-grained control over individual leaf columns within nested structures:

- [x] `ColumnProperties` struct with `compression`, `encoding`, `use_dictionary`, `dictionary_size_limit`, `max_page_size`
- [x] `addColumn` / `addColumnNested` accept `ColumnProperties` as third argument
- [x] `setPathProperties(path, props)` for per-leaf overrides using dot-joined paths (e.g., `"address.city"`)
- [x] `resolveOpts` merges global → column → path properties (most specific wins)
- [x] `ColumnProperties` exported from `lib.zig` alongside `Encoding` and `CompressionCodec`
- [x] C ABI and WASM API updated to pass defaults

### P4 — Nested Encoding Options ✅

Extended `writeColumnChunkFromValues` to accept and apply all encoding options for nested leaf columns. Previously, nested leaves were hardcoded to PLAIN encoding regardless of configuration.

- [x] Added `writeDataPageWithLevelsAndEncoding` and `writeDataPageWithLevelsByteArrayWithEncoding` to `page_writer.zig`
- [x] Added `NestedEncodingOpts` struct to `column_writer.zig`
- [x] Extended `writeColumnChunkFromValues` and typed helpers to branch on encoding options
- [x] Dictionary-with-levels for typed and byte array nested columns (builds dictionary, writes dict page + RLE-encoded data pages with multi-level def/rep)
- [x] Encoding override and int/float encoding for nested typed columns
- [x] Encoding override for nested byte array and fixed byte array columns
- [x] Updated `writeColumnChunkWithPath` to record correct encoding in metadata (was hardcoded to `.plain`)
- [x] Wired `flushNestedColumn` to pass resolved `EncodingOpts` through to `NestedEncodingOpts`
- [x] Tests verifying dictionary, delta, and encoding metadata for nested leaves
