# API Design

Status: draft

This document describes how `zig-parquet` should structure its internals so the same Parquet implementation can cleanly support the Zig API, a future C-facing ABI, `wasm32-wasi`, and `wasm32-freestanding`.

The goal is an internal architecture boundary, not a package split. Everything remains in the same repository and module tree.

## Problem Statement

The Parquet logic itself should not need to know whether bytes come from a file, a caller-owned memory buffer, a browser host callback, an archive member, or a container runtime. When transport concerns leak into the core, each new API surface grows its own lifecycle rules and duplicate entry points.

The desired direction is: one Parquet core, thin transport adapters, thin surface-specific APIs.

## Design Goals

- Keep one implementation of Parquet semantics, validation, encoding, decoding, and schema logic.
- Make transport a first-class internal boundary.
- Let the caller own file access, path resolution, sandbox constraints, and host integration.
- Preserve ergonomic Zig APIs for common native use cases.
- Make a future C-facing ABI natural for embedders that already manage their own IO.
- Support `wasm32-wasi` and `wasm32-freestanding` without forcing filesystem assumptions into the core.
- Keep hardening rules intact: malformed input must return errors rather than panic.

## Non-Goals

- Splitting `zig-parquet` into multiple packages or repositories.
- Designing the full exported WASM ABI in this first draft.

## Architecture

### Core Principle

The Parquet engine should operate on abstract byte sources and byte sinks, not on paths or environment-specific IO APIs.

- Read-side core code accepts a random-access source abstraction (`SeekableReader`).
- Write-side core code accepts an output target abstraction (`WriteTarget`).
- File opening, memory ownership, callback bridging, and host bindings live outside the core.

### Layers

**1. Parquet Core** — owns footer parsing, page parsing, column decoding/encoding, row assembly/flattening, schema handling, logical type handling, compression integration, validation/hardening, allocator-driven memory management, Arrow C Data Interface conversion, and the Parquet ↔ Arrow batch API (runtime type dispatch for reading/writing row groups as Arrow arrays). Must not own path opening, cwd semantics, sandbox/container policy, or host wiring. Allowed dependencies: `SeekableReader`, `WriteTarget`, pure data/config structs.

**2. Transport Adapters** — own file-backed, memory-backed, and callback-backed readers and writers, plus ownership wrappers and optional convenience path helpers. Examples: `FileReader`, `BufferReader`, `CallbackReader`, `FileTarget`, `BufferTarget`, `CallbackWriter`.

**3. Surface APIs** — own ergonomic constructors, ABI-specific handle layout, error translation and context capture, exported symbol naming, lifecycle conventions, and data access patterns (e.g., the C ABI cursor/iterator over row groups returning Arrow arrays). Error context (column names, row group indices, byte offsets in human-readable messages) is a surface concern, not a core concern. Surfaces: Zig API, future C-facing ABI, WASI-facing API, freestanding WASM API.

### Boundary Rules

**Rule 1: Core code does not open files.** Core modules should not call `openFile`, `createFile`, or depend on paths. They receive already-constructed transport objects.

**Rule 2: Core code does not own host policy.** Where data comes from, whether file access is allowed, how WASM imports are wired — all outside core.

**Rule 3: Surface APIs may add convenience, not alternate logic.** Each surface can provide convenience constructors that delegate into the same transport-neutral core path.

**Rule 4: Hardening remains a core responsibility.** Validation of untrusted data belongs in the Parquet engine regardless of byte source.

## Current State Audit

### Read Side

The read-side transport abstraction is well-established:

- `SeekableReader` provides vtable-based dispatch with `readAt(offset, buf)` and `size()`.
- `BufferReader` and `FileReader` implement the vtable.
- `Reader`, `DynamicReader`, and `RowReader` all converge on `SeekableReader` at runtime through `getSourceFromBackend()`.
- `initFromSeekable(...)` exists on all three readers.

**Gap:** `initFromFile` and `initFromBuffer` do not delegate to `initFromSeekable`. They build `SourceBackend` variants and call shared helpers directly. The code paths converge at `parseFooter(allocator, source)` and all subsequent reads, so the semantics are identical, but the constructors are parallel rather than layered. The fix is straightforward: have file/buffer constructors build the adapter, then call the seekable constructor.

### Write Side

The write-side transport abstraction exists but has a fundamental gap:

- `WriteTarget` provides vtable-based dispatch with `write(data)` and `close()`.
- `FileTarget` and `BufferTarget` implement the vtable.
- `Writer.initWithTarget(...)` exists and accepts an external `WriteTarget`.

**Gap:** The column write path requires `*std.Io.Writer` (obtained via `getWriter()`), but `WriteTarget` does not expose one. This means:

- `Writer.initWithTarget` can write the PAR1 magic and the footer through `WriteTarget.write()`, but column data writes go through `getWriter()` which returns `error.ExternalTargetNotSupported` for the `.external` backend variant.
- External targets on `Writer` are therefore broken for actual column writing — only footer writing works.

**Gap:** `RowWriter(T)` has no `initWithTarget` / `initToTarget` constructor at all. The `WriteBackend.external` variant exists in the union but is never constructed. `getWriter()` returns `ExternalTargetNotSupported` for it, so `flush()` (which writes column data) would fail.

**Resolution path:** The column write path needs to work through `WriteTarget` rather than requiring `*std.Io.Writer`. This means either:

1. Adding a `writer()` method to `WriteTarget` that returns a `*std.Io.Writer` (wrapping the vtable write), or
2. Refactoring `column_writer.writeColumnChunk*` to accept `WriteTarget` instead of `*std.Io.Writer`.

Option 1 is lower-risk because it doesn't change the column writer internals. `WriteTarget` would own a small adapter struct that bridges its `write(data)` vtable call into the `*std.Io.Writer` interface.

### Write Buffering Strategy

The writer emits data incrementally, not buffered-then-flushed:

- Column data is written immediately through `*std.Io.Writer` as each column chunk is encoded.
- `current_offset` tracks the byte position for footer metadata.
- The footer (serialized Thrift `FileMetaData` + 4-byte length + `PAR1` magic) is written at `close()`.
- Footer metadata references column chunks by their recorded byte offsets.

This means the write side is append-only in practice. No seeking is needed. The `WriteTarget` interface correctly reflects this — `write` + `close` is sufficient for all current writer modes.

### Constructor Naming

The document uses `initToTarget` as the canonical write-side constructor name. The codebase uses `initWithTarget`. This document adopts `initWithTarget` to match the existing code. If renamed later, it should be a single coordinated change.

### Error Handling

The codebase uses Zig's native error unions throughout. Error sets are defined per-module with significant overlap between `ReaderError`, `DynamicReaderError`, `RowReaderError`, and `WriterError`. Transport errors (`SeekableReader.Error`, `WriteTarget.WriteError`) are small and focused.

**Gap:** No error context mechanism exists. Errors are bare tags with no associated column name, row group index, or byte offset. This is adequate for the Zig API but insufficient for the C ABI's `zp_error_message(handle)` requirement. See the Error Handling section for the resolution.

**Gap:** `DynamicReader` coalesces many distinct errors into `SchemaParseError` via an `else` catch, losing diagnostic information. This should be improved during error set consolidation.

**Not a gap:** The three-way error category split (transport, format, caller) is correct and should be preserved through the redesign.

### Arrow C Data Interface

`arrow.zig` implements the `ArrowSchema` and `ArrowArray` structs with format strings, validity bitmaps, and release callbacks. It also provides `ArrowColumn(T)`, a Zig helper for typed columnar data.

**Gap:** `arrow.zig` provides the Arrow types but no Parquet ↔ Arrow conversion logic. There is no function to decode a Parquet column into an `ArrowArray`, and no function to consume an `ArrowArray` and write it as a Parquet column. This conversion layer is required for the C ABI.

### API Levels and C ABI Scope

The codebase has three reader/writer API levels:

| API | Orientation | Type dispatch | Best for |
|-----|-------------|---------------|----------|
| **Column-level** (Reader/Writer) | Column | Comptime (`[]const T`) | Full control, typed Zig usage |
| **Row-level** (RowReader/RowWriter) | Row | Comptime (struct `T`) | Ergonomic Zig usage |
| **Dynamic** (DynamicReader) | Row | Runtime (`Value`) | Schema-agnostic tools (`pq`) |

The C ABI needs a fourth level that doesn't exist yet: **columnar with runtime type dispatch and Arrow conversion.** Here's why:

- **RowWriter/RowReader are wrong for the C ABI.** Arrow is columnar. Using row-level APIs would require transposing Arrow columns → rows → columns, which is pointless overhead.
- **Column-level Reader/Writer are close but use comptime types.** `writeColumn` takes `[]const T` where `T` is known at compile time. The C ABI receives `ArrowArray` with a runtime-determined type from `ArrowSchema`. There is no comptime `T` available.
- **DynamicReader does runtime type dispatch** but outputs `Value`, not `ArrowArray`. Converting `Value` → `ArrowArray` adds unnecessary intermediate representation.

The missing layer is a **batch API** in core that operates on row groups as sets of columns with runtime type dispatch:

- **Read path:** `readRowGroupAsArrow(allocator, source, metadata, rg_index, col_indices) → ArrowArray` — decodes selected columns from a row group directly into Arrow arrays. Uses the same column decoder internals as the existing Reader, but dispatches on schema metadata at runtime instead of comptime `T`.
- **Write path:** `writeRowGroupFromArrow(allocator, target, schema, arrow_arrays) → void` — encodes Arrow arrays into Parquet column chunks. Uses the same column encoder internals as the existing Writer, but dispatches on Arrow format strings at runtime.

This layer lives in `core/` because it's Parquet logic (decode/encode), not surface logic. The C ABI surface in `api/c/` is a thin wrapper that manages handles, error context, and ABI conventions on top of this batch API.

This batch API is also useful for the Zig API — Zig users who want Arrow interop can use it directly without going through the C ABI.

### What Doesn't Exist Yet

- Callback-backed `SeekableReader` adapter
- Callback-backed `WriteTarget` adapter
- Parquet ↔ Arrow conversion layer (batch API with runtime type dispatch)
- C ABI surface
- WASM-specific wrapper surface
- `src/core/`, `src/io/`, `src/api/` directory structure

## Surface Validation

### Comparison Matrix

| Surface | Natural input model | Natural output model | Data exchange format | Filesystem assumptions |
|---------|---------------------|----------------------|---------------------|------------------------|
| Zig API | File or borrowed memory | File or owned memory target | Native Zig types, optional Arrow | Optional convenience |
| Future C-facing ABI | Borrowed memory or callbacks | Callbacks or owned memory buffer | Arrow C Data Interface | Should be optional |
| `wasm32-wasi` | Borrowed memory, optionally files | Owned memory or callbacks | Arrow C Data Interface or raw buffers | Conditional |
| `wasm32-freestanding` | Borrowed memory or imported callbacks | Owned memory or imported callbacks | Arrow C Data Interface or raw buffers | None |

### Key Observations Per Surface

**Zig API** — The existing two-layer shape (convenience constructors + transport-neutral constructors) is correct. The main work is making convenience constructors actually delegate to the transport-neutral path rather than being parallel implementations.

**Future C-facing ABI** — Caller-owned data access should be the primary model: open from memory buffer, open from caller-provided random-access callbacks, write to callbacks or owned output buffer. Decoded data should be returned as Arrow C Data Interface arrays (see C ABI Data Access Pattern). Path-based functions, if they exist, should be convenience wrappers. The C caller owns file handles, callback userdata, and host buffers. The library owns Parquet state and internal allocations.

**`wasm32-wasi`** — Can support file-oriented convenience APIs where the host runtime allows, but should flow through the same transport-neutral core. Memory-based APIs are the portable baseline.

**`wasm32-freestanding`** — The strongest constraint and best stress test. No filesystem, no host runtime beyond imported functions. If the design works here without distorting the core, it works everywhere. The primary API shape is memory- or callback-based exported functions.

### Consumer Validation

**`pq` CLI** — File-oriented by nature. Owns command-line parsing, path handling, file opening, stdout formatting. Uses the library for Parquet semantics only. The redesign fits cleanly: `pq` continues to open files and pass them to convenience constructors. No changes needed to `pq`'s usage patterns.

**Examples** — Already exercise file-based, buffer-based, and WASM usage. The redesign preserves all existing patterns. The WASM demo (`examples/wasm_demo/`) is a practical validation of the transport-neutral principle — it uses only buffer-backed entry points.

### Cross-Surface Invariants

Identical across all surfaces: Parquet format support, validation/hardening, schema interpretation, encoding/decoding logic, metadata parsing, Arrow conversion logic, error conditions at the semantic level.

Varies by surface only: handle shapes, ABI details, memory ownership conventions, convenience constructors, error transport format and context detail (see Error Handling), data access patterns (Zig iterators vs C cursor vs WASM exports).

## Constructor Model

Three levels, from lowest to highest:

### 1. Engine-Level Constructors (internal source of truth)

- `Reader.initFromSeekable(allocator, source)`
- `DynamicReader.initFromSeekable(allocator, source)`
- `RowReader(T).initFromSeekable(allocator, source, options)`
- `Writer.initWithTarget(allocator, target, columns, options)`
- `RowWriter(T).initWithTarget(allocator, target, options)`

### 2. Native Convenience Constructors (Zig wrappers)

- `Reader.initFromFile(allocator, file)` — creates `FileReader`, delegates to `initFromSeekable`
- `Reader.initFromBuffer(allocator, data)` — creates `BufferReader`, delegates to `initFromSeekable`
- `Writer.initToFile(allocator, file, ...)` — creates `FileTarget`, delegates to `initWithTarget`
- `Writer.initToBuffer(allocator, ...)` — creates `BufferTarget`, delegates to `initWithTarget`
- Same pattern for `RowReader`, `RowWriter`, `DynamicReader`.

### 3. ABI/Host Constructors (surface-specific wrappers)

- `zp_reader_open_memory(...)` — future C ABI, returns opaque handle
- `zp_reader_open_callbacks(...)` — future C ABI, returns opaque handle
- `zp_reader_get_schema(handle, ...)` — retrieve schema as `ArrowSchema`
- `zp_reader_read_row_group(handle, ...)` — decode row group as `ArrowArray`
- `zp_writer_open_*(...)`, `zp_writer_write_row_group(...)` — write-side equivalents
- Freestanding WASM exports for parse/read/write against host memory

See C ABI Data Access Pattern for the full cursor/iterator design.

## Callback Transport Design

### Read-Side Callback

The callback-backed reader maps directly onto `SeekableReader`'s vtable:

```zig
pub const CallbackReader = struct {
    ctx: *anyopaque,
    read_at_fn: *const fn (ctx: *anyopaque, offset: u64, out: []u8) SeekableReader.Error!usize,
    size_fn: *const fn (ctx: *anyopaque) u64,

    const vtable = SeekableReader.VTable{
        .readAt = callbackReadAt,
        .size = callbackSize,
    };

    fn callbackReadAt(ptr: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *CallbackReader = @ptrCast(@alignCast(ptr));
        return self.read_at_fn(self.ctx, offset, buf);
    }

    fn callbackSize(ptr: *anyopaque) u64 {
        const self: *CallbackReader = @ptrCast(@alignCast(ptr));
        return self.size_fn(self.ctx);
    }

    pub fn reader(self: *CallbackReader) SeekableReader {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};
```

Semantics:

- `size()` returns the total byte size of the source.
- `read_at(offset, out)` fills up to `out.len` bytes starting at `offset`, returns bytes read.
- Short reads are allowed at end-of-data.
- Transport failures map to `SeekableReader.Error`, not Parquet semantic errors.

### Write-Side Callback

The callback-backed writer maps onto `WriteTarget`'s vtable:

```zig
pub const CallbackWriter = struct {
    ctx: *anyopaque,
    write_fn: *const fn (ctx: *anyopaque, data: []const u8) WriteTarget.WriteError!void,
    close_fn: ?*const fn (ctx: *anyopaque) WriteTarget.WriteError!void = null,

    const vtable = WriteTarget.VTable{
        .write = callbackWrite,
        .close = callbackClose,
    };

    fn callbackWrite(ptr: *anyopaque, data: []const u8) WriteTarget.WriteError!void {
        const self: *CallbackWriter = @ptrCast(@alignCast(ptr));
        return self.write_fn(self.ctx, data);
    }

    fn callbackClose(ptr: *anyopaque) WriteTarget.WriteError!void {
        const self: *CallbackWriter = @ptrCast(@alignCast(ptr));
        if (self.close_fn) |f| return f(self.ctx);
    }

    pub fn target(self: *CallbackWriter) WriteTarget {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};
```

Semantics:

- `write(data)` appends exactly `data.len` bytes or returns an error. No partial writes.
- `close()` is invoked once at normal completion if provided.
- The core writer does not need to know whether the sink is a file, network target, or in-memory collector.

### Ownership Rules

- The caller owns the callback context.
- The caller guarantees the context outlives the reader or writer using it.
- The library never frees the callback context.
- The adapter owns only the wrapper struct.

### Error Separation

- Callback layer reports IO/transport failure only.
- Core reports Parquet format and validation failure.
- Callbacks should never report Parquet-specific errors (invalid magic, unsupported encoding, etc.).

## C ABI Data Access Pattern

The transport layer defines how bytes flow in and out. The data access layer defines how SDKs actually consume decoded Parquet data. Both must be designed together for the C ABI to be useful to language bindings.

### Arrow C Data Interface as primary output

The C ABI should return decoded data through the Arrow C Data Interface (`ArrowSchema` + `ArrowArray`). The codebase implements these structures in `arrow.zig`. The Parquet ↔ Arrow conversion logic (see API Levels and C ABI Scope in Current State Audit) provides the internal implementation that the C ABI wraps.

This is the standard columnar data exchange protocol used by Python (pyarrow), R (arrow), Julia, Rust, Go, and others. Exposing decoded data as Arrow arrays means:

- Python gets `pyarrow.Table` and pandas DataFrames without SDK-side type conversion
- R gets native Arrow arrays
- Any language with Arrow bindings gets zero-copy access
- SDK authors don't need to write per-type deserialization for every Parquet physical/logical type combination

The alternative — returning raw buffers and type tags — forces every SDK to reimplement Arrow-equivalent type mapping, which is expensive to build and error-prone.

### Cursor/iterator pattern

The C ABI should expose a row-group-oriented cursor:

1. `zp_reader_open_*(...)` — open from memory or callbacks, returns opaque handle
2. `zp_reader_get_schema(handle, ArrowSchema* out)` — retrieve schema as Arrow
3. `zp_reader_get_num_row_groups(handle)` — for progress/planning
4. `zp_reader_read_row_group(handle, rg_index, column_indices, num_columns, ArrowArray* out)` — decode selected columns from a row group as Arrow arrays
5. `zp_reader_close(handle)` — release resources

This pattern gives SDKs control over:

- **Column selection** — only decode what's needed
- **Memory pressure** — one row group at a time, free between groups
- **Parallelism** — SDK can read row groups concurrently with multiple handles
- **Progress reporting** — SDK knows total row groups and current position
- **Error isolation** — a corrupted row group does not prevent reading other row groups (see Error Handling for handle state after errors)

### Write-side data input

The write-side C ABI should accept data as Arrow arrays for the same reasons:

1. `zp_writer_open_*(...)` — open to memory or callbacks, returns opaque handle
2. `zp_writer_set_schema(handle, ArrowSchema*)` — define output schema from Arrow
3. `zp_writer_write_row_group(handle, ArrowArray*)` — write a batch of rows
4. `zp_writer_close(handle)` — finalize footer and release resources

### String and byte data across the boundary

All strings (column names, metadata keys/values, string column values) cross the C ABI as length-prefixed byte pointers (`const uint8_t* data, size_t len`), not null-terminated C strings. This avoids encoding assumptions and handles embedded nulls in binary data.

Borrowed strings (e.g., from schema inspection) are valid until the next mutating call on the same handle or until the handle is closed. SDKs should copy if they need to retain them.

### C callback signatures

The Zig-internal callback types use `*anyopaque` and Zig slices. The C ABI translation uses standard C conventions:

```c
typedef int (*zp_read_at_fn)(void* ctx, uint64_t offset, uint8_t* buf, size_t buf_len, size_t* bytes_read);
typedef uint64_t (*zp_size_fn)(void* ctx);
typedef int (*zp_write_fn)(void* ctx, const uint8_t* data, size_t len);
typedef int (*zp_close_fn)(void* ctx);
```

Return `0` for success, non-zero error code for failure. Out-params for results. The C ABI wrapper translates these into the internal Zig callback adapter types.

### Async integration

The C ABI is synchronous. SDKs that need async behavior (Node.js, Python asyncio) should run C ABI calls in worker threads. The library guarantees:

- No internal threading — safe to call from any single thread
- No shared mutable state between handles — safe to use separate handles from separate threads
- Callbacks are invoked synchronously and sequentially within a single C ABI call

This is the standard pattern for compute-bound native libraries (SQLite, zlib, etc.) and avoids pulling async runtime concerns into the library.

## Ownership Model

**Core engine owns:** internal decode/encode buffers, returned decoded allocations (where documented).

**Adapters/surface APIs own:** file handles, caller context pointers, borrowed input slices, ABI-specific handles, memory buffers returned to foreign callers.

The core does not need to know whether the underlying source is borrowed, reference-counted, or host-managed.

## Error Handling

### Current State

Zig errors are bare enum tags with no payload. The codebase defines several overlapping error sets:

- `ReaderError` (~40 tags) — covers IO, format, Thrift, compression, and checksum errors
- `WriterError` (~15 tags) — covers IO, state, type, and compression errors
- `DynamicReaderError`, `RowReaderError` — subsets of `ReaderError` with reader-specific additions
- `SeekableReader.Error` — transport only: `InputOutput`, `Unseekable`
- `WriteTarget.WriteError` — transport only: `WriteError`, `OutOfMemory`, `IntegerOverflow`
- Module-level error sets in `safe.zig`, `compress/`, `encoding/`, `thrift/`

Errors are propagated by return value. There is no error logging, context mechanism, or stack trace capture.

### Error Categories

Errors fall into three categories, distinguished by tag rather than by type hierarchy:

| Category | Examples | Responsibility |
|----------|----------|---------------|
| **Transport / IO** | `InputOutput`, `Unseekable`, `WriteError`, `BrokenPipe` | Transport adapters report these |
| **Format / data** | `InvalidMagic`, `FileTooSmall`, `InvalidPageData`, `EndOfData`, `DecompressionError`, `PageChecksumMismatch` | Core reports these during parsing/decoding |
| **Caller / API** | `InvalidColumnIndex`, `TypeMismatch`, `InvalidState`, `SchemaMismatch`, `TooManyRows` | Core reports these for API misuse |

This separation is correct and should be preserved. Transport errors originate in adapters and callbacks. Format errors originate in core. Caller errors originate in core's public method validation.

### Error Context Problem

Zig's error model carries no payload. When the core returns `error.InvalidPageData`, there is no information about which page, column, row group, or byte offset triggered the failure. For the Zig API, this is tolerable — Zig users can attach context with `errdefer` or error return traces. For the C ABI, it's inadequate — `zp_error_message(handle)` is useless if the library can't generate a meaningful message.

**Resolution:** Add a per-handle error context mechanism in the C ABI layer. When translating Zig errors to C status codes, the ABI wrapper captures context (column index, row group index, byte offset where available) and formats a human-readable message. This context is surface-specific — it lives in `src/api/c/`, not in core.

For the Zig API, the existing error tag model is sufficient. Zig callers can use error return traces (`@errorReturnTrace()`) for debugging. Adding structured error context to the core would add overhead to every error path for a benefit that primarily serves the C ABI.

### Error Coalescing

Some reader types lose information by mapping many errors into catch-all tags. Notably, `DynamicReader` maps any `ReaderError` not in a small explicit list to `SchemaParseError`:

```zig
else => error.SchemaParseError,
```

This loses the original cause (e.g., `EndOfData`, `InvalidFieldType`, `VarIntTooLong` all become `SchemaParseError`). This should be improved as error sets are consolidated — either by expanding the explicit list or by making `DynamicReaderError` a superset of the errors it can actually encounter.

### Error Set Consolidation

The current error sets overlap significantly. `ReaderError` includes IO errors that also appear in `SeekableReader.Error`. `WriterError` includes errors that also appear in `WriteTarget.WriteError`. The redesign should consolidate toward:

- **Core error set** — format parsing, encoding, schema, validation errors. Defined in `core/`.
- **Transport error set** — IO failures. Defined alongside `SeekableReader`/`WriteTarget` in `core/`.
- **Surface error sets** — each surface (Zig API, C ABI) can compose these. The Zig API can use Zig's error set unions directly. The C ABI maps to integer codes.

This consolidation does not need to happen immediately. It should be done during Phase 4 (module reorganization) when import paths are already changing.

### Handle State After Errors

For the C ABI, callers need to know whether a handle is still usable after an error:

**Reader handles:**
- Errors during `zp_reader_open_*` — handle is not created. No cleanup needed.
- Errors during `zp_reader_read_row_group` — handle remains valid. The caller can retry with a different row group or different columns. A corrupted row group does not poison the handle.
- Errors during `zp_reader_get_schema` — handle remains valid (schema is parsed at open time; this is a format/export error).

**Writer handles:**
- Errors during `zp_writer_open_*` — handle is not created. No cleanup needed.
- Errors during `zp_writer_write_row_group` — depends on error type. Data errors (`ZP_ERROR_INVALID_DATA`, `ZP_ERROR_INVALID_ARGUMENT`, `ZP_ERROR_SCHEMA`) are detected before writing; handle remains valid, caller can fix data and retry. Transport errors (`ZP_ERROR_IO`) may leave the output stream corrupted; handle should be closed (see Write Atomicity).
- Errors during `zp_writer_close` — if footer writing fails, the output is incomplete. The handle is consumed regardless.

The general rule: **reader handles survive all errors; writer handles survive data errors but not transport errors.**

### Partial Success in Row Group Reads

`zp_reader_read_row_group` is all-or-nothing: it either decodes all requested columns successfully or returns an error with no partial output. This is simpler and safer than returning partial results where the SDK would need to track which columns succeeded.

SDKs that want to isolate per-column failures should issue separate `zp_reader_read_row_group` calls with individual column indices. This gives the SDK full control over fallback behavior (skip unsupported columns, substitute nulls, etc.) without complicating the C ABI.

### Write Atomicity

Row group writes are **not atomic** at the transport level. The writer emits column data incrementally — if a transport error occurs mid-write, the output stream contains a partial row group and is not a valid Parquet file.

Data errors (encoding failure, type mismatch) are detected **before** writing to the output stream, because encoding happens in memory before the encoded bytes are emitted. This means:

- **Transport errors** (`ZP_ERROR_IO`): output stream is corrupted. Close the handle. Do not retry on the same stream.
- **Data errors** (`ZP_ERROR_INVALID_DATA`, `ZP_ERROR_INVALID_ARGUMENT`): nothing was written. Handle is valid. Caller can fix the data and retry.

SDKs should document this distinction. A Python SDK might raise `ParquetIOError` (fatal, close writer) vs `ParquetDataError` (fixable, retry).

### C ABI Error Mapping

Zig's error sets are comptime constructs without stable numeric values. The C ABI maps them to stable integer status codes:

| Code | Category | Meaning | SDK action |
|------|----------|---------|------------|
| `0` | `ZP_OK` | Success | — |
| `1` | `ZP_ERROR_INVALID_DATA` | Malformed Parquet structure (corrupt pages, invalid encoding) | Report to user, skip row group if partial read desired |
| `2` | `ZP_ERROR_NOT_PARQUET` | Not a Parquet file (bad magic bytes, file too small) | Raise a distinct "not a Parquet file" exception |
| `3` | `ZP_ERROR_IO` | Transport-level read/write failure | Retry if source is transient (network), fatal otherwise |
| `4` | `ZP_ERROR_OUT_OF_MEMORY` | Allocation failure | Reduce batch size or fail |
| `5` | `ZP_ERROR_UNSUPPORTED` | Valid Parquet feature or codec not available | Skip column/row group, or upgrade library |
| `6` | `ZP_ERROR_INVALID_ARGUMENT` | Caller passed invalid arguments (bad column index, null pointer) | Fix caller code |
| `7` | `ZP_ERROR_INVALID_STATE` | Operation called in wrong lifecycle state (write after close) | Fix caller code |
| `8` | `ZP_ERROR_CHECKSUM` | Page checksum verification failed | Data corruption, skip row group or re-obtain file |
| `9` | `ZP_ERROR_SCHEMA` | Schema-level error (incompatible types, missing columns) | Fix schema or input data |

Error codes are assigned explicit integer values and are **stable across library versions**. New codes may be added but existing codes will not change meaning. SDKs should handle unknown codes as generic failures.

The C ABI provides:

- `zp_error_message(handle) → const char*` — human-readable description including context (column name, row group index, byte offset where available). Stored per-handle, valid until the next API call on that handle.
- `zp_error_code(handle) → int` — the most recent error code, for programmatic access without re-parsing the message.

SDKs should map these to language-native exceptions:

| C ABI code | Python exception | Node.js error | Go error |
|------------|-----------------|---------------|----------|
| `ZP_ERROR_NOT_PARQUET` | `NotParquetFileError` | `NotParquetError` | `ErrNotParquet` |
| `ZP_ERROR_INVALID_DATA` | `CorruptDataError` | `CorruptDataError` | `ErrCorruptData` |
| `ZP_ERROR_IO` | `ParquetIOError` | `IOError` | `ErrIO` |
| `ZP_ERROR_UNSUPPORTED` | `UnsupportedFeatureError` | `UnsupportedError` | `ErrUnsupported` |
| `ZP_ERROR_SCHEMA` | `SchemaError` | `SchemaError` | `ErrSchema` |
| `ZP_ERROR_CHECKSUM` | `ChecksumError` | `ChecksumError` | `ErrChecksum` |
| `ZP_ERROR_OUT_OF_MEMORY` | `MemoryError` | `RangeError` | `ErrOutOfMemory` |
| `ZP_ERROR_INVALID_ARGUMENT` | `ValueError` | `TypeError` | `ErrInvalidArgument` |
| `ZP_ERROR_INVALID_STATE` | `RuntimeError` | `Error` | `ErrInvalidState` |

The mapping layer lives in `src/api/c/` and translates at the ABI boundary. The Zig API continues to use native error sets.

## Concurrency Model

Single-threaded access per handle. The library does not synchronize internally. If callers need to share a reader or writer across threads, they must provide their own synchronization.

Callback functions must not be invoked concurrently by the library for a given handle. Callers can rely on callbacks being invoked sequentially.

This is the simplest correct model and avoids pulling threading concerns into the core. If parallel column decoding is added later, it should be opt-in and internal to the engine, not visible in the transport contract.

## Module Organization

Target directory structure:

```
src/
├── core/           # Parquet engine: format parsing, schema, encoding, decoding,
│                   # row assembly, validation, errors, shared config,
│                   # Parquet ↔ Arrow batch API (runtime type dispatch)
├── io/             # Transport: SeekableReader, WriteTarget, file/buffer/callback adapters
├── api/
│   ├── zig/        # Ergonomic Zig constructors and public re-exports
│   ├── c/          # C-facing ABI wrappers, opaque handle policy
│   └── wasm/       # WASI and freestanding wrapper surfaces
└── lib.zig         # Public Zig entry point (re-exports from api/zig/)
```

### Dependency Direction

The rule is: `core/` never imports from `io/`, `api/`, or `lib.zig`. `io/` never imports from `api/`. `api/` surfaces import from `core/` and `io/`.

Transport interfaces (`SeekableReader`, `WriteTarget`) live in `core/` because core code depends on them. Transport adapters (File, Buffer, Callback implementations) live in `io/` because they implement core interfaces using environment-specific IO. This is standard dependency inversion: core defines the abstraction, io provides the implementation.

### Migration Approach

Move files incrementally, enforcing dependency direction at each step. `lib.zig` remains the stable public Zig entry surface throughout.

The current top-level `reader.zig` and `writer.zig` contain both the public `Reader`/`Writer` struct definitions (with all methods) and construction logic. These need to be split: the struct and its core methods move to `core/`, while convenience constructors that create transport adapters move to `api/zig/`.

Tests that import internal modules directly (e.g., `@import("../reader/row_reader.zig")`) must have their import paths updated as files move. This should be done file-by-file alongside each move, not deferred.

### File Mapping

| Current location | Target location | Notes |
|-----------------|-----------------|-------|
| `reader/seekable_reader.zig` | `core/seekable_reader.zig` | Interface, used by core |
| `writer/write_target.zig` | `core/write_target.zig` | Interface, used by core |
| `reader/parquet_reader.zig` | `core/parquet_reader.zig` | |
| `reader/row_reader.zig` | `core/row_reader.zig` | |
| `reader/dynamic_reader.zig` | `core/dynamic_reader.zig` | |
| `reader/column_decoder.zig` | `core/column_decoder.zig` | |
| `reader/list_decoder.zig` | `core/list_decoder.zig` | |
| `reader/map_decoder.zig` | `core/map_decoder.zig` | |
| `writer/column_writer.zig` | `core/column_writer.zig` | |
| `writer/page_writer.zig` | `core/page_writer.zig` | |
| `writer/row_writer.zig` | `core/row_writer.zig` | |
| `writer/column_def.zig` | `core/column_def.zig` | |
| `writer/statistics.zig` | `core/statistics.zig` | |
| `writer/column_write_list.zig` | `core/column_write_list.zig` | |
| `writer/column_write_struct.zig` | `core/column_write_struct.zig` | |
| `writer/column_write_map.zig` | `core/column_write_map.zig` | |
| `writer/list_encoder.zig` | `core/list_encoder.zig` | |
| `writer/map_encoder.zig` | `core/map_encoder.zig` | |
| `safe.zig` | `core/safe.zig` | Shared utility |
| `types.zig` | `core/types.zig` | Shared utility |
| `format.zig` | `core/format.zig` | Re-exports from format/ |
| `format/` | `core/format/` | Format type definitions |
| `schema.zig` | `core/schema.zig` | Shared utility |
| `value.zig` | `core/value.zig` | Shared utility |
| `nested.zig` | `core/nested.zig` | Shared utility |
| `struct_utils.zig` | `core/struct_utils.zig` | Comptime struct helpers |
| `encoding/` | `core/encoding/` | |
| `compress/` | `core/compress/` | |
| `thrift/` | `core/thrift/` | |
| `geo/` | `core/geo/` | |
| `arrow.zig` | `core/arrow.zig` | Arrow C Data Interface types |
| New file | `core/arrow_batch.zig` | Parquet ↔ Arrow batch API (readRowGroupAsArrow, writeRowGroupFromArrow) |
| `reader.zig` (top-level) | Split: core methods → `core/`, convenience constructors → `api/zig/reader.zig` |
| `writer.zig` (top-level) | Split: core methods → `core/`, convenience constructors → `api/zig/writer.zig` |
| New file | `io/file_reader.zig` | `FileReader` adapter (extracted from seekable_reader.zig) |
| New file | `io/buffer_reader.zig` | `BufferReader` adapter (extracted from seekable_reader.zig) |
| New file | `io/callback_reader.zig` | `CallbackReader` adapter (new) |
| New file | `io/file_target.zig` | `FileTarget` adapter (extracted from write_target.zig) |
| New file | `io/buffer_target.zig` | `BufferTarget` adapter (extracted from write_target.zig) |
| New file | `io/callback_writer.zig` | `CallbackWriter` adapter (new) |

## Compression

### Why Compression Belongs in Core

Compression is fundamentally different from transport. Transport is about *where bytes come from* — that's environment policy, and the right abstraction is runtime injection (`SeekableReader`, `WriteTarget`). Compression is about *what the bytes mean* — it's Parquet format semantics. A zstd-compressed page is meaningless without decompression, the same way a delta-encoded column is meaningless without delta decoding.

Making compression injectable (like transport) would mean every reader init call needs a codec registry passed in. But you don't know which codecs a file uses until you read the metadata, and a single file can use different codecs per column. The common case — all codecs available — would become boilerplate. Decompressing untrusted data is also security-relevant; keeping it in core keeps the hardening boundary clear.

The right abstraction boundary is *build configuration*, not *runtime injection*. You choose at compile time which codecs your binary supports. The `compress/mod.zig` dispatch stays in core, individual codec implementations are conditionally compiled, and missing codecs return `error.UnsupportedCompression`.

### Current Architecture

Compression is dispatched through `compress/mod.zig` which provides `compress()` and `decompress()` functions. Dispatch uses a comptime `switch` on `format.CompressionCodec`. Individual codec modules (`zstd.zig`, `gzip.zig`, `lz4.zig`, `brotli.zig`, `snappy.zig`) are only imported by `mod.zig` — reader and writer code never references specific codecs directly.

Compression is per-column, not per-file. Each `ColumnDef` specifies a `codec` field (default: `.uncompressed`). On the read side, the codec is read from `ColumnMetaData` in the Parquet footer and dispatched automatically.

### All Codecs Are C/C++

Every supported codec (zstd, gzip, snappy, lz4, brotli) is implemented via C/C++ libraries linked at build time. There are no pure-Zig codec implementations. This has direct consequences for portability:

- **Native targets**: all codecs work, C/C++ linking is standard.
- **`wasm32-wasi`**: codecs can work if the WASM toolchain supports C/C++ compilation to WASM. The existing `wasm_demo` exercises this path.
- **`wasm32-freestanding`**: C/C++ linking may not be available or may produce unacceptable binary sizes. Without codecs, only uncompressed Parquet files can be read or written.

### Compile-Time Codec Control

The `build.zig` does not currently have a `no_compression` flag or per-codec flags — all codecs are always compiled. This needs to change to support constrained targets.

Required build options:

- `no_compression` — disable all codecs, only `.uncompressed` is supported. `compress()` returns `error.UnsupportedCompression` for any other codec.
- Optionally, per-codec flags (e.g., `enable_zstd`, `enable_snappy`) for targets that can link some but not all C/C++ dependencies.

The codec set is fixed at compile time. The C ABI cannot add codecs at runtime — embedders get whatever was compiled in.

### Compression in the C ABI

The C ABI data access pattern needs to address compression in two places:

**Writer configuration:** When writing through `zp_writer_set_schema` or a separate configuration call, the caller must be able to specify per-column compression. Options:

- Encode codec in the `ArrowSchema` metadata (non-standard but practical).
- Provide a separate `zp_writer_set_column_codec(handle, col_index, codec)` function.
- Accept a configuration struct alongside the schema.

The second option (explicit function) is simplest and most C-friendly.

**Reader behavior:** Decompression is automatic based on file metadata. No configuration needed. If the file uses a codec that was not compiled in, the reader returns `ZP_ERROR_UNSUPPORTED` for that column/row group.

### Limitations for Constrained Targets

When built with `no_compression`:

- Writing: only `.uncompressed` output. Attempting to set a codec returns an error at configuration time.
- Reading: files with uncompressed columns work normally. Files with compressed columns return `error.UnsupportedCompression` (mapped to `ZP_ERROR_UNSUPPORTED` in the C ABI) when the affected column is decoded.
- This is a known and expected limitation, not a bug.

## Implementation Plan

The plan is organized into six stages. Within each stage, work items can run in parallel. A stage must complete before the next stage begins (except where noted).

### Stage 1: Foundation

Two independent work items. No prerequisites.

**1A: Fix the WriteTarget gap**

External `WriteTarget` works end-to-end for both `Writer` and `RowWriter`.

1. Add a `writer()` method to `WriteTarget` that returns `*std.Io.Writer` by bridging the vtable `write` call into the `std.Io.Writer` interface. This is a small adapter struct stored alongside the `WriteTarget`.
2. Update `Writer.getWriter()` to return the bridged writer for the `.external` backend variant instead of `error.ExternalTargetNotSupported`.
3. Add `RowWriter(T).initWithTarget(allocator, target, options)` that constructs with an external `WriteTarget`.
4. Update `RowWriter.getWriter()` to use the same bridged writer for `.external`.
5. Remove `ExternalTargetNotSupported` from the codebase.

Verification: test that creates `Writer` with `initWithTarget`, writes column data, writes footer, reads back. Same for `RowWriter`. All existing tests pass.

**1B: `no_compression` build flag**

Implement in `build.zig`. When enabled, `compress/mod.zig` returns `error.UnsupportedCompression` for all codecs and C/C++ codec sources are not compiled (see Compression section).

Verification: library builds and passes tests with `-Dno_compression`. Compressed files return `UnsupportedCompression`.

### Stage 2: Transport normalization

Two independent work items. Both depend on Stage 1A.

**2A: Normalize constructors**

File/buffer constructors become thin wrappers over transport-neutral constructors.

1. Refactor `Reader.initFromFile` to create a `FileReader`, store it, then call `initFromSeekable` with the resulting `SeekableReader`.
2. Refactor `Reader.initFromBuffer` to create a `BufferReader`, store it, then call `initFromSeekable`.
3. Same refactor for `DynamicReader.initFromFile` / `initFromBuffer`.
4. Same refactor for `RowReader.initFromFile` / `initFromBuffer` — these already partially converge through `initWithSource`, so the change is smaller.
5. Refactor `Writer.initToFile` and `initToBuffer` to create the transport adapter, then delegate to `initWithTarget`.
6. Refactor `RowWriter.initToFile` and `initToBuffer` to delegate to `initWithTarget` (added in Stage 1A).

Key constraint: The `SourceBackend` union currently stores owned `FileReader`/`BufferReader` inline to avoid separate heap allocation. The refactored constructors must preserve this — they should build the backend, store it in the union, then derive the `SeekableReader` from the stored backend. The existing `getSourceFromBackend()` pattern (recompute on each call to avoid dangling pointers after struct moves) remains correct.

Verification: all existing tests pass without modification (behavior is identical, only constructor routing changed). Examples build. `pq` builds.

**2B: Add callback transport adapters**

Callback-backed IO as a first-class transport.

1. Add `CallbackReader` struct (see Callback Transport Design) that implements the `SeekableReader` vtable by forwarding to caller-provided function pointers.
2. Add `CallbackWriter` struct that implements the `WriteTarget` vtable by forwarding to caller-provided function pointers.
3. Add tests that use callback adapters backed by in-memory arrays (proving the same reader/writer code works through callbacks).
4. Expose `CallbackReader` and `CallbackWriter` through `lib.zig`.

Verification: callback-backed round-trip tests (write to memory via callbacks, read back, verify). No changes to existing file/buffer tests.

### Stage 3: Restructure and Arrow batch API

Two work items that can run in parallel. Both depend on Stage 2.

**3A: Module reorganization**

Source files organized into `core/`, `io/`, `api/` directories. Purely structural — no functional changes. Every step must leave all tests, examples, and `pq` building.

Actions, in order:

1. Create target directories: `src/core/`, `src/io/`, `src/api/zig/`, `src/api/c/`, `src/api/wasm/`.
2. Move shared utilities first (lowest dependency count): `safe.zig`, `types.zig` → `core/`. Update all import paths. Build and test.
3. Move `SeekableReader` and `WriteTarget` interfaces (without adapter implementations) → `core/`. Update import paths. Build and test.
4. Extract `FileReader`, `BufferReader` from `seekable_reader.zig` into `io/file_reader.zig`, `io/buffer_reader.zig`. Extract `FileTarget`, `BufferTarget` from `write_target.zig` into `io/file_target.zig`, `io/buffer_target.zig`. Move `CallbackReader`, `CallbackWriter` → `io/`. Update imports. Build and test.
5. Move encoding, compression, thrift, format subsystems → `core/`. Update imports. Build and test.
6. Move remaining core files: `parquet_reader.zig`, `column_decoder.zig`, `row_reader.zig`, `dynamic_reader.zig`, `column_writer.zig`, `page_writer.zig`, `row_writer.zig`, etc. → `core/`. Update imports. Build and test.
7. Split top-level `reader.zig`: core Reader struct → `core/reader.zig`, convenience constructors → `api/zig/reader.zig`. Split top-level `writer.zig` similarly. Update imports. Build and test.
8. Move `arrow.zig`, `schema.zig`, `value.zig`, `nested.zig`, `struct_utils.zig`, `format.zig`, `geo/` → `core/`. Update imports. Build and test.
9. Consolidate error sets: define `core/errors.zig` with a core error set (format, encoding, schema, validation) and a transport error set (IO). Migrate existing error sets in `types.zig`, reader modules, and writer modules to compose from these. Remove duplicated error tags. Fix `DynamicReader`'s `else => SchemaParseError` coalescing.
10. Update `lib.zig` to re-export from `api/zig/` and `core/` as needed.
11. Update `tests/` import paths if any still reference old locations.

Key constraint: build and run the full test suite after every step. If a step breaks something, fix it before moving to the next file. Do not batch moves.

Verification: full test suite, `pq`, and all examples pass after each step. No file in `core/` imports from `io/` or `api/`. No file in `io/` imports from `api/`.

**3B: Parquet ↔ Arrow batch API**

A core batch API that reads and writes row groups as Arrow arrays with runtime type dispatch. This is new code — it can be built against the current file layout while 3A is in progress, then moved to `core/arrow_batch.zig` when 3A completes.

The read path, write path, and schema conversion are independent sub-items that can themselves be split across contributors.

1. Implement `readRowGroupAsArrow(allocator, source, metadata, rg_index, col_indices) → ArrowArray` in `core/`. This should:
   - Read column chunks using the existing column decoder internals.
   - Dispatch on `ColumnMetaData` physical/logical type at runtime (not comptime).
   - Construct `ArrowArray` with appropriate format strings, validity bitmaps, and data buffers.
   - Support flat types (int32, int64, float, double, boolean, binary, string), logical types (date, timestamp, decimal, UUID), and nested types (lists, structs, maps as nested Arrow arrays).
2. Implement `writeRowGroupFromArrow(allocator, target, schema, arrow_arrays) → void` in `core/`. This should:
   - Accept `ArrowArray` with `ArrowSchema` describing the column types.
   - Dispatch on Arrow format strings at runtime.
   - Encode into Parquet column chunks using existing encoder internals.
   - Support the same type set as the read path.
3. Implement `exportSchemaAsArrow(metadata) → ArrowSchema` for converting Parquet file metadata into Arrow schema.
4. Implement `importSchemaFromArrow(ArrowSchema) → []ColumnDef` for converting Arrow schema into Parquet column definitions.

Verification: round-trip tests (Parquet → Arrow → verify; Arrow → Parquet → verify). Nested type round-trip. Schema conversion round-trip.

### Stage 4: C ABI

Depends on Stage 3 (both 3A and 3B). Reader and writer entry points are independent and can be built in parallel. Error infrastructure is independent of both.

**4A: C ABI reader entry points**

- `zp_reader_open_memory(data, len, handle_out) → int`
- `zp_reader_open_callbacks(ctx, read_at_fn, size_fn, handle_out) → int`
- `zp_reader_get_num_row_groups(handle) → int`
- `zp_reader_get_schema(handle, ArrowSchema* out) → int`
- `zp_reader_read_row_group(handle, rg_index, col_indices, num_cols, ArrowArray* out) → int`
- `zp_reader_close(handle)`

**4B: C ABI writer entry points**

- `zp_writer_open_memory(handle_out) → int`
- `zp_writer_open_callbacks(ctx, write_fn, close_fn, handle_out) → int`
- `zp_writer_set_schema(handle, ArrowSchema*) → int`
- `zp_writer_set_column_codec(handle, col_index, codec) → int`
- `zp_writer_write_row_group(handle, ArrowArray*) → int` — all-or-nothing: data errors detected before writing to output stream
- `zp_writer_get_buffer(handle, data_out, len_out) → int`
- `zp_writer_close(handle) → int`

**4C: C ABI error and handle infrastructure**

Can start as soon as 3A provides `api/c/`, before 3B completes.

1. Define opaque handle types. A reader handle wraps: allocator, `SeekableReader` (or adapter owning the source), parsed footer metadata, and per-handle error state. A writer handle wraps: allocator, `WriteTarget` (or adapter owning the sink), schema, writer state, and per-handle error state.
2. Map Zig error unions to stable `ZP_OK` / `ZP_ERROR_*` integer codes (codes 0–9, see Error Handling).
3. Implement per-handle error context capture — store column name, row group index, and byte offset where available.
4. Expose `zp_error_message(handle) → const char*` and `zp_error_code(handle) → int`.
5. Implement handle-state-after-error rules: reader handles survive all errors; writer handles survive data errors but not transport errors.

**4D: Header generation and integration**

Depends on 4A, 4B, 4C.

Generate `libparquet.h` from exported symbols.

Verification (for all of Stage 4):

- C smoke test: open a known Parquet file from memory, read schema, read each row group as Arrow arrays, verify expected values.
- C smoke test: create a writer, set schema, set column codecs, write row groups from Arrow arrays, close, then read back and verify round-trip.
- C callback smoke test: same operations using callback-based open.
- Error handling tests: verify `zp_error_code` and `zp_error_message` return correct codes and context for at least: `ZP_ERROR_NOT_PARQUET` (bad magic), `ZP_ERROR_INVALID_DATA` (corrupt page), `ZP_ERROR_UNSUPPORTED` (unimplemented codec), `ZP_ERROR_INVALID_ARGUMENT` (bad column index).
- Handle recovery test: reader handle continues to work after a failed `zp_reader_read_row_group`. Writer handle becomes unusable after a simulated transport error during `zp_writer_write_row_group`.
- Valgrind/asan: verify no leaks or undefined behavior in the C ABI layer.

### Stage 5: WASM wrappers

Depends on Stage 3 (module structure and batch API). Can run in parallel with or after Stage 4.

**5A: `wasm32-wasi` wrapper**

Define a wrapper surface in `src/api/wasm/` that exposes the same function set as the C ABI, using WASI-compatible calling conventions. Verify the existing `wasm_demo` continues to work through the buffer-based path.

**5B: `wasm32-freestanding` wrapper**

Define export functions that accept memory offsets and lengths instead of pointers (since the host manages linear memory). Map imported host functions (`read_at`, `write`, `size`) to `CallbackReader`/`CallbackWriter` internally. Depends on Stage 1B (`no_compression` flag).

Verification:

- `examples/wasm_demo/` builds and runs under `wasm32-wasi`.
- A minimal freestanding test builds with `wasm32-freestanding` and `-Dno_compression`.
- No core code has target-specific branches.

### Dependency Summary

```
Stage 1: [1A: WriteTarget fix] [1B: no_compression]
              │
Stage 2: [2A: Normalize constructors] [2B: Callback adapters]
              │
Stage 3: [3A: Module reorg] [3B: Arrow batch API ←── can start during 3A]
              │
Stage 4: [4A: C reader] [4B: C writer] [4C: Error infra ←── can start during 3B]
              │                              │
          [4D: Header gen + integration]     │
              │                              │
Stage 5: [5A: WASI wrapper] [5B: Freestanding ←── needs 1B]
```

### Cross-Phase Constraints

- `pq` remains a thin file-oriented consumer of the public Zig API.
- Examples remain teachable — not forced to callback-level abstractions.
- Hardening behavior does not diverge by transport.
- New abstractions reduce duplicated logic rather than introducing parallel code paths.
- Public Zig ergonomics do not regress.

### Cross-Phase Verification Set

Run after each phase:

- Library unit tests (`zig build test`)
- `pq` build and basic command coverage (`pq schema`, `pq head`, `pq cat` on a known file)
- `examples/basic/` buildability
- `examples/wasm_demo/` buildability

## Open Questions

### Should callback-backed transport be part of the public Zig API?

**Trade-offs:** Making it public is low-cost (it's just a struct with function pointers) and enables Zig embedders with custom IO backends (database engines, object stores, archive readers). Making it internal reduces the public API surface but forces advanced users to work around the limitation. **Suggested default:** Public. Advanced Zig users who embed the library are a real audience.

### Should the write abstraction distinguish streaming vs seekable sinks?

**Trade-offs:** The current append-only model works for all existing writer modes. A seekable write target would enable features like in-place footer updates or offset patching, but Parquet's format doesn't require this. Adding seek would complicate the callback contract for minimal benefit. **Suggested default:** Keep append-only. If a future feature requires seek, introduce a separate `SeekableWriteTarget` rather than complicating the common path.

### Should path-based convenience exist in the C ABI?

**Trade-offs:** Path helpers are convenient for simple C programs but bake in filesystem assumptions and platform-specific path encoding. Most serious C embedders already have their own file IO. **Suggested default:** Omit from the initial C ABI. Add later only if real users request it.

### How should compression work for `wasm32-freestanding`?

**Trade-offs:** All codecs are C/C++ libraries. `wasm32-freestanding` may not support full C/C++ linking. Options: (a) compile C codec sources to WASM (increases binary size but preserves compatibility), (b) add a `no_compression` build flag so freestanding builds only support uncompressed files, (c) add per-codec flags for selective inclusion. Option (b) does not exist yet and must be implemented. **Suggested default:** Implement `no_compression` as the first step. Add per-codec flags if selective compression support is needed. See the Compression section for details.

## Conclusion

`zig-parquet` should strengthen an internal boundary where:

- the Parquet engine is transport-neutral
- adapters handle files, memory, and callbacks
- each public surface is a thin wrapper over the same engine

The most urgent work is fixing the write path so external `WriteTarget` works end-to-end, then normalizing constructors to route through the transport-neutral layer. The C ABI should be built on Arrow C Data Interface as the primary data exchange format, with per-handle error context and clear handle-state-after-error semantics for SDK authors. The module reorganization and new surfaces build on that foundation.
