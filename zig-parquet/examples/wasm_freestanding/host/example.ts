/**
 * Deno host example for zig-parquet freestanding WASM module.
 *
 * Demonstrates the **row reader API** — a cursor-based interface that yields
 * one row at a time with typed getters. No Arrow knowledge required.
 *
 * Usage:
 *   deno run --allow-read example.ts <path-to-parquet-file>
 */

import type { HostImports, ParquetFreestandingExports } from "./types.d.ts";

const ZP_OK = 0;
const ZP_ROW_END = 11;

const ZP_TYPE_NULL = 0;
const ZP_TYPE_BOOL = 1;
const ZP_TYPE_INT32 = 2;
const ZP_TYPE_INT64 = 3;
const ZP_TYPE_FLOAT = 4;
const ZP_TYPE_DOUBLE = 5;
const ZP_TYPE_BYTES = 6;
const ZP_TYPE_LIST = 7;
const ZP_TYPE_MAP = 8;
const ZP_TYPE_STRUCT = 9;

// ---------------------------------------------------------------------------
// Host IO implementation backed by ArrayBuffer(s)
// ---------------------------------------------------------------------------

class BufferStore {
  private nextCtx = 1;
  private readers = new Map<number, Uint8Array>();

  addReader(data: Uint8Array): number {
    const ctx = this.nextCtx++;
    this.readers.set(ctx, data);
    return ctx;
  }

  getImports(memory: () => WebAssembly.Memory): HostImports {
    const self = this;
    return {
      host_read_at(
        ctx: number,
        offset_lo: number,
        offset_hi: number,
        buf_ptr: number,
        buf_len: number,
      ): number {
        const data = self.readers.get(ctx);
        if (!data) return -1;
        const offset = offset_lo + offset_hi * 0x1_0000_0000;
        const end = Math.min(offset + buf_len, data.length);
        const count = end - offset;
        if (count <= 0) return 0;
        const dest = new Uint8Array(memory().buffer, buf_ptr, count);
        dest.set(data.subarray(offset, end));
        return count;
      },
      host_size(ctx: number): bigint {
        const data = self.readers.get(ctx);
        return BigInt(data?.length ?? 0);
      },
      host_write(_ctx: number, _data_ptr: number, _data_len: number): number {
        return 0;
      },
      host_close(_ctx: number): number {
        return 0;
      },
    };
  }
}

// ---------------------------------------------------------------------------
// Helper: read a C string from WASM memory
// ---------------------------------------------------------------------------

function readCString(memory: WebAssembly.Memory, ptr: number): string {
  if (!ptr) return "";
  const bytes = new Uint8Array(memory.buffer, ptr);
  let end = 0;
  while (bytes[end] !== 0) end++;
  return new TextDecoder().decode(bytes.subarray(0, end));
}

function typeLabel(t: number): string {
  switch (t) {
    case ZP_TYPE_NULL:   return "null";
    case ZP_TYPE_BOOL:   return "bool";
    case ZP_TYPE_INT32:  return "int32";
    case ZP_TYPE_INT64:  return "int64";
    case ZP_TYPE_FLOAT:  return "float";
    case ZP_TYPE_DOUBLE: return "double";
    case ZP_TYPE_BYTES:  return "bytes";
    case ZP_TYPE_LIST:   return "list";
    case ZP_TYPE_MAP:    return "map";
    case ZP_TYPE_STRUCT: return "struct";
    default:             return `unknown(${t})`;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const filePath = Deno.args[0];
  if (!filePath) {
    console.log("Usage: deno run --allow-read example.ts <path-to-parquet-file>");
    Deno.exit(1);
  }

  const parquetData = await Deno.readFile(filePath);
  console.log(`Loaded ${filePath} (${parquetData.length} bytes)\n`);

  // Instantiate the WASM module
  const store = new BufferStore();
  let wasmMemory: WebAssembly.Memory | null = null;

  const wasmPath = new URL("../zig-out/bin/parquet_freestanding_demo.wasm", import.meta.url);
  const wasmBytes = await Deno.readFile(wasmPath);

  const { instance } = await WebAssembly.instantiate(wasmBytes, {
    env: store.getImports(() => wasmMemory!),
  });

  const zp = instance.exports as unknown as ParquetFreestandingExports;
  wasmMemory = zp.memory;

  const versionPtr = zp.zp_version();
  console.log(`zig-parquet version: ${readCString(wasmMemory, versionPtr)}`);

  // Register the Parquet data with the host IO layer
  const ctx = store.addReader(parquetData);

  // -----------------------------------------------------------------------
  // Open a row reader (no Arrow needed!)
  // -----------------------------------------------------------------------
  const handle = zp.zp_row_reader_open_host(ctx);
  if (handle < 0) {
    console.error(`Failed to open row reader: error code ${-handle}`);
    Deno.exit(1);
  }

  // Get metadata
  const numRowGroups = zp.zp_row_reader_get_num_row_groups(handle);
  console.log(`Row groups: ${numRowGroups}`);

  const rowCountBuf = zp.zp_alloc(8);
  let rc = zp.zp_row_reader_get_num_rows(handle, rowCountBuf);
  if (rc === ZP_OK) {
    const totalRows = new DataView(wasmMemory.buffer, rowCountBuf, 8).getBigInt64(0, true);
    console.log(`Total rows: ${totalRows}`);
  }
  zp.zp_free(rowCountBuf, 8);

  const colCountBuf = zp.zp_alloc(4);
  rc = zp.zp_row_reader_get_column_count(handle, colCountBuf);
  const numCols = rc === ZP_OK
    ? new DataView(wasmMemory.buffer, colCountBuf, 4).getInt32(0, true)
    : 0;
  zp.zp_free(colCountBuf, 4);

  // Print column names
  console.log(`\nColumns (${numCols}):`);
  for (let i = 0; i < numCols; i++) {
    const namePtr = zp.zp_row_reader_get_column_name(handle, i);
    console.log(`  [${i}] ${readCString(wasmMemory, namePtr)}`);
  }

  // -----------------------------------------------------------------------
  // Read rows using the cursor API
  // -----------------------------------------------------------------------
  const MAX_ROWS_TO_PRINT = 20;
  let totalPrinted = 0;

  for (let rg = 0; rg < numRowGroups; rg++) {
    rc = zp.zp_row_reader_read_row_group(handle, rg);
    if (rc !== ZP_OK) {
      const msg = readCString(wasmMemory, zp.zp_row_reader_error_message(handle));
      console.error(`\nFailed to read row group ${rg}: ${msg}`);
      continue;
    }

    if (rg === 0) {
      console.log(`\nFirst ${MAX_ROWS_TO_PRINT} rows:`);
    }

    while (zp.zp_row_reader_next(handle) === ZP_OK) {
      if (totalPrinted >= MAX_ROWS_TO_PRINT) {
        totalPrinted++;
        continue;
      }

      const fields: string[] = [];
      for (let c = 0; c < numCols; c++) {
        if (zp.zp_row_reader_is_null(handle, c)) {
          fields.push("null");
          continue;
        }

        const t = zp.zp_row_reader_get_type(handle, c);
        switch (t) {
          case ZP_TYPE_BOOL:
            fields.push(zp.zp_row_reader_get_bool(handle, c) ? "true" : "false");
            break;
          case ZP_TYPE_INT32:
            fields.push(String(zp.zp_row_reader_get_int32(handle, c)));
            break;
          case ZP_TYPE_INT64:
            fields.push(String(zp.zp_row_reader_get_int64(handle, c)));
            break;
          case ZP_TYPE_FLOAT:
            fields.push(zp.zp_row_reader_get_float(handle, c).toFixed(4));
            break;
          case ZP_TYPE_DOUBLE:
            fields.push(zp.zp_row_reader_get_double(handle, c).toFixed(4));
            break;
          case ZP_TYPE_BYTES: {
            const ptr = zp.zp_row_reader_get_bytes_ptr(handle, c);
            const len = zp.zp_row_reader_get_bytes_len(handle, c);
            if (ptr && len > 0) {
              const raw = new Uint8Array(wasmMemory!.buffer, ptr, len);
              try {
                fields.push(`"${new TextDecoder("utf-8", { fatal: true }).decode(raw)}"`);
              } catch {
                fields.push(`<${len} bytes>`);
              }
            } else {
              fields.push(`""`);
            }
            break;
          }
          case ZP_TYPE_LIST:
            fields.push("[...]");
            break;
          case ZP_TYPE_MAP:
            fields.push("{...}");
            break;
          case ZP_TYPE_STRUCT:
            fields.push("(...)");
            break;
          default:
            fields.push(typeLabel(t));
            break;
        }
      }
      console.log(`  ${fields.join(" | ")}`);
      totalPrinted++;
    }
  }

  if (totalPrinted > MAX_ROWS_TO_PRINT) {
    console.log(`  ... and ${totalPrinted - MAX_ROWS_TO_PRINT} more rows`);
  }

  // Clean up
  zp.zp_row_reader_close(handle);
  console.log("\nRow reader closed.");

  // -----------------------------------------------------------------------
  // Write a small Parquet file using the row writer API
  // -----------------------------------------------------------------------
  console.log("\n--- Row Writer Demo ---");

  const writerHandle = zp.zp_row_writer_open_buffer();
  if (writerHandle < 0) {
    console.error(`Failed to open row writer: error code ${-writerHandle}`);
    Deno.exit(1);
  }

  // Helper: write a null-terminated string into WASM memory
  function allocCString(s: string): number {
    const encoded = new TextEncoder().encode(s);
    const ptr = zp.zp_alloc(encoded.length + 1);
    if (!ptr) throw new Error("alloc failed");
    const dest = new Uint8Array(wasmMemory!.buffer, ptr, encoded.length + 1);
    dest.set(encoded);
    dest[encoded.length] = 0;
    return ptr;
  }

  // Define schema: id (int32), name (bytes/string), score (double)
  const nameId = allocCString("id");
  const nameName = allocCString("name");
  const nameScore = allocCString("score");

  zp.zp_row_writer_add_column(writerHandle, nameId, ZP_TYPE_INT32);
  zp.zp_row_writer_add_column(writerHandle, nameName, ZP_TYPE_BYTES);
  zp.zp_row_writer_add_column(writerHandle, nameScore, ZP_TYPE_DOUBLE);

  zp.zp_free(nameId, "id".length + 1);
  zp.zp_free(nameName, "name".length + 1);
  zp.zp_free(nameScore, "score".length + 1);

  rc = zp.zp_row_writer_begin(writerHandle);
  if (rc !== ZP_OK) {
    console.error(`begin failed: ${rc}`);
    Deno.exit(1);
  }

  // Write some rows
  const names = ["Alice", "Bob", "Charlie"];
  for (let i = 0; i < names.length; i++) {
    zp.zp_row_writer_set_int32(writerHandle, 0, i + 1);

    const encoded = new TextEncoder().encode(names[i]);
    const strPtr = zp.zp_alloc(encoded.length);
    if (strPtr) {
      new Uint8Array(wasmMemory!.buffer, strPtr, encoded.length).set(encoded);
      zp.zp_row_writer_set_bytes(writerHandle, 1, strPtr, encoded.length);
      zp.zp_free(strPtr, encoded.length);
    }

    zp.zp_row_writer_set_double(writerHandle, 2, (i + 1) * 10.5);
    zp.zp_row_writer_add_row(writerHandle);
  }

  // Close (flushes remaining rows + writes footer)
  rc = zp.zp_row_writer_close(writerHandle);
  if (rc !== ZP_OK) {
    const msg = readCString(wasmMemory!, zp.zp_row_writer_error_message(writerHandle));
    console.error(`close failed: ${msg}`);
  } else {
    // Retrieve the written buffer
    const ptrBuf = zp.zp_alloc(8); // 4 bytes ptr + 4 bytes len
    if (ptrBuf) {
      rc = zp.zp_row_writer_get_buffer(writerHandle, ptrBuf, ptrBuf + 4);
      if (rc === ZP_OK) {
        const dv = new DataView(wasmMemory!.buffer, ptrBuf, 8);
        const dataPtr = dv.getUint32(0, true);
        const dataLen = dv.getUint32(4, true);
        console.log(`Written ${dataLen} bytes of Parquet data at WASM ptr ${dataPtr}`);

        // Verify by reading it back
        const verifyHandle = zp.zp_row_reader_open_buffer(dataPtr, dataLen);
        if (verifyHandle >= 0) {
          const verifyRgs = zp.zp_row_reader_get_num_row_groups(verifyHandle);
          console.log(`Verification: ${verifyRgs} row group(s)`);
          zp.zp_row_reader_close(verifyHandle);
        }
      }
      zp.zp_free(ptrBuf, 8);
    }
  }

  zp.zp_row_writer_free(writerHandle);
  console.log("Row writer demo complete.");
}

main();
