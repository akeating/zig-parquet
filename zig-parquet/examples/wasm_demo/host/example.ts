/**
 * Node.js host example for zig-parquet WASI WASM module.
 *
 * Demonstrates the **row reader API** — a cursor-based interface that yields
 * one row at a time with typed getters. No Arrow knowledge required.
 *
 * Requires Node.js >= 20 (for stable node:wasi).
 *
 * Usage:
 *   npx tsx example.ts <path-to-parquet-file>
 *
 * Or compile and run with plain node:
 *   npx tsc example.ts --module nodenext --moduleResolution nodenext
 *   node example.js <path-to-parquet-file>
 */

import { readFileSync } from "node:fs";
import { WASI } from "node:wasi";

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
// Helpers
// ---------------------------------------------------------------------------

function readCString(memory: WebAssembly.Memory, ptr: number): string {
  if (!ptr) return "";
  const bytes = new Uint8Array(memory.buffer, ptr);
  let end = 0;
  while (bytes[end] !== 0) end++;
  return new TextDecoder().decode(bytes.subarray(0, end));
}

interface WasiExports {
  memory: WebAssembly.Memory;
  [key: string]: unknown;
}

function getExport<T>(exports: WasiExports, name: string): T {
  const fn = exports[name];
  if (!fn) throw new Error(`Missing export: ${name}`);
  return fn as T;
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
  const filePath = process.argv[2];
  if (!filePath) {
    console.log("Usage: npx tsx example.ts <path-to-parquet-file>");
    process.exit(1);
  }

  const parquetData = readFileSync(filePath);
  console.log(`Loaded ${filePath} (${parquetData.length} bytes)\n`);

  const wasmPath = new URL(
    "../../../zig-out/bin/parquet_wasi.wasm",
    import.meta.url,
  );

  const wasi = new WASI({ version: "preview1" });

  const wasmBytes = readFileSync(wasmPath);
  const wasmModule = await WebAssembly.compile(wasmBytes);
  const instance = await WebAssembly.instantiate(wasmModule, {
    wasi_snapshot_preview1: wasi.getImportObject().wasi_snapshot_preview1,
  });

  const exports = instance.exports as WasiExports;
  const memory = exports.memory;

  // Version
  const zp_version = getExport<() => number>(exports, "zp_version");
  console.log(`zig-parquet version: ${readCString(memory, zp_version())}`);

  // Copy Parquet data into WASM linear memory
  const currentPages = memory.buffer.byteLength / 65536;
  const neededBytes = parquetData.length + 4096;
  const neededPages = Math.ceil(neededBytes / 65536);
  if (neededPages > currentPages) {
    memory.grow(neededPages - currentPages + 1);
  }

  const dataOffset = Math.max(memory.buffer.byteLength - neededBytes - 65536, 65536);
  new Uint8Array(memory.buffer).set(parquetData, dataOffset);

  // Scratch space for out-params
  const scratchOffset = dataOffset + parquetData.length;
  const handleOutPtr = scratchOffset;
  const countOutPtr = scratchOffset + 8;
  const count32OutPtr = scratchOffset + 16;

  // -----------------------------------------------------------------------
  // Open a row reader (no Arrow needed!)
  // -----------------------------------------------------------------------
  const zp_row_reader_open_memory = getExport<
    (data: number, len: number, handle_out: number) => number
  >(exports, "zp_row_reader_open_memory");

  new DataView(memory.buffer).setUint32(handleOutPtr, 0, true);
  let rc = zp_row_reader_open_memory(dataOffset, parquetData.length, handleOutPtr);
  if (rc !== ZP_OK) {
    console.error(`Failed to open row reader: error code ${rc}`);
    process.exit(1);
  }
  const handle = new DataView(memory.buffer).getUint32(handleOutPtr, true);

  // -----------------------------------------------------------------------
  // Metadata
  // -----------------------------------------------------------------------
  const zp_row_reader_get_num_row_groups = getExport<(h: number, out: number) => number>(
    exports, "zp_row_reader_get_num_row_groups",
  );
  const zp_row_reader_get_num_rows = getExport<(h: number, out: number) => number>(
    exports, "zp_row_reader_get_num_rows",
  );
  const zp_row_reader_get_column_count = getExport<(h: number, out: number) => number>(
    exports, "zp_row_reader_get_column_count",
  );
  const zp_row_reader_get_column_name = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_column_name",
  );
  const zp_row_reader_read_row_group = getExport<(h: number, rg: number) => number>(
    exports, "zp_row_reader_read_row_group",
  );
  const zp_row_reader_next = getExport<(h: number) => number>(
    exports, "zp_row_reader_next",
  );
  const zp_row_reader_get_type = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_type",
  );
  const zp_row_reader_is_null = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_is_null",
  );
  const zp_row_reader_get_int32 = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_int32",
  );
  const zp_row_reader_get_int64 = getExport<(h: number, col: number) => bigint>(
    exports, "zp_row_reader_get_int64",
  );
  const zp_row_reader_get_float = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_float",
  );
  const zp_row_reader_get_double = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_double",
  );
  const zp_row_reader_get_bool = getExport<(h: number, col: number) => number>(
    exports, "zp_row_reader_get_bool",
  );
  const zp_row_reader_get_bytes = getExport<
    (h: number, col: number, data_out: number, len_out: number) => number
  >(exports, "zp_row_reader_get_bytes");
  const zp_row_reader_error_message = getExport<(h: number) => number>(
    exports, "zp_row_reader_error_message",
  );
  const zp_row_reader_close = getExport<(h: number) => void>(
    exports, "zp_row_reader_close",
  );

  rc = zp_row_reader_get_num_row_groups(handle, count32OutPtr);
  const numRowGroups = rc === ZP_OK
    ? new DataView(memory.buffer).getInt32(count32OutPtr, true)
    : 0;
  console.log(`Row groups: ${numRowGroups}`);

  rc = zp_row_reader_get_num_rows(handle, countOutPtr);
  if (rc === ZP_OK) {
    const totalRows = new DataView(memory.buffer).getBigInt64(countOutPtr, true);
    console.log(`Total rows: ${totalRows}`);
  }

  rc = zp_row_reader_get_column_count(handle, count32OutPtr);
  const numCols = rc === ZP_OK
    ? new DataView(memory.buffer).getInt32(count32OutPtr, true)
    : 0;

  console.log(`\nColumns (${numCols}):`);
  for (let i = 0; i < numCols; i++) {
    const namePtr = zp_row_reader_get_column_name(handle, i);
    console.log(`  [${i}] ${readCString(memory, namePtr)}`);
  }

  // -----------------------------------------------------------------------
  // Read rows using the cursor API
  // -----------------------------------------------------------------------
  const bytesDataPtr = scratchOffset + 24;
  const bytesLenPtr = scratchOffset + 32;

  const MAX_ROWS_TO_PRINT = 20;
  let totalPrinted = 0;

  for (let rg = 0; rg < numRowGroups; rg++) {
    rc = zp_row_reader_read_row_group(handle, rg);
    if (rc !== ZP_OK) {
      const msg = readCString(memory, zp_row_reader_error_message(handle));
      console.error(`\nFailed to read row group ${rg}: ${msg}`);
      continue;
    }

    if (rg === 0) {
      console.log(`\nFirst ${MAX_ROWS_TO_PRINT} rows:`);
    }

    while (zp_row_reader_next(handle) === ZP_OK) {
      if (totalPrinted >= MAX_ROWS_TO_PRINT) {
        totalPrinted++;
        continue;
      }

      const fields: string[] = [];
      for (let c = 0; c < numCols; c++) {
        if (zp_row_reader_is_null(handle, c)) {
          fields.push("null");
          continue;
        }

        const t = zp_row_reader_get_type(handle, c);
        switch (t) {
          case ZP_TYPE_BOOL:
            fields.push(zp_row_reader_get_bool(handle, c) ? "true" : "false");
            break;
          case ZP_TYPE_INT32:
            fields.push(String(zp_row_reader_get_int32(handle, c)));
            break;
          case ZP_TYPE_INT64:
            fields.push(String(zp_row_reader_get_int64(handle, c)));
            break;
          case ZP_TYPE_FLOAT:
            fields.push(zp_row_reader_get_float(handle, c).toFixed(4));
            break;
          case ZP_TYPE_DOUBLE:
            fields.push(zp_row_reader_get_double(handle, c).toFixed(4));
            break;
          case ZP_TYPE_BYTES: {
            rc = zp_row_reader_get_bytes(handle, c, bytesDataPtr, bytesLenPtr);
            if (rc === ZP_OK) {
              const view = new DataView(memory.buffer);
              const ptr = view.getUint32(bytesDataPtr, true);
              const len = view.getUint32(bytesLenPtr, true);
              if (ptr && len > 0) {
                const raw = new Uint8Array(memory.buffer, ptr, len);
                try {
                  fields.push(`"${new TextDecoder("utf-8", { fatal: true }).decode(raw)}"`);
                } catch {
                  fields.push(`<${len} bytes>`);
                }
              } else {
                fields.push(`""`);
              }
            } else {
              fields.push("null");
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
  zp_row_reader_close(handle);
  console.log("\nRow reader closed.");

  // -----------------------------------------------------------------------
  // Write a small Parquet file using the row writer API
  // -----------------------------------------------------------------------
  console.log("\n--- Row Writer Demo ---");

  const zp_row_writer_open_memory = getExport<(out: number) => number>(exports, "zp_row_writer_open_memory");
  const zp_row_writer_add_column = getExport<(h: number, name: number, t: number) => number>(exports, "zp_row_writer_add_column");
  const zp_row_writer_begin = getExport<(h: number) => number>(exports, "zp_row_writer_begin");
  const zp_row_writer_set_int32 = getExport<(h: number, col: number, v: number) => number>(exports, "zp_row_writer_set_int32");
  const zp_row_writer_set_bytes = getExport<(h: number, col: number, ptr: number, len: number) => number>(exports, "zp_row_writer_set_bytes");
  const zp_row_writer_set_double = getExport<(h: number, col: number, v: number) => number>(exports, "zp_row_writer_set_double");
  const zp_row_writer_add_row = getExport<(h: number) => number>(exports, "zp_row_writer_add_row");
  const zp_row_writer_close = getExport<(h: number) => number>(exports, "zp_row_writer_close");
  const zp_row_writer_get_buffer = getExport<(h: number, dout: number, lout: number) => number>(exports, "zp_row_writer_get_buffer");
  const zp_row_writer_error_message = getExport<(h: number) => number>(exports, "zp_row_writer_error_message");
  const zp_row_writer_free = getExport<(h: number) => void>(exports, "zp_row_writer_free");

  // Helper: write a null-terminated string into WASM scratch memory
  function writeCString(s: string, offset: number): number {
    const encoded = new TextEncoder().encode(s);
    new Uint8Array(memory.buffer).set(encoded, offset);
    new Uint8Array(memory.buffer)[offset + encoded.length] = 0;
    return encoded.length + 1;
  }

  // Open writer
  new DataView(memory.buffer).setUint32(handleOutPtr, 0, true);
  rc = zp_row_writer_open_memory(handleOutPtr);
  if (rc !== ZP_OK) {
    console.error(`Failed to open row writer: error code ${rc}`);
    process.exit(1);
  }
  const wHandle = new DataView(memory.buffer).getUint32(handleOutPtr, true);

  // Define schema using scratch space for column names
  let nameOff = scratchOffset + 32;
  const nameIdLen = writeCString("id", nameOff);
  zp_row_writer_add_column(wHandle, nameOff, ZP_TYPE_INT32);
  nameOff += nameIdLen;
  const nameNameLen = writeCString("name", nameOff);
  zp_row_writer_add_column(wHandle, nameOff, ZP_TYPE_BYTES);
  nameOff += nameNameLen;
  writeCString("score", nameOff);
  zp_row_writer_add_column(wHandle, nameOff, ZP_TYPE_DOUBLE);

  rc = zp_row_writer_begin(wHandle);
  if (rc !== ZP_OK) {
    console.error(`begin failed: ${rc}`);
    process.exit(1);
  }

  // Write rows
  const names = ["Alice", "Bob", "Charlie"];
  const strBuf = scratchOffset + 128;
  for (let i = 0; i < names.length; i++) {
    zp_row_writer_set_int32(wHandle, 0, i + 1);

    const encoded = new TextEncoder().encode(names[i]);
    new Uint8Array(memory.buffer).set(encoded, strBuf);
    zp_row_writer_set_bytes(wHandle, 1, strBuf, encoded.length);

    zp_row_writer_set_double(wHandle, 2, (i + 1) * 10.5);
    zp_row_writer_add_row(wHandle);
  }

  // Close (writes footer)
  rc = zp_row_writer_close(wHandle);
  if (rc !== ZP_OK) {
    const msg = readCString(memory, zp_row_writer_error_message(wHandle));
    console.error(`close failed: ${msg}`);
  } else {
    // Retrieve buffer
    const bufOutPtr = scratchOffset + 256;
    rc = zp_row_writer_get_buffer(wHandle, bufOutPtr, bufOutPtr + 8);
    if (rc === ZP_OK) {
      const dv = new DataView(memory.buffer);
      const bufPtr = dv.getUint32(bufOutPtr, true);
      const bufLen = dv.getUint32(bufOutPtr + 8, true);
      console.log(`Written ${bufLen} bytes of Parquet data`);

      // Verify by reading back
      new DataView(memory.buffer).setUint32(handleOutPtr, 0, true);
      rc = zp_row_reader_open_memory(bufPtr, bufLen, handleOutPtr);
      if (rc === ZP_OK) {
        const verifyHandle = new DataView(memory.buffer).getUint32(handleOutPtr, true);
        zp_row_reader_get_num_row_groups(verifyHandle, count32OutPtr);
        const verifyRgs = new DataView(memory.buffer).getInt32(count32OutPtr, true);
        console.log(`Verification: ${verifyRgs} row group(s)`);
        zp_row_reader_close(verifyHandle);
      }
    }
  }

  zp_row_writer_free(wHandle);
  console.log("Row writer demo complete.");
}

main();
