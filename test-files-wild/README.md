# Test Files Wild

Real-world Parquet files from various sources for compatibility testing.

## Structure

- `parquet-testing/` — Files from [Apache parquet-testing](https://github.com/apache/parquet-testing) (215 files)
- `*.parquet` — Additional regression and real-world test files (14 files)

## Validation Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total files** | 229 | |
| **Currently passing** | 219 | 95.6% |
| **Currently failing** | 10 | 4.4% |

### Adjusted Pass Rates

| Scope | Passing | Total | Rate |
|-------|---------|-------|------|
| **Supported features only** | 219 | 219 | **100%** |

**Excluded from adjusted rates:**
- 3 files: Hadoop LZ4 (deprecated, not planned)
- 5 files: Intentionally malformed/invalid (expected failures)
- 2 files: Intentionally corrupt checksums (detected by CRC32 verification)

### Failure Breakdown

| Category | Files | Status |
|----------|-------|--------|
| Hadoop LZ4 | 3 | Not planned |
| Intentionally invalid | 4 | Expected failure |
| Malformed schema | 1 | Expected failure (PyArrow also fails) |
| Corrupt checksums | 2 | Detected by CRC32 verification |

---

## Failing Files Analysis

| File | Error Type | Root Cause | Status |
|------|------------|------------|--------|
| `hadoop_lz4_compressed.parquet` | UnsupportedCompression | Hadoop LZ4 (deprecated, different from LZ4_RAW) | Not planned |
| `hadoop_lz4_compressed_larger.parquet` | UnsupportedCompression | Hadoop LZ4 format | Not planned |
| `non_hadoop_lz4_compressed.parquet` | UnsupportedCompression | Non-standard LZ4 variant | Not planned |
| `fixed_length_byte_array.parquet` | EndOfData | Malformed: REQUIRED column with RLE-encoded nulls | Expected failure |
| `ARROW-GH-41321.parquet` | InvalidBitWidth | Intentionally malformed bit width | Expected failure |
| `ARROW-RS-GH-6229-DICTHEADER.parquet` | InvalidPageSize | Invalid dictionary header | Expected failure |
| `PARQUET-1481.parquet` | SchemaParseError | Intentionally malformed schema | Expected failure |
| `nation.dict-malformed.parquet` | EndOfData | Intentionally corrupted dictionary | Expected failure |
| `datapage_v1-corrupt-checksum.parquet` | PageChecksumMismatch | Intentionally corrupt CRC32 | Expected failure |
| `rle-dict-uncompressed-corrupt-checksum.parquet` | PageChecksumMismatch | Intentionally corrupt CRC32 | Expected failure |

### Error Categories

#### 1. Hadoop LZ4 Compression (Not Planned)
Legacy Hadoop LZ4 format differs from standard LZ4_RAW. Modern tools use LZ4_RAW.
- `hadoop_lz4_compressed.parquet`
- `hadoop_lz4_compressed_larger.parquet`
- `non_hadoop_lz4_compressed.parquet`

#### 2. Intentionally Invalid Files (Expected Failures)
Files from `bad_data/` designed to test error handling:
- `PARQUET-1481.parquet` - Malformed schema
- `ARROW-RS-GH-6229-DICTHEADER.parquet` - Invalid dictionary header
- `ARROW-GH-41321.parquet` - Malformed bit width
- `nation.dict-malformed.parquet` - Corrupted dictionary

#### 3. Corrupt Checksums (Expected Failures)
Files with intentionally corrupt CRC32 page checksums. Detected because `pq validate` enables checksum verification by default. The library default is `validate_page_checksum: false`.
- `datapage_v1-corrupt-checksum.parquet` - Corrupt page CRC
- `rle-dict-uncompressed-corrupt-checksum.parquet` - Corrupt page CRC

#### 4. Malformed Files (Expected Failure)
- `fixed_length_byte_array.parquet` - REQUIRED column contains nulls
  - File declares REQUIRED but has 105 nulls
  - PyArrow also fails: `OSError: Unexpected end of stream`
  - V2 pages: handled correctly (header provides explicit level byte counts)
  - V1 pages: no reliable detection, all implementations fail similarly

---

## Passing Files by Feature

### Compression
| File | Compression | Notes |
|------|-------------|-------|
| `*.snappy.parquet` | Snappy | Multiple files |
| `*.zstd.parquet` | Zstd | e.g., `byte_stream_split.zstd.parquet` |
| `*.gzip.parquet` | Gzip | Including concatenated streams |
| `lz4_raw_compressed*.parquet` | LZ4_RAW | Standard LZ4 |
| `covid.snappy.parquet` | Snappy | Real-world COVID data |

### Encodings
| File | Encoding | Notes |
|------|----------|-------|
| `delta_binary_packed.parquet` | DELTA_BINARY_PACKED | Integer delta encoding |
| `delta_byte_array.parquet` | DELTA_BYTE_ARRAY | String delta encoding |
| `delta_length_byte_array.parquet` | DELTA_LENGTH_BYTE_ARRAY | Variable-length encoding |
| `delta_encoding_*.parquet` | Delta variants | Optional/required columns |
| `byte_stream_split.zstd.parquet` | BYTE_STREAM_SPLIT | Float/double |
| `byte_stream_split_extended.gzip.parquet` | BYTE_STREAM_SPLIT | INT32/INT64/FLBA/Float16 |
| `rle_boolean_encoding.parquet` | RLE | Boolean with RLE encoding |
| `alltypes_dictionary.parquet` | Dictionary | All types with dict encoding |

### Logical Types
| File | Logical Type | Notes |
|------|--------------|-------|
| `int32_decimal.parquet` | Decimal(INT32) | Decimal stored as INT32 |
| `int64_decimal.parquet` | Decimal(INT64) | Decimal stored as INT64 |
| `fixed_length_decimal*.parquet` | Decimal(FLBA) | Decimal in fixed bytes |
| `byte_array_decimal.parquet` | Decimal(BYTE_ARRAY) | Variable-length decimal |
| `int96_from_spark.parquet` | INT96 timestamp | Legacy Spark timestamp |
| `float16_*.parquet` | Float16 | Half-precision floats |

### Nested Types
| File | Structure | Notes |
|------|-----------|-------|
| `list_columns.parquet` | Lists | Simple list columns |
| `nested_lists.snappy.parquet` | Nested lists | List of lists |
| `nested_maps.snappy.parquet` | Nested maps | Map structures |
| `nested_structs.rust.parquet` | Nested structs | Struct of structs |
| `null_list.parquet` | Nullable lists | Lists with null elements |

### DataPage V2
| File | Notes |
|------|-------|
| `datapage_v2.snappy.parquet` | Standard V2 pages |
| `page_v2_empty_compressed.parquet` | Compressed empty V2 |
| `datapage_v2_empty_datapage.snappy.parquet` | Empty data pages |
| `alltypes_tiny_pages.parquet` | Many small V2 pages (7300 rows) |
| `alltypes_tiny_pages_plain.parquet` | Many small V2 pages, plain encoding |

### Edge Cases (Passing)
| File | What it tests |
|------|---------------|
| `empty.parquet` | Empty file |
| `single_nan.parquet` | NaN handling |
| `null_columns.parquet` | All-null columns |
| `nulls.snappy.parquet` | Mixed nulls |
| `concatenated_gzip_members.parquet` | Multiple gzip streams in page |
| `issue206.parquet` | GitHub issue regression |
| `issue229.parquet` | GitHub issue regression |
| `issue276_*.parquet` | Page boundary issues |
| `issue368.parquet` | Another regression test |

### Geospatial (GeoParquet)
All geospatial files in `parquet-testing/data/geospatial/` pass validation:
- `crs-*.parquet` - Various CRS configurations
- `geospatial*.parquet` - Geometry columns

### VARIANT/Shredded (138 files)
All shredded variant test cases pass validation.

---

## Not Planned (Excluded from Pass Rate)

- **Hadoop LZ4** (3 files) - Deprecated format, replaced by LZ4_RAW
  - Modern Arrow/Spark use LZ4_RAW which we fully support
- **Intentionally Invalid** (4 files) - Designed to test error handling
  - `PARQUET-1481.parquet`, `ARROW-RS-GH-6229-DICTHEADER.parquet`
  - `ARROW-GH-41321.parquet`, `nation.dict-malformed.parquet`
- **Corrupt Checksums** (2 files) - Intentionally corrupt CRC32
  - `datapage_v1-corrupt-checksum.parquet`, `rle-dict-uncompressed-corrupt-checksum.parquet`

---

## Running Validation

```bash
# Validate all files (shows failures only by default)
cd pq && ./validate-wild.sh

# Show all files including passes
cd pq && ./validate-wild.sh --all

# Verbose output with details
cd pq && ./validate-wild.sh --verbose

# Validate single file
./pq/zig-out/bin/pq validate test-files-wild/file.parquet

# Get file stats
./pq/zig-out/bin/pq stats test-files-wild/file.parquet
```

## Sources

- `parquet-testing/` — [Apache parquet-testing](https://github.com/apache/parquet-testing) repository
- `issue*.parquet` — Regression test files from [parquet-go](https://github.com/parquet-go/parquet-go)
- `covid.snappy.parquet`, `trace.snappy.parquet`, etc. — Real-world datasets
