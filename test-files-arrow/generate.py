#!/usr/bin/env python3
"""
Generate canonical Parquet test files using PyArrow.

These files are used to validate Zig Parquet reader implementations
against the reference C++ implementation (which PyArrow wraps).

Usage:
    python generate.py          # Generate all files
    python generate.py --p0     # Generate only P0 (MVP) files
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import hashlib
import argparse
from pathlib import Path

# Output directory (same directory as this script)
OUTPUT_DIR = Path(__file__).parent


def write_with_options(table: pa.Table, category: str, name: str, **kwargs) -> dict:
    """
    Write a table to a Parquet file with specified options.
    
    Returns metadata dict for manifest.
    """
    dir_path = OUTPUT_DIR / category
    dir_path.mkdir(exist_ok=True)
    
    file_path = dir_path / f"{name}.parquet"
    pq.write_table(table, file_path, **kwargs)
    
    # Read back metadata
    meta = pq.read_metadata(file_path)
    
    # Compute file hash
    with open(file_path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    
    file_size = os.path.getsize(file_path)
    
    print(f"  {category}/{name}.parquet: {file_size} bytes, "
          f"{meta.num_row_groups} row group(s), {meta.num_rows} rows")
    
    # Build column info
    columns = []
    if meta.num_row_groups > 0:
        for i in range(meta.num_columns):
            col = meta.row_group(0).column(i)
            columns.append({
                "name": col.path_in_schema,
                "physical_type": str(col.physical_type),
                "compression": str(col.compression),
                "encodings": [str(e) for e in col.encodings] if hasattr(col, 'encodings') else [],
            })
    
    return {
        "path": f"{category}/{name}.parquet",
        "sha256": file_hash,
        "size_bytes": file_size,
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "created_by": meta.created_by,
        "schema": str(table.schema),
        "columns": columns,
    }


# =============================================================================
# P0: Basic files for MVP
# =============================================================================

def generate_basic_types_plain_uncompressed() -> dict:
    """All primitive types, PLAIN encoding, no compression."""
    # Fixed-length byte array (16 bytes, like a UUID)
    uuid_type = pa.binary(16)
    uuid1 = b"\x12\x34\x56\x78\x9a\xbc\xde\xf0\x12\x34\x56\x78\x9a\xbc\xde\xf0"
    uuid2 = b"\x00" * 16
    uuid3 = b"\xff" * 16
    uuid4 = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
    
    table = pa.table({
        "bool_col": pa.array([True, False, True, None, False]),
        "int32_col": pa.array([1, -2, 2147483647, -2147483648, None], type=pa.int32()),
        "int64_col": pa.array([1, -2, 9223372036854775807, None, 0], type=pa.int64()),
        "float_col": pa.array([1.5, -2.5, float('inf'), float('-inf'), None], type=pa.float32()),
        "double_col": pa.array([1.5, -2.5, 1.7976931348623157e308, None, 0.0]),
        "string_col": pa.array(["hello", "world", "", None, "🎉 unicode"]),
        "binary_col": pa.array([b"\x00\x01\x02", b"", None, b"\xff" * 100, b"test"]),
        "fixed_binary_col": pa.array([uuid1, uuid2, uuid3, None, uuid4], type=uuid_type),
    })
    return write_with_options(
        table, "basic", "basic_types_plain_uncompressed",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_boundary_values() -> dict:
    """Integer boundary values for all integer types."""
    table = pa.table({
        "i8": pa.array([-128, 127, 0], type=pa.int8()),
        "i16": pa.array([-32768, 32767, 0], type=pa.int16()),
        "i32": pa.array([-2147483648, 2147483647, 0], type=pa.int32()),
        "i64": pa.array([-9223372036854775808, 9223372036854775807, 0], type=pa.int64()),
        "u8": pa.array([0, 255, 128], type=pa.uint8()),
        "u16": pa.array([0, 65535, 32768], type=pa.uint16()),
        "u32": pa.array([0, 4294967295, 2147483648], type=pa.uint32()),
        "u64": pa.array([0, 18446744073709551615, 9223372036854775808], type=pa.uint64()),
    })
    return write_with_options(
        table, "basic", "boundary_values",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_nulls_and_empties() -> dict:
    """Null handling and empty values."""
    table = pa.table({
        "all_null": pa.array([None] * 100, type=pa.string()),
        "all_empty": pa.array([""] * 100),
        "mixed": pa.array([None, "", "x", None, ""] * 20),
        "no_nulls": pa.array(["a", "b", "c"] * 33 + ["d"]),
    })
    return write_with_options(
        table, "basic", "nulls_and_empties",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


# =============================================================================
# P1: Encodings and compression
# =============================================================================

def generate_dictionary_low_cardinality() -> dict:
    """DICTIONARY encoding with low cardinality (few unique values)."""
    table = pa.table({
        "status": pa.array(["active", "inactive", "pending"] * 1000),
        "category": pa.array(["A", "B", "C", "D"] * 750),
    })
    return write_with_options(
        table, "encodings", "dictionary_low_cardinality",
        use_dictionary=True,
        compression=None,
        data_page_version="1.0",
    )


def generate_dictionary_high_cardinality() -> dict:
    """DICTIONARY encoding with high cardinality (many unique values)."""
    table = pa.table({
        "uuid": pa.array([f"uuid-{i:06d}" for i in range(3000)]),
    })
    return write_with_options(
        table, "encodings", "dictionary_high_cardinality",
        use_dictionary=True,
        compression=None,
        data_page_version="1.0",
    )


# =============================================================================
# Delta Encodings
# =============================================================================

def generate_delta_binary_packed_int32() -> dict:
    """DELTA_BINARY_PACKED encoding for INT32 values."""
    # Test various patterns that exercise delta encoding:
    # - Sequential (small deltas)
    # - Random (varying deltas)
    # - Repeated (zero deltas)
    # - Boundary values
    import random
    random.seed(42)
    
    sequential = list(range(1000))
    random_vals = [random.randint(-1000000, 1000000) for _ in range(1000)]
    repeated = [42] * 1000
    boundaries = [-2147483648, 2147483647, 0, -1, 1] * 200
    
    table = pa.table({
        "sequential": pa.array(sequential, type=pa.int32()),
        "random": pa.array(random_vals, type=pa.int32()),
        "repeated": pa.array(repeated, type=pa.int32()),
        "boundaries": pa.array(boundaries, type=pa.int32()),
    })
    return write_with_options(
        table, "encodings", "delta_binary_packed_int32",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_BINARY_PACKED",
        data_page_version="1.0",
    )


def generate_delta_binary_packed_int64() -> dict:
    """DELTA_BINARY_PACKED encoding for INT64 values."""
    import random
    random.seed(43)
    
    # Timestamps-like sequential values
    timestamps = [1704067200000000 + i * 1000000 for i in range(1000)]  # Microseconds
    random_vals = [random.randint(-10**15, 10**15) for _ in range(1000)]
    small_deltas = [i * 3 for i in range(1000)]
    
    table = pa.table({
        "timestamps": pa.array(timestamps, type=pa.int64()),
        "random": pa.array(random_vals, type=pa.int64()),
        "small_deltas": pa.array(small_deltas, type=pa.int64()),
    })
    return write_with_options(
        table, "encodings", "delta_binary_packed_int64",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_BINARY_PACKED",
        data_page_version="1.0",
    )


def generate_delta_length_byte_array() -> dict:
    """DELTA_LENGTH_BYTE_ARRAY encoding for variable-length byte arrays."""
    # Lengths are delta-encoded, values are concatenated
    import random
    random.seed(44)
    
    # Various string patterns
    uniform_length = ["x" * 10 for _ in range(500)]
    varying_length = ["y" * (i % 50 + 1) for i in range(500)]
    empty_and_long = ["", "a" * 1000, "", "b" * 500, ""] * 100
    binary_data = [bytes([i % 256] * (i % 20 + 1)) for i in range(500)]
    
    table = pa.table({
        "uniform": pa.array(uniform_length),
        "varying": pa.array(varying_length),
        "empty_and_long": pa.array(empty_and_long),
        "binary": pa.array(binary_data, type=pa.binary()),
    })
    return write_with_options(
        table, "encodings", "delta_length_byte_array",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_LENGTH_BYTE_ARRAY",
        data_page_version="1.0",
    )


def generate_delta_byte_array() -> dict:
    """DELTA_BYTE_ARRAY (incremental) encoding for strings with common prefixes."""
    # Prefix lengths and suffix lengths are delta-encoded
    # Best for sorted strings or strings with common prefixes
    
    # URLs with common prefix
    urls = [f"https://example.com/api/v1/users/{i}" for i in range(500)]
    
    # File paths with common prefix
    paths = [f"/home/user/documents/project/src/module_{i}/file_{j}.txt" 
             for i in range(50) for j in range(10)]
    
    # Sorted strings (ideal case for delta byte array)
    sorted_words = sorted([f"word_{i:05d}" for i in range(500)])
    
    # Mixed - some common prefixes, some not
    mixed = ["apple", "application", "apply", "banana", "bandana", "band"] * 83 + ["unique", "zoo"]
    
    table = pa.table({
        "urls": pa.array(urls),
        "paths": pa.array(paths),
        "sorted": pa.array(sorted_words),
        "mixed": pa.array(mixed),
    })
    return write_with_options(
        table, "encodings", "delta_byte_array",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_BYTE_ARRAY",
        data_page_version="1.0",
    )


def generate_byte_stream_split_float() -> dict:
    """BYTE_STREAM_SPLIT encoding for FLOAT values."""
    import random
    import math
    random.seed(45)
    
    # Various float patterns
    sequential = [float(i) * 0.1 for i in range(1000)]
    random_vals = [random.uniform(-1e6, 1e6) for _ in range(1000)]
    special = [0.0, -0.0, float('inf'), float('-inf'), float('nan')] * 200
    small = [random.uniform(-1e-30, 1e-30) for _ in range(1000)]
    
    table = pa.table({
        "sequential": pa.array(sequential, type=pa.float32()),
        "random": pa.array(random_vals, type=pa.float32()),
        "special": pa.array(special, type=pa.float32()),
        "small": pa.array(small, type=pa.float32()),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_float",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_byte_stream_split_double() -> dict:
    """BYTE_STREAM_SPLIT encoding for DOUBLE values."""
    import random
    random.seed(46)
    
    # Various double patterns  
    sequential = [float(i) * 0.001 for i in range(1000)]
    random_vals = [random.uniform(-1e100, 1e100) for _ in range(1000)]
    precise = [1.0 + i * 1e-15 for i in range(1000)]  # Very small differences
    
    table = pa.table({
        "sequential": pa.array(sequential, type=pa.float64()),
        "random": pa.array(random_vals, type=pa.float64()),
        "precise": pa.array(precise, type=pa.float64()),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_double",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_delta_with_nulls() -> dict:
    """Delta encodings with null values interspersed."""
    import random
    random.seed(47)
    
    # INT32 with nulls - DELTA_BINARY_PACKED
    int_vals = [i if i % 7 != 0 else None for i in range(1000)]
    
    table = pa.table({
        "int_with_nulls": pa.array(int_vals, type=pa.int32()),
    })
    return write_with_options(
        table, "encodings", "delta_with_nulls_int",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_BINARY_PACKED",
        data_page_version="1.0",
    )


def generate_delta_strings_with_nulls() -> dict:
    """DELTA_LENGTH_BYTE_ARRAY with null values."""
    # Strings with nulls
    str_vals = [f"value_{i}" if i % 5 != 0 else None for i in range(500)]
    
    table = pa.table({
        "str_with_nulls": pa.array(str_vals),
    })
    return write_with_options(
        table, "encodings", "delta_with_nulls_string",
        use_dictionary=False,
        compression=None,
        column_encoding="DELTA_LENGTH_BYTE_ARRAY",
        data_page_version="1.0",
    )


def generate_byte_stream_split_with_nulls() -> dict:
    """BYTE_STREAM_SPLIT with null values."""
    import random
    random.seed(48)
    
    float_vals = [random.uniform(-100, 100) if i % 3 != 0 else None for i in range(1000)]
    
    table = pa.table({
        "float_with_nulls": pa.array(float_vals, type=pa.float32()),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_with_nulls",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_byte_stream_split_int32() -> dict:
    """BYTE_STREAM_SPLIT encoding for INT32 values (Parquet 2024+)."""
    import random
    random.seed(49)
    
    # Various int32 patterns
    sequential = list(range(1000))
    random_vals = [random.randint(-2**30, 2**30) for _ in range(1000)]
    small_range = [random.randint(-100, 100) for _ in range(1000)]
    
    table = pa.table({
        "sequential": pa.array(sequential, type=pa.int32()),
        "random": pa.array(random_vals, type=pa.int32()),
        "small_range": pa.array(small_range, type=pa.int32()),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_int32",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_byte_stream_split_int64() -> dict:
    """BYTE_STREAM_SPLIT encoding for INT64 values (Parquet 2024+)."""
    import random
    random.seed(50)
    
    # Various int64 patterns
    sequential = list(range(1000))
    random_vals = [random.randint(-2**60, 2**60) for _ in range(1000)]
    timestamps = [1700000000000 + i * 1000 for i in range(1000)]  # Simulated timestamps
    
    table = pa.table({
        "sequential": pa.array(sequential, type=pa.int64()),
        "random": pa.array(random_vals, type=pa.int64()),
        "timestamps": pa.array(timestamps, type=pa.int64()),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_int64",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_byte_stream_split_float16() -> dict:
    """BYTE_STREAM_SPLIT encoding for FLOAT16 values (Parquet 2024+)."""
    import random
    import numpy as np
    random.seed(51)
    
    # Various float16 patterns
    sequential = [float(i) * 0.01 for i in range(200)]
    random_vals = [random.uniform(-100, 100) for _ in range(200)]
    
    table = pa.table({
        "sequential": pa.array(np.array(sequential, dtype=np.float16)),
        "random": pa.array(np.array(random_vals, dtype=np.float16)),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_float16",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_byte_stream_split_flba() -> dict:
    """BYTE_STREAM_SPLIT encoding for FIXED_LEN_BYTE_ARRAY values (Parquet 2024+)."""
    import random
    random.seed(52)
    
    # Fixed 8-byte arrays (like UUIDs first 8 bytes)
    flba_vals = [bytes([random.randint(0, 255) for _ in range(8)]) for _ in range(200)]
    
    # Create a fixed-size binary type
    table = pa.table({
        "fixed_bytes": pa.array(flba_vals, type=pa.binary(8)),
    })
    return write_with_options(
        table, "encodings", "byte_stream_split_flba",
        use_dictionary=False,
        compression=None,
        column_encoding="BYTE_STREAM_SPLIT",
        data_page_version="1.0",
    )


def generate_compression_none() -> dict:
    """Baseline uncompressed data for compression comparison."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_none",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_compression_zstd() -> dict:
    """zstd compressed data."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_zstd",
        use_dictionary=False,
        compression="zstd",
        data_page_version="1.0",
    )


def generate_compression_gzip() -> dict:
    """gzip compressed data."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_gzip",
        use_dictionary=False,
        compression="gzip",
        data_page_version="1.0",
    )


def generate_compression_snappy() -> dict:
    """Snappy compressed data."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_snappy",
        use_dictionary=False,
        compression="snappy",
        data_page_version="1.0",
    )


def generate_compression_lz4() -> dict:
    """LZ4 compressed data."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_lz4",
        use_dictionary=False,
        compression="lz4",
        data_page_version="1.0",
    )


def generate_compression_brotli() -> dict:
    """Brotli compressed data."""
    table = pa.table({
        "repeated": pa.array(["AAAAAAAAAA"] * 10000),
        "sequence": pa.array(list(range(10000))),
    })
    return write_with_options(
        table, "compression", "compression_brotli",
        use_dictionary=False,
        compression="brotli",
        data_page_version="1.0",
    )


# =============================================================================
# P1: Structure tests
# =============================================================================

def generate_multiple_row_groups() -> dict:
    """Multiple row groups to test row group iteration."""
    table = pa.table({
        "id": pa.array(range(100000)),
        "value": pa.array([f"row-{i}" for i in range(100000)]),
    })
    return write_with_options(
        table, "structure", "multiple_row_groups",
        use_dictionary=False,
        compression=None,
        row_group_size=10000,  # 10 row groups
        data_page_version="1.0",
    )


def generate_single_value() -> dict:
    """Minimal file with single value."""
    table = pa.table({
        "col": pa.array([42], type=pa.int32()),
    })
    return write_with_options(
        table, "edge_cases", "single_value",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_empty_table() -> dict:
    """Valid Parquet file with no rows."""
    table = pa.table({
        "col": pa.array([], type=pa.int32()),
    })
    return write_with_options(
        table, "edge_cases", "empty_table",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_large_strings() -> dict:
    """Large string values to test buffer handling."""
    table = pa.table({
        "small": pa.array(["x"] * 10),
        "medium": pa.array(["y" * 1000] * 10),
        "large": pa.array(["z" * 100000] * 10),
    })
    return write_with_options(
        table, "edge_cases", "large_strings",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


# =============================================================================
# Logical Types
# =============================================================================

def generate_logical_string() -> dict:
    """STRING logical type (UTF8 annotation on BYTE_ARRAY)."""
    from datetime import date, datetime, time, timezone
    
    table = pa.table({
        "str_col": pa.array(["hello", "world", "", "unicode: 🎉", None], type=pa.string()),
        "str_required": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
    })
    return write_with_options(
        table, "logical_types", "string",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_date() -> dict:
    """DATE logical type (days since epoch as INT32)."""
    from datetime import date
    
    table = pa.table({
        "date_col": pa.array([
            date(2024, 1, 15),
            date(1970, 1, 1),  # epoch
            date(2000, 6, 15),
            date(1999, 12, 31),
            None,
        ], type=pa.date32()),
        "date_required": pa.array([
            date(2020, 1, 1),
            date(2021, 2, 2),
            date(2022, 3, 3),
            date(2023, 4, 4),
            date(2024, 5, 5),
        ], type=pa.date32()),
    })
    return write_with_options(
        table, "logical_types", "date",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_timestamp() -> dict:
    """TIMESTAMP logical types (millis/micros/nanos, with/without UTC)."""
    from datetime import datetime, timezone
    
    # Timestamps as microseconds since epoch (default for PyArrow)
    ts1 = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    ts2 = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts3 = datetime(2000, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    
    table = pa.table({
        # TIMESTAMP with milliseconds, UTC
        "ts_millis_utc": pa.array([ts1, ts2, ts3, None, ts1], type=pa.timestamp("ms", tz="UTC")),
        # TIMESTAMP with microseconds, UTC  
        "ts_micros_utc": pa.array([ts1, ts2, ts3, None, ts1], type=pa.timestamp("us", tz="UTC")),
        # TIMESTAMP with nanoseconds, UTC
        "ts_nanos_utc": pa.array([ts1, ts2, ts3, None, ts1], type=pa.timestamp("ns", tz="UTC")),
        # TIMESTAMP without timezone (local)
        "ts_micros_local": pa.array([
            datetime(2024, 1, 15, 10, 30, 0),
            datetime(1970, 1, 1, 0, 0, 0),
            datetime(2000, 6, 15, 12, 0, 0),
            None,
            datetime(2024, 1, 15, 10, 30, 0),
        ], type=pa.timestamp("us")),
    })
    return write_with_options(
        table, "logical_types", "timestamp",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_time() -> dict:
    """TIME logical types (millis/micros/nanos)."""
    from datetime import time
    
    t1 = time(10, 30, 0)
    t2 = time(0, 0, 0)
    t3 = time(23, 59, 59)
    t4 = time(12, 0, 0)
    
    table = pa.table({
        # TIME with milliseconds
        "time_millis": pa.array([t1, t2, t3, None, t4], type=pa.time32("ms")),
        # TIME with microseconds  
        "time_micros": pa.array([t1, t2, t3, None, t4], type=pa.time64("us")),
        # TIME with nanoseconds
        "time_nanos": pa.array([t1, t2, t3, None, t4], type=pa.time64("ns")),
    })
    return write_with_options(
        table, "logical_types", "time",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_decimal() -> dict:
    """DECIMAL logical type with various precisions."""
    from decimal import Decimal
    
    table = pa.table({
        # DECIMAL(9,2) - fits in INT32
        "decimal_9_2": pa.array([
            Decimal("123.45"),
            Decimal("-999.99"),
            Decimal("0.01"),
            None,
            Decimal("1234567.89"),
        ], type=pa.decimal128(9, 2)),
        # DECIMAL(18,4) - fits in INT64
        "decimal_18_4": pa.array([
            Decimal("12345678901234.5678"),
            Decimal("-99999999999999.9999"),
            None,
            Decimal("0.0001"),
            Decimal("1.0000"),
        ], type=pa.decimal128(18, 4)),
        # DECIMAL(38,10) - requires FIXED_LEN_BYTE_ARRAY
        "decimal_38_10": pa.array([
            Decimal("1234567890123456789012345678.1234567890"),
            None,
            Decimal("-0.0000000001"),
            Decimal("9999999999999999999999999999.9999999999"),
            Decimal("0.0000000000"),
        ], type=pa.decimal128(38, 10)),
    })
    return write_with_options(
        table, "logical_types", "decimal",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_uuid() -> dict:
    """UUID logical type (FIXED_LEN_BYTE_ARRAY(16) with UUID annotation)."""
    import uuid as uuid_mod
    
    uuid1 = uuid_mod.UUID("12345678-1234-5678-1234-567812345678")
    uuid2 = uuid_mod.UUID("00000000-0000-0000-0000-000000000000")
    uuid3 = uuid_mod.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    uuid4 = uuid_mod.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    
    # pa.uuid() extension type writes FLBA(16) with UUID logical type annotation
    table = pa.table({
        "uuid_col": pa.array([
            uuid1.bytes,
            uuid2.bytes,
            uuid3.bytes,
            None,
            uuid4.bytes,
        ], type=pa.uuid()),
    })
    
    return write_with_options(
        table, "logical_types", "uuid",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_int_types() -> dict:
    """INT8/16/32/64 and UINT8/16/32/64 logical types."""
    table = pa.table({
        # Signed integers
        "int8_col": pa.array([1, -128, 127, 0, None], type=pa.int8()),
        "int16_col": pa.array([1, -32768, 32767, 0, None], type=pa.int16()),
        # Note: int32 and int64 don't need special logical type annotation
        # but we include them for completeness
        "int32_col": pa.array([1, -2147483648, 2147483647, 0, None], type=pa.int32()),
        "int64_col": pa.array([1, -9223372036854775808, 9223372036854775807, 0, None], type=pa.int64()),
        # Unsigned integers
        "uint8_col": pa.array([0, 128, 255, 1, None], type=pa.uint8()),
        "uint16_col": pa.array([0, 32768, 65535, 1, None], type=pa.uint16()),
        "uint32_col": pa.array([0, 2147483648, 4294967295, 1, None], type=pa.uint32()),
        "uint64_col": pa.array([0, 9223372036854775808, 18446744073709551615, 1, None], type=pa.uint64()),
    })
    return write_with_options(
        table, "logical_types", "int_types",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_float16() -> dict:
    """FLOAT16 (half-precision float) logical type."""
    import numpy as np
    
    # Create float16 values
    f16_values = np.array([1.5, -2.25, 0.0, 65504.0, None], dtype=object)
    # Convert to float16 where not None
    f16_array = []
    for v in f16_values:
        if v is None:
            f16_array.append(None)
        else:
            f16_array.append(np.float16(v))
    
    # PyArrow 12+ has float16 type
    try:
        table = pa.table({
            "float16_col": pa.array(f16_array, type=pa.float16()),
        })
    except AttributeError:
        # Fallback: store as fixed binary(2)
        print("  Note: PyArrow doesn't support float16, using binary(2)")
        binary_values = []
        for v in f16_array:
            if v is None:
                binary_values.append(None)
            else:
                binary_values.append(np.float16(v).tobytes())
        table = pa.table({
            "float16_col": pa.array(binary_values, type=pa.binary(2)),
        })
    
    return write_with_options(
        table, "logical_types", "float16",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_enum() -> dict:
    """ENUM logical type (dictionary-encoded strings)."""
    # ENUM in Parquet is represented as BYTE_ARRAY with ENUM annotation
    # PyArrow uses dictionary encoding for this
    table = pa.table({
        "enum_col": pa.array(
            ["RED", "GREEN", "BLUE", None, "RED"],
            type=pa.dictionary(pa.int32(), pa.string())
        ),
        # Also test as plain string with enum-like values
        "color": pa.array(["red", "green", "blue", "red", None]),
    })
    return write_with_options(
        table, "logical_types", "enum",
        use_dictionary=True,  # Use dictionary encoding for enum
        compression=None,
        data_page_version="1.0",
    )


def generate_logical_json() -> dict:
    """JSON logical type (BYTE_ARRAY with JSON annotation)."""
    # JSON values as strings
    json_values = [
        '{"name": "Alice", "age": 30}',
        '{"items": [1, 2, 3], "count": 3}',
        '[]',
        None,
        '{"nested": {"key": "value"}}',
    ]
    
    # PyArrow doesn't have native JSON type, use large_string or string
    table = pa.table({
        "json_col": pa.array(json_values, type=pa.large_string()),
    })
    return write_with_options(
        table, "logical_types", "json",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


# =============================================================================
# Interop Types (Phase 14)
# =============================================================================

def generate_interop_int96() -> dict:
    """INT96 timestamps (legacy Spark/Impala format).

    PyArrow writes timestamps as INT96 when use_deprecated_int96_timestamps=True.
    Each value is 12 bytes: 8-byte nanoseconds-of-day + 4-byte Julian day number.
    """
    from datetime import datetime, timezone

    ts1 = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    ts2 = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts3 = datetime(2000, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    ts4 = datetime(2024, 7, 4, 18, 30, 45, 123456, tzinfo=timezone.utc)

    table = pa.table({
        "ts_col": pa.array([ts1, ts2, ts3, None, ts4], type=pa.timestamp("us", tz="UTC")),
    })
    return write_with_options(
        table, "logical_types", "int96_timestamp",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
        use_deprecated_int96_timestamps=True,
    )


# =============================================================================
# Nested Types (Phase 3 prep)
# =============================================================================

def generate_list_int() -> dict:
    """List of integers (non-nullable list, non-nullable elements)."""
    table = pa.table({
        "list_col": pa.array([
            [1, 2, 3],
            [4, 5],
            [6],
            [],
            [7, 8, 9, 10],
        ], type=pa.list_(pa.int32())),
    })
    return write_with_options(
        table, "nested", "list_int",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_nullable() -> dict:
    """Nullable list of nullable integers."""
    table = pa.table({
        "list_col": pa.array([
            [1, None, 3],
            None,  # null list
            [4, 5],
            [],
            [None, None],
        ], type=pa.list_(pa.int32())),
    })
    return write_with_options(
        table, "nested", "list_nullable",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_of_list() -> dict:
    """Nested lists (list of list of int)."""
    table = pa.table({
        "list_col": pa.array([
            [[1, 2], [3]],
            [[4, 5, 6]],
            [[], [7]],
            [],
            [[8], [9], [10]],
        ], type=pa.list_(pa.list_(pa.int32()))),
    })
    return write_with_options(
        table, "nested", "list_of_list",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_struct_simple() -> dict:
    """Simple struct with scalar fields."""
    struct_type = pa.struct([
        ("x", pa.int32()),
        ("y", pa.int32()),
        ("name", pa.string()),
    ])
    
    table = pa.table({
        "point": pa.array([
            {"x": 1, "y": 2, "name": "A"},
            {"x": 3, "y": 4, "name": "B"},
            None,  # null struct
            {"x": 5, "y": 6, "name": "C"},
            {"x": 7, "y": 8, "name": None},  # null field
        ], type=struct_type),
    })
    return write_with_options(
        table, "nested", "struct_simple",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_struct_nested() -> dict:
    """Nested struct (struct containing struct)."""
    inner_type = pa.struct([
        ("a", pa.int32()),
        ("b", pa.int32()),
    ])
    outer_type = pa.struct([
        ("inner", inner_type),
        ("value", pa.string()),
    ])
    
    table = pa.table({
        "nested": pa.array([
            {"inner": {"a": 1, "b": 2}, "value": "first"},
            {"inner": {"a": 3, "b": 4}, "value": "second"},
            {"inner": None, "value": "null_inner"},
            None,
            {"inner": {"a": 5, "b": 6}, "value": None},
        ], type=outer_type),
    })
    return write_with_options(
        table, "nested", "struct_nested",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_map_string_int() -> dict:
    """Map from string to int."""
    table = pa.table({
        "map_col": pa.array([
            [("a", 1), ("b", 2)],
            [("c", 3)],
            [],
            None,
            [("d", 4), ("e", 5), ("f", 6)],
        ], type=pa.map_(pa.string(), pa.int32())),
    })
    return write_with_options(
        table, "nested", "map_string_int",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_int64() -> dict:
    """List of int64 values with nulls."""
    table = pa.table({
        "list_col": pa.array([
            [100, 200, 300],
            [9223372036854775807, -9223372036854775808],
            None,
            [],
            [0, 1],
        ], type=pa.list_(pa.int64())),
    })
    return write_with_options(
        table, "nested", "list_int64",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_f32() -> dict:
    """List of float32 values with nulls and special values."""
    table = pa.table({
        "list_col": pa.array([
            [1.5, -2.5, 0.0],
            [float('inf'), float('-inf')],
            None,
            [],
            [3.14],
        ], type=pa.list_(pa.float32())),
    })
    return write_with_options(
        table, "nested", "list_f32",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_map_string_string() -> dict:
    """Map from string to string."""
    table = pa.table({
        "map_col": pa.array([
            [("key1", "val1"), ("key2", "val2")],
            [("hello", "world")],
            [],
            None,
            [("a", ""), ("b", "test"), ("c", "unicode: 🎉")],
        ], type=pa.map_(pa.string(), pa.string())),
    })
    return write_with_options(
        table, "nested", "map_string_string",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_map_int_int() -> dict:
    """Map from int32 to int32."""
    table = pa.table({
        "map_col": pa.array([
            [(1, 100), (2, 200)],
            [(3, 300)],
            [],
            None,
            [(10, 1000), (20, 2000), (30, 3000)],
        ], type=pa.map_(pa.int32(), pa.int32())),
    })
    return write_with_options(
        table, "nested", "map_int_int",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_string() -> dict:
    """List of strings with nulls and empty values."""
    table = pa.table({
        "list_col": pa.array([
            ["hello", "world"],
            ["", None, "test"],
            None,  # null list
            [],
            ["unicode: 🎉"],
        ], type=pa.list_(pa.string())),
    })
    return write_with_options(
        table, "nested", "list_string",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_float() -> dict:
    """List of floats including special values."""
    table = pa.table({
        "list_col": pa.array([
            [1.5, -2.5, 0.0],
            [float('inf'), float('-inf'), float('nan')],
            None,  # null list
            [],
            [3.14159],
        ], type=pa.list_(pa.float64())),
    })
    return write_with_options(
        table, "nested", "list_float",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_bool() -> dict:
    """List of booleans with nulls."""
    table = pa.table({
        "list_col": pa.array([
            [True, False, True],
            [False],
            None,  # null list
            [],
            [True, None, False],
        ], type=pa.list_(pa.bool_())),
    })
    return write_with_options(
        table, "nested", "list_bool",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_list_struct() -> dict:
    """List of structs."""
    point_type = pa.struct([("x", pa.int32()), ("y", pa.int32())])
    table = pa.table({
        "points": pa.array([
            [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            [{"x": 5, "y": 6}],
            None,  # null list
            [],
            [{"x": 7, "y": 8}, None],  # list with null struct element
        ], type=pa.list_(point_type)),
    })
    return write_with_options(
        table, "nested", "list_struct",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_struct_with_list() -> dict:
    """Struct containing a list field (flat columns, one is a list)."""
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        "tags": pa.array([
            ["a", "b"],
            ["c"],
            [],
            None,
            ["d", "e", "f"],
        ], type=pa.list_(pa.string())),
    })
    return write_with_options(
        table, "nested", "struct_with_list",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


def generate_struct_with_temporal() -> dict:
    """Struct with temporal fields."""
    from datetime import date, datetime, timezone
    
    ts1 = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 20, 14, 45, 30, tzinfo=timezone.utc)
    ts3 = datetime(2024, 3, 25, 8, 0, 0, tzinfo=timezone.utc)
    ts4 = datetime(2024, 4, 30, 18, 15, 0, tzinfo=timezone.utc)
    ts5 = datetime(2024, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
    
    table = pa.table({
        "event": pa.array(["login", "logout", "click", None, "purchase"]),
        "timestamp": pa.array([ts1, ts2, ts3, None, ts5], type=pa.timestamp("us", tz="UTC")),
        "date": pa.array([
            date(2024, 1, 15),
            date(2024, 2, 20),
            date(2024, 3, 25),
            None,
            date(2024, 5, 5),
        ], type=pa.date32()),
    })
    return write_with_options(
        table, "nested", "struct_with_temporal",
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )


# =============================================================================
# Dictionary-encoded integers
# =============================================================================

def generate_dict_integers_flat() -> dict:
    """Dictionary-encoded integer columns with low cardinality."""
    num_rows = 10000
    table = pa.table({
        "category_id": pa.array([i % 5 for i in range(num_rows)], type=pa.int64()),
        "status": pa.array([(i % 3) * 100 for i in range(num_rows)], type=pa.int64()),
        "priority": pa.array([i % 10 for i in range(num_rows)], type=pa.int64()),
    })
    return write_with_options(
        table, "encodings", "dict_integers_flat",
        use_dictionary=True,
        compression=None,
        write_statistics=False,
        data_page_version="1.0",
    )


def generate_dict_integers_list() -> dict:
    """List column with dictionary-encoded integer elements."""
    table = pa.table({
        "list_col": pa.array(
            [[1, 2, 3], [1, 1, 1], [2, 3], None, [1, 2, 3, 1, 2, 3]] * 2000,
            type=pa.list_(pa.int64()),
        ),
    })
    return write_with_options(
        table, "encodings", "dict_integers_list",
        use_dictionary=True,
        compression=None,
        write_statistics=False,
        data_page_version="1.0",
    )


def generate_dict_integers_mixed() -> dict:
    """Mixed dictionary and plain encoded integer columns."""
    num_rows = 5000
    table = pa.table({
        "id": pa.array(list(range(num_rows)), type=pa.int64()),
        "category": pa.array([i % 3 for i in range(num_rows)], type=pa.int64()),
        "value": pa.array([i * 10 for i in range(num_rows)], type=pa.int64()),
    })
    return write_with_options(
        table, "encodings", "dict_integers_mixed",
        use_dictionary=["category"],
        compression=None,
        write_statistics=False,
        data_page_version="1.0",
    )


# =============================================================================
# Multipage
# =============================================================================

def generate_multipage_large_column() -> dict:
    """Large column that spans multiple data pages."""
    num_rows = 500000
    table = pa.table({
        "id": pa.array(list(range(num_rows)), type=pa.int64()),
        "value": pa.array([i * 10 for i in range(num_rows)], type=pa.int64()),
    })
    return write_with_options(
        table, "multipage", "large_column",
        use_dictionary=False,
        compression=None,
        write_statistics=False,
        data_page_size=64 * 1024,
        data_page_version="1.0",
    )


def generate_multipage_with_page_index() -> dict:
    """Multi-page column with page index emitted.

    The id column is monotonically increasing so the ColumnIndex's
    boundary_order is ascending, which tests that our reader correctly
    consumes both ColumnIndex min/max and OffsetIndex first_row_index
    when skipping pages.
    """
    num_rows = 4000
    table = pa.table({
        "id": pa.array(list(range(num_rows)), type=pa.int32()),
        "bucket": pa.array([i // 1000 for i in range(num_rows)], type=pa.int32()),
    })
    return write_with_options(
        table, "multipage", "with_page_index",
        use_dictionary=False,
        compression=None,
        write_statistics=True,
        row_group_size=num_rows,
        data_page_size=1024,
        data_page_version="1.0",
        write_page_index=True,
    )


# =============================================================================
# Main
# =============================================================================

def generate_manifest(files: list[dict]) -> None:
    """Write manifest.json with metadata for all generated files."""
    manifest = {
        "description": "Canonical Parquet test files generated by PyArrow",
        "pyarrow_version": pa.__version__,
        "files": {f["path"]: f for f in files},
    }
    
    manifest_path = OUTPUT_DIR / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\nManifest written to: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet test files")
    parser.add_argument("--p0", action="store_true", help="Generate only P0 (MVP) files")
    args = parser.parse_args()
    
    files = []
    
    # P0: Basic files (always generated)
    print("Generating P0 files (basic)...")
    files.append(generate_basic_types_plain_uncompressed())
    files.append(generate_boundary_values())
    files.append(generate_nulls_and_empties())
    
    if not args.p0:
        # P1: Encodings
        print("\nGenerating P1 files (encodings)...")
        files.append(generate_dictionary_low_cardinality())
        files.append(generate_dictionary_high_cardinality())
        
        # Delta encodings
        print("\nGenerating delta encoding files...")
        files.append(generate_delta_binary_packed_int32())
        files.append(generate_delta_binary_packed_int64())
        files.append(generate_delta_length_byte_array())
        files.append(generate_delta_byte_array())
        files.append(generate_byte_stream_split_float())
        files.append(generate_byte_stream_split_double())
        files.append(generate_byte_stream_split_int32())
        files.append(generate_byte_stream_split_int64())
        files.append(generate_byte_stream_split_float16())
        files.append(generate_byte_stream_split_flba())
        files.append(generate_delta_with_nulls())
        files.append(generate_delta_strings_with_nulls())
        files.append(generate_byte_stream_split_with_nulls())
        
        # Dictionary-encoded integers
        print("\nGenerating dictionary integer files...")
        files.append(generate_dict_integers_flat())
        files.append(generate_dict_integers_list())
        files.append(generate_dict_integers_mixed())
        
        # P1: Compression
        print("\nGenerating P1 files (compression)...")
        files.append(generate_compression_none())
        files.append(generate_compression_zstd())
        files.append(generate_compression_gzip())
        try:
            files.append(generate_compression_snappy())
        except Exception as e:
            print(f"  Skipping snappy (not available): {e}")
        try:
            files.append(generate_compression_lz4())
        except Exception as e:
            print(f"  Skipping lz4 (not available): {e}")
        try:
            files.append(generate_compression_brotli())
        except Exception as e:
            print(f"  Skipping brotli (not available): {e}")
        
        # P1: Structure
        print("\nGenerating P1 files (structure)...")
        files.append(generate_multiple_row_groups())
        
        # Multipage
        print("\nGenerating multipage files...")
        files.append(generate_multipage_large_column())
        files.append(generate_multipage_with_page_index())
        
        # Edge cases
        print("\nGenerating edge case files...")
        files.append(generate_single_value())
        files.append(generate_empty_table())
        files.append(generate_large_strings())
        
        # Logical types (core)
        print("\nGenerating logical type files (core)...")
        files.append(generate_logical_string())
        files.append(generate_logical_date())
        files.append(generate_logical_timestamp())
        files.append(generate_logical_time())
        
        # Logical types (extended)
        print("\nGenerating logical type files (extended)...")
        files.append(generate_logical_decimal())
        files.append(generate_logical_uuid())
        files.append(generate_logical_int_types())
        try:
            files.append(generate_logical_float16())
        except Exception as e:
            print(f"  Skipping float16 (not available): {e}")
        files.append(generate_logical_enum())
        files.append(generate_logical_json())
        
        # Interop types (Phase 14)
        print("\nGenerating interop type files...")
        files.append(generate_interop_int96())
        
        # Nested types (Phase 3 prep)
        print("\nGenerating nested type files...")
        files.append(generate_list_int())
        files.append(generate_list_nullable())
        files.append(generate_list_of_list())
        files.append(generate_list_int64())
        files.append(generate_list_f32())
        files.append(generate_list_string())
        files.append(generate_list_float())
        files.append(generate_list_bool())
        files.append(generate_list_struct())
        files.append(generate_struct_simple())
        files.append(generate_struct_nested())
        files.append(generate_struct_with_list())
        files.append(generate_struct_with_temporal())
        files.append(generate_map_string_int())
        files.append(generate_map_string_string())
        files.append(generate_map_int_int())
    
    # Generate manifest
    generate_manifest(files)
    
    print(f"\nDone! Generated {len(files)} test files.")


if __name__ == "__main__":
    main()
