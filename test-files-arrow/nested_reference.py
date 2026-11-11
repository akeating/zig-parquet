"""Write PyArrow's reading of nested columns as canonical text, for the nested-column test in
zig-parquet/src/core/arrow_batch.zig.

generate.py calls write("nested_reference.json") after generating the test files. To run it on
its own (from test-files-arrow, with uv):
    uv run python -I nested_reference.py nested_reference.json

For every .parquet file under ../test-files-arrow and ../test-files-wild with at least one
list, map, struct or decimal top-level column and at most MAX_ROWS rows, writes the first
ROWS_PER_GROUP rows of each row group as
    {"<root>/<path>": {"<row group>:<top-level column index>": "<canonical text>", ...}, ...}
Files PyArrow cannot read are recorded as {"error": "<message>"}.

Canonical text, one line per row:
    null            n
    list            [e1,e2,...]
    map             [(k1:v1),(k2:v2),...]
    struct          {f1,f2,...}           (fields in schema order)
    integer         i<value>              (unsigned 32/64-bit values as their signed bit pattern)
    boolean         t / f
    float, double   d<hex of the little-endian IEEE double>
    string, binary  x<hex of the bytes>
    decimal         e<unscaled integer>   (the value times 10^scale)
    other leaves    v                     (only presence is compared)
"""
import decimal
import json
import os
import struct
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

ROOTS = ["../test-files-arrow", "../test-files-wild"]
# Exact unscaled values for decimal256 (up to 76 digits).
decimal.getcontext().prec = 100
MAX_ROWS = 200_000
# Rows compared per row group and column (the first ones).
ROWS_PER_GROUP = 2_000


def storage(t):
    return t.storage_type if isinstance(t, pa.BaseExtensionType) else t


def is_decimal(t):
    return pa.types.is_decimal(storage(t))


def is_nested(t):
    t = storage(t)
    return pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_map(t) or pa.types.is_struct(t)


def canon(v, t):
    """Canonical text of one Python value `v` (from to_pylist) of Arrow type `t`."""
    if v is None:
        return "n"
    t = storage(t)
    if pa.types.is_map(t):
        return "[" + ",".join(f"({canon(k, t.key_type)}:{canon(it, t.item_type)})" for k, it in v) + "]"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return "[" + ",".join(canon(x, t.value_type) for x in v) + "]"
    if pa.types.is_struct(t):
        return "{" + ",".join(canon(v[t.field(k).name], t.field(k).type) for k in range(t.num_fields)) + "}"
    return leaf(v, t)


def leaf(v, t):
    if pa.types.is_boolean(t):
        return "t" if v else "f"
    if pa.types.is_integer(t):
        if pa.types.is_unsigned_integer(t) and t.bit_width >= 32 and v >= 1 << (t.bit_width - 1):
            v -= 1 << t.bit_width
        return f"i{v}"
    if pa.types.is_decimal(t):
        return f"e{int(v.scaleb(t.scale))}"
    if pa.types.is_float32(t) or pa.types.is_float64(t):
        return "d" + struct.pack("<d", float(v)).hex()
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "x" + v.encode().hex()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "x" + (v.bytes if isinstance(v, uuid.UUID) else bytes(v)).hex()
    return "v"


def file_reference(path):
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows > MAX_ROWS:
        return None
    fields = list(pf.schema_arrow)
    if not any(is_nested(f.type) or is_decimal(f.type) for f in fields):
        return None
    out = {}
    for rg in range(pf.num_row_groups):
        table = pf.read_row_group(rg)
        for c, field in enumerate(fields):
            values = table.column(c).slice(0, ROWS_PER_GROUP).to_pylist()
            out[f"{rg}:{c}"] = "\n".join(canon(v, field.type) for v in values)
    return out


def write(out_path):
    result = {}
    for root in ROOTS:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d != ".venv"]
            for name in sorted(filenames):
                if not name.endswith(".parquet"):
                    continue
                path = os.path.join(dirpath, name)
                try:
                    ref = file_reference(path)
                except Exception as e:  # files PyArrow rejects
                    ref = {"error": f"{type(e).__name__}: {e}"}
                if ref is not None:
                    result[path] = ref
    with open(out_path, "w") as f:
        json.dump(result, f, indent=0, sort_keys=True)
    readable = sum(1 for v in result.values() if "error" not in v)
    print(f"{len(result)} files with nested or decimal columns ({readable} read by PyArrow {pa.__version__})")


if __name__ == "__main__":
    write(sys.argv[1])
