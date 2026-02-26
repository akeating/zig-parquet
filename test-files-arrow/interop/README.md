# Interop Test Files

These Parquet files are **written by zig-parquet** (not PyArrow) during `zig build test`.
They are then read and validated by `../test_interop.py` using PyArrow to confirm
cross-implementation compatibility.

Do not edit these files manually — they are regenerated on every test run.
