# Large File Roundtrip

This sub-project acts as an integration and stress test for `zig-parquet`.

It generates a large Parquet file spanning multiple row groups and pages, demonstrating usage of complex schema topologies, dictionary encodings, compression, and then verifies it by dynamically reading it back using `DynamicReader`.

## Run

```bash
zig build run
```