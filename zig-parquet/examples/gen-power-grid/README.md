# Power Grid Monitor — Benchmark & Showcase

This sub-project demonstrates how to use `zig-parquet` for streaming high-frequency time-series data.
It generates 1 minute of synthetic 3-phase power grid monitoring data (15,000 rows) at 250 Hz and writes it to a Parquet file in batches.

The 10 columns capture voltage and current waveforms across three phases, grid frequency, and power factor — all as integers with realistic harmonic distortion and load variation.

By using the `.delta_binary_packed` encoding for integers, it showcases massive compression ratios compared to raw storage.

## Run

```bash
zig build run
```
