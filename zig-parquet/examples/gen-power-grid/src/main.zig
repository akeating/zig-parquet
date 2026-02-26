//! Generate synthetic power grid monitoring data as a Parquet file
//!
//! Simulates a 3-phase substation power quality monitor capturing waveform data.
//!
//! Schema:
//!   - timestamp_ms: i64 (millis since epoch, increments by 4ms)
//!   - seq: i64 (sequence number, starts at 0)
//!   - voltage_a, voltage_b, voltage_c: i32 (3-phase voltages in millivolts)
//!   - current_a, current_b, current_c: i32 (3-phase currents in milliamps)
//!   - frequency: i32 (grid frequency in microhertz, ~60,000,000)
//!   - power_factor: i32 (power factor × 1,000,000, e.g. 950000 = 0.95)
//!
//! Duration: 1 minute = 15,000 samples at 250 Hz (4ms interval)
//! Uses RowWriter with batch writes and periodic flush() for streaming row groups.

const std = @import("std");
const parquet = @import("parquet");

const GridSample = struct {
    timestamp_ms: i64,
    seq: i64,
    voltage_a: i32,
    voltage_b: i32,
    voltage_c: i32,
    current_a: i32,
    current_b: i32,
    current_c: i32,
    frequency: i32,
    power_factor: i32,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const sample_interval_ms: i64 = 4; // 250 Hz
    const duration_seconds: i64 = 60;
    const duration_ms: i64 = duration_seconds * 1000;
    const total_samples: usize = @intCast(@divExact(duration_ms, sample_interval_ms));
    const batch_size: usize = 2500; // 10 seconds of data (250 Hz × 10s), flush after each batch

    // Start timestamp: 2024-01-01 00:00:00 UTC
    const start_timestamp_ms: i64 = 1704067200000;

    const num_batches = (total_samples + batch_size - 1) / batch_size;

    std.debug.print("Generating power grid monitoring data:\n", .{});
    std.debug.print("  Duration: {} seconds\n", .{duration_seconds});
    std.debug.print("  Sample rate: 250 Hz ({}ms interval)\n", .{sample_interval_ms});
    std.debug.print("  Total samples: {}\n", .{total_samples});
    std.debug.print("  Channels: 3-phase V/I + frequency + power factor\n", .{});
    std.debug.print("  Batch size: {} (10 seconds)\n", .{batch_size});
    std.debug.print("  Row groups: {}\n", .{num_batches});

    const file = try std.fs.cwd().createFile("grid_data.parquet", .{});
    defer file.close();

    // Delta encoding is optimal for:
    //   - timestamp_ms: constant +4ms deltas → near-zero storage
    //   - seq: constant +1 deltas → near-zero storage
    //   - voltages/currents: AC waveform deltas are smaller than raw values
    //   - frequency: near-constant with tiny drift → excellent deltas
    //   - power_factor: slowly varying → small deltas
    var writer = try parquet.writeToFileRows(GridSample, allocator, file, .{
        .compression = .zstd,
        .use_dictionary = false,
        .int_encoding = .delta_binary_packed,
    });
    defer writer.deinit();

    var batch = try allocator.alloc(GridSample, batch_size);
    defer allocator.free(batch);

    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    const grid_freq_hz: f64 = 60.0;
    const voltage_peak_mv: f64 = 169_706.0; // 120V RMS ≈ 169.7V peak, in millivolts
    const current_peak_ma: f64 = 29_698.0; // 21A RMS ≈ 29.7A peak, in milliamps

    // 3-phase offsets: 0°, 120°, 240°
    const phase_offsets = [3]f64{ 0.0, 2.0 * std.math.pi / 3.0, 4.0 * std.math.pi / 3.0 };

    // Current lags voltage by the power-factor angle (~18° for PF = 0.95)
    var current_lag: f64 = std.math.acos(@as(f64, 0.95));
    var freq_drift: f64 = 0.0;

    std.debug.print("\nWriting batches...\n", .{});
    var timer = try std.time.Timer.start();

    var samples_written: usize = 0;
    for (0..num_batches) |batch_idx| {
        const batch_start = batch_idx * batch_size;
        const this_batch_size = @min(batch_size, total_samples - batch_start);

        for (0..this_batch_size) |i| {
            const sample_idx = batch_start + i;
            const t: f64 = @as(f64, @floatFromInt(sample_idx)) * 0.004; // time in seconds

            // Grid frequency with slow mean-reverting drift (±0.05 Hz)
            freq_drift += (@as(f64, @floatFromInt(random.int(i32))) / @as(f64, @floatFromInt(std.math.maxInt(i32)))) * 0.00001;
            freq_drift *= 0.9999;
            const inst_freq = grid_freq_hz + freq_drift;
            const omega = 2.0 * std.math.pi * inst_freq;

            // 3-phase voltages with harmonic distortion (3rd + 5th)
            var voltages: [3]i32 = undefined;
            for (0..3) |ph| {
                const angle = omega * t + phase_offsets[ph];
                const fundamental = @sin(angle);
                const third_harmonic = 0.02 * @sin(3.0 * angle);
                const fifth_harmonic = 0.01 * @sin(5.0 * angle);
                const noise = (@as(f64, @floatFromInt(random.int(i32))) / @as(f64, @floatFromInt(std.math.maxInt(i32)))) * 0.005;
                const v = (fundamental + third_harmonic + fifth_harmonic + noise) * voltage_peak_mv;
                voltages[ph] = @intFromFloat(std.math.clamp(v, -200_000.0, 200_000.0));
            }

            // 3-phase currents: lag voltage, higher harmonic content, hourly load cycle
            var currents: [3]i32 = undefined;
            const load_variation = 1.0 + 0.15 * @sin(2.0 * std.math.pi * t / 3600.0);
            for (0..3) |ph| {
                const angle = omega * t + phase_offsets[ph] - current_lag;
                const fundamental = @sin(angle);
                const third_harmonic = 0.05 * @sin(3.0 * angle);
                const fifth_harmonic = 0.03 * @sin(5.0 * angle);
                const noise = (@as(f64, @floatFromInt(random.int(i32))) / @as(f64, @floatFromInt(std.math.maxInt(i32)))) * 0.01;
                const c = (fundamental + third_harmonic + fifth_harmonic + noise) * current_peak_ma * load_variation;
                currents[ph] = @intFromFloat(std.math.clamp(c, -50_000.0, 50_000.0));
            }

            // Frequency in microhertz (e.g. 60,000,000 µHz = 60.000000 Hz)
            const freq_uhz: i32 = @intFromFloat(inst_freq * 1_000_000.0);

            // Power factor: slowly varying around 0.95, clamped to realistic range
            current_lag += (@as(f64, @floatFromInt(random.int(i32))) / @as(f64, @floatFromInt(std.math.maxInt(i32)))) * 0.000001;
            current_lag = std.math.clamp(current_lag, 0.05, 0.6); // PF ≈ 0.83 to 0.998
            const pf: i32 = @intFromFloat(@cos(current_lag) * 1_000_000.0);

            batch[i] = .{
                .timestamp_ms = start_timestamp_ms + @as(i64, @intCast(sample_idx)) * sample_interval_ms,
                .seq = @intCast(sample_idx),
                .voltage_a = voltages[0],
                .voltage_b = voltages[1],
                .voltage_c = voltages[2],
                .current_a = currents[0],
                .current_b = currents[1],
                .current_c = currents[2],
                .frequency = freq_uhz,
                .power_factor = pf,
            };
        }

        try writer.writeRows(batch[0..this_batch_size]);
        try writer.flush();

        samples_written += this_batch_size;

        if (batch_idx == num_batches - 1) {
            const elapsed_s = @as(f64, @floatFromInt(timer.read())) / 1_000_000_000.0;
            const rate = @as(f64, @floatFromInt(samples_written)) / elapsed_s;
            std.debug.print("  {} samples written ({d:.0} samples/sec)\n", .{ samples_written, rate });
        }
    }

    try writer.close();
    const elapsed_s = @as(f64, @floatFromInt(timer.read())) / 1_000_000_000.0;

    const stat = try std.fs.cwd().statFile("grid_data.parquet");
    const size_mb = @as(f64, @floatFromInt(stat.size)) / (1024.0 * 1024.0);

    std.debug.print("\nDone!\n", .{});
    std.debug.print("  Output: grid_data.parquet\n", .{});
    std.debug.print("  Size: {d:.2} MB\n", .{size_mb});
    std.debug.print("  Rows: {}\n", .{total_samples});
    std.debug.print("  Row groups: {}\n", .{num_batches});
    std.debug.print("  Time: {d:.1}s\n", .{elapsed_s});
    std.debug.print("  Rate: {d:.0} rows/sec\n", .{@as(f64, @floatFromInt(total_samples)) / elapsed_s});

    const raw_size = total_samples * (8 + 8 + 8 * 4); // 2 i64 + 8 i32
    const compression_ratio = @as(f64, @floatFromInt(raw_size)) / @as(f64, @floatFromInt(stat.size));
    std.debug.print("  Raw size would be: {d:.2} MB\n", .{@as(f64, @floatFromInt(raw_size)) / (1024.0 * 1024.0)});
    std.debug.print("  Compression ratio: {d:.1}x\n", .{compression_ratio});
}
