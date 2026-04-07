//! Zstandard (zstd) compression/decompression for Parquet
//!
//! Both compression and decompression are pure Zig implementations.
//! Decompression uses std.compress.zstd.
//! Compression implements zstd level 1: greedy LZ matching with repeated
//! offset tracking, raw literals, and predefined FSE tables.

const std = @import("std");
const zstd_lib = std.compress.zstd;
const safe = @import("../safe.zig");

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};

// =========================================================================
// Constants
// =========================================================================

const ZSTD_MAGIC: u32 = 0xFD2FB528;
const BLOCK_SIZE_MAX: usize = zstd_lib.block_size_max; // 1 << 17 = 128KB
const MIN_MATCH: usize = 4;
const HASH_LOG: u5 = 17;
const HASH_SIZE: usize = 1 << HASH_LOG;
const HASH_PRIME: u64 = 2654435761;

// =========================================================================
// Code lookup tables (comptime-built from std.compress.zstd code tables)
// =========================================================================

const ll_code_lut: [64]u8 = blk: {
    @setEvalBranchQuota(10000);
    var table: [64]u8 = @splat(0);
    for (0..64) |i| {
        for (0..36) |c| {
            if (zstd_lib.literals_length_code_table[c][0] <= i) {
                table[i] = @intCast(c);
            }
        }
    }
    break :blk table;
};

const ml_code_lut: [128]u8 = blk: {
    @setEvalBranchQuota(20000);
    var table: [128]u8 = @splat(0);
    for (0..128) |i| {
        const match_len = i + 3;
        for (0..53) |c| {
            if (zstd_lib.match_length_code_table[c][0] <= match_len) {
                table[i] = @intCast(c);
            }
        }
    }
    break :blk table;
};

fn getLLCode(lit_length: u32) u8 {
    if (lit_length < 64) return ll_code_lut[lit_length];
    return safe.castTo(u8, highbit32(lit_length) + 19) catch unreachable; // max 31 + 19 = 50
}

fn getMLCode(match_length: u32) u8 {
    const ml_base = match_length - 3; // match_length >= MIN_MATCH=4, so ml_base >= 1
    if (ml_base < 128) return ml_code_lut[ml_base];
    return safe.castTo(u8, highbit32(ml_base) + 36) catch unreachable; // max 31 + 36 = 67
}

fn getOFCode(off_base: u32) u5 {
    return safe.castTo(u5, highbit32(off_base)) catch unreachable; // highbit32 returns 0-31
}

fn getLLBits(code: u8) u5 {
    return zstd_lib.literals_length_code_table[code][1];
}

fn getMLBits(code: u8) u5 {
    return zstd_lib.match_length_code_table[code][1];
}

fn highbit32(val: u32) u32 {
    if (val == 0) return 0;
    return 31 - @as(u32, @clz(val));
}

// =========================================================================
// Bitstream Writer (forward, matching zstd BIT_CStream format)
// =========================================================================

const BitWriter = struct {
    container: u64 = 0,
    bit_pos: u32 = 0,
    buf: []u8,
    pos: usize = 0,
    overflow: bool = false,

    fn init(buf: []u8) BitWriter {
        return .{ .buf = buf };
    }

    fn addBits(self: *BitWriter, value: u64, nb_bits: u6) void {
        if (nb_bits == 0) return;
        if (self.bit_pos + nb_bits >= 64) {
            self.overflow = true;
            return;
        }
        const mask = (@as(u64, 1) << nb_bits) - 1;
        self.container |= (value & mask) << @intCast(self.bit_pos); // bit_pos < 64 checked above
        self.bit_pos += nb_bits;
    }

    fn flush(self: *BitWriter) void {
        while (self.bit_pos >= 8) {
            if (self.pos >= self.buf.len) {
                self.overflow = true;
                return;
            }
            self.buf[self.pos] = @truncate(self.container);
            self.pos += 1;
            self.container >>= 8;
            self.bit_pos -= 8;
        }
    }

    fn close(self: *BitWriter) ?usize {
        self.addBits(1, 1);
        self.flush();
        if (self.overflow) return null;
        if (self.bit_pos > 0) {
            if (self.pos >= self.buf.len) return null;
            self.buf[self.pos] = @truncate(self.container);
            self.pos += 1;
        }
        return self.pos;
    }
};

// =========================================================================
// FSE Encoding Tables (comptime from predefined distributions)
// =========================================================================

fn FseEncTable(comptime num_symbols: usize, comptime table_log: comptime_int) type {
    const table_size: usize = 1 << table_log;

    return struct {
        const Self = @This();

        const SymbolTT = struct {
            delta_nb_bits: u32,
            delta_find_state: i32,
        };

        symbol_tt: [num_symbols]SymbolTT,
        state_table: [table_size]u16,

        fn build(comptime dist: anytype) Self {
            @setEvalBranchQuota(100000);
            var result: Self = undefined;

            // Phase 1: place "less than 1" probability symbols at table end
            var table_symbol: [table_size]u16 = undefined;
            var high_threshold: usize = table_size - 1;

            for (0..num_symbols) |s| {
                if (dist[s] == -1) {
                    table_symbol[high_threshold] = @intCast(s);
                    high_threshold -= 1;
                }
            }

            // Phase 2: spread remaining symbols using step
            const step = (table_size >> 1) + (table_size >> 3) + 3;
            const mask = table_size - 1;
            var position: usize = 0;

            for (0..num_symbols) |s| {
                if (dist[s] <= 0) continue; // skip -1 (already placed) and 0
                const freq: usize = @intCast(dist[s]);
                for (0..freq) |_| {
                    table_symbol[position] = @intCast(s);
                    position = (position + step) & mask;
                    while (position > high_threshold) {
                        position = (position + step) & mask;
                    }
                }
            }

            // Phase 3: build state table
            var cumul: [num_symbols + 1]usize = undefined;
            cumul[0] = 0;
            for (0..num_symbols) |s| {
                const c: usize = if (dist[s] == -1) 1 else if (dist[s] > 0) @intCast(dist[s]) else 0;
                cumul[s + 1] = cumul[s] + c;
            }

            var cumul_copy = cumul;
            for (0..table_size) |u| {
                const s = table_symbol[u];
                result.state_table[cumul_copy[s]] = @intCast(table_size + u);
                cumul_copy[s] += 1;
            }

            // Phase 4: build symbol transform table
            var total: i32 = 0;
            for (0..num_symbols) |s| {
                const count = dist[s];
                if (count == 0) {
                    result.symbol_tt[s] = .{
                        .delta_nb_bits = ((@as(u32, table_log) + 1) << 16) -| (@as(u32, 1) << @intCast(table_log)),
                        .delta_find_state = 0,
                    };
                } else if (count == -1 or count == 1) {
                    result.symbol_tt[s] = .{
                        .delta_nb_bits = (@as(u32, table_log) << 16) -| (@as(u32, 1) << @intCast(table_log)),
                        .delta_find_state = total - 1,
                    };
                    total += 1;
                } else {
                    const c: u32 = @intCast(count);
                    const hb = 31 - @as(u5, @clz(c - 1));
                    const max_bits_out: u32 = @as(u32, table_log) - hb;
                    const min_state_plus: u32 = c << @intCast(max_bits_out);
                    result.symbol_tt[s] = .{
                        .delta_nb_bits = (max_bits_out << 16) -| min_state_plus,
                        .delta_find_state = total - @as(i32, @intCast(c)),
                    };
                    total += @intCast(c);
                }
            }

            return result;
        }

        fn initState(self: *const Self, symbol: u8) u32 {
            const st = self.symbol_tt[symbol];
            const init_value: u32 = table_size;
            const nb_bits_out: u5 = @intCast((init_value +% @as(u32, @bitCast(st.delta_nb_bits))) >> 16);
            const reduced: u32 = init_value >> nb_bits_out;
            const idx = @as(i32, @intCast(reduced)) + st.delta_find_state;
            return self.state_table[@intCast(idx)];
        }

        fn encodeSymbol(self: *const Self, bw: *BitWriter, state: *u32, symbol: u8) void {
            const st = self.symbol_tt[symbol];
            const nb_bits_out: u5 = @intCast((state.* +% @as(u32, @bitCast(st.delta_nb_bits))) >> 16);
            bw.addBits(state.*, @intCast(nb_bits_out));
            const reduced: u32 = state.* >> nb_bits_out;
            const idx = @as(i32, @intCast(reduced)) + st.delta_find_state;
            state.* = self.state_table[@intCast(idx)];
        }

        fn flushState(bw: *BitWriter, state: u32, accuracy_log: u6) void {
            bw.addBits(state, accuracy_log);
            bw.flush();
        }
    };
}

const LLEncTable = FseEncTable(36, zstd_lib.default_accuracy_log.literal);
const MLEncTable = FseEncTable(53, zstd_lib.default_accuracy_log.match);
const OFEncTable = FseEncTable(29, zstd_lib.default_accuracy_log.offset);

const ll_enc: LLEncTable = LLEncTable.build(zstd_lib.literals_length_default_distribution);
const ml_enc: MLEncTable = MLEncTable.build(zstd_lib.match_lengths_default_distribution);
const of_enc: OFEncTable = OFEncTable.build(zstd_lib.offset_codes_default_distribution);

// =========================================================================
// Sequence type
// =========================================================================

const Sequence = struct {
    lit_length: u32,
    match_length: u32,
    offset: u32, // 0 = rep[0] match, >= 1 = raw distance
};

// =========================================================================
// Match Finder
// =========================================================================

fn hash4(ptr: [*]const u8) u32 {
    const val: u32 = std.mem.readInt(u32, ptr[0..4], .little);
    const h: u32 = val *% @as(u32, @truncate(HASH_PRIME));
    return h >> @intCast(32 - @as(u32, HASH_LOG));
}

fn findSequences(
    src: []const u8,
    hash_table: []u32,
    sequences: *std.ArrayListUnmanaged(Sequence),
    allocator: std.mem.Allocator,
) error{OutOfMemory}!void {
    if (src.len < MIN_MATCH) return;

    @memset(hash_table, 0);

    var rep: [3]u32 = .{
        zstd_lib.start_repeated_offset_1,
        zstd_lib.start_repeated_offset_2,
        zstd_lib.start_repeated_offset_3,
    };

    var anchor: usize = 0;
    var ip: usize = 0;
    const ip_limit = src.len - MIN_MATCH;

    while (ip <= ip_limit) {
        // Check rep[0] before hash lookup, but only when lit_length > 0.
        // When lit_length == 0, off_base=1 means rep[1] in the zstd spec
        // (RFC 8878 sec 3.1.2.1.2), so we skip rep matching for adjacent matches.
        if (ip > anchor and rep[0] <= ip) {
            const rep_candidate = ip - rep[0];
            if (std.mem.readInt(u32, src[rep_candidate..][0..4], .little) ==
                std.mem.readInt(u32, src[ip..][0..4], .little))
            {
                var match_len: usize = 4;
                while (ip + match_len < src.len and
                    rep_candidate + match_len < ip and
                    src[rep_candidate + match_len] == src[ip + match_len])
                {
                    match_len += 1;
                }

                const lit_length = ip - anchor;
                try sequences.append(allocator, .{
                    .lit_length = safe.castTo(u32, lit_length) catch unreachable, // bounded by BLOCK_SIZE_MAX
                    .match_length = safe.castTo(u32, match_len) catch unreachable, // bounded by BLOCK_SIZE_MAX
                    .offset = 0, // rep[0] match
                });

                ip += match_len;
                anchor = ip;
                continue;
            }
        }

        const h = hash4(src.ptr + ip);
        const candidate = hash_table[h];
        hash_table[h] = safe.castTo(u32, ip) catch unreachable; // ip < BLOCK_SIZE_MAX (128KB)

        if (candidate < ip and
            std.mem.readInt(u32, src[candidate..][0..4], .little) ==
            std.mem.readInt(u32, src[ip..][0..4], .little))
        {
            var match_len: usize = 4;
            while (ip + match_len < src.len and
                candidate + match_len < ip and
                src[candidate + match_len] == src[ip + match_len])
            {
                match_len += 1;
            }

            const lit_length = ip - anchor;
            const distance = ip - candidate;

            try sequences.append(allocator, .{
                .lit_length = safe.castTo(u32, lit_length) catch unreachable, // bounded by BLOCK_SIZE_MAX
                .match_length = safe.castTo(u32, match_len) catch unreachable, // bounded by BLOCK_SIZE_MAX
                .offset = safe.castTo(u32, distance) catch unreachable, // bounded by BLOCK_SIZE_MAX
            });

            rep[2] = rep[1];
            rep[1] = rep[0];
            rep[0] = safe.castTo(u32, distance) catch unreachable; // bounded by BLOCK_SIZE_MAX

            ip += match_len;
            anchor = ip;
        } else {
            ip += 1;
        }
    }
}

// =========================================================================
// Block Encoder
// =========================================================================

fn encodeBlock(allocator: std.mem.Allocator, src: []const u8) Error!?[]u8 {
    if (src.len < MIN_MATCH) return null;

    // Allocate hash table
    const hash_table = allocator.alloc(u32, HASH_SIZE) catch return error.OutOfMemory;
    defer allocator.free(hash_table);

    // Find matches
    var sequences: std.ArrayListUnmanaged(Sequence) = .empty;
    defer sequences.deinit(allocator);

    findSequences(src, hash_table, &sequences, allocator) catch return error.OutOfMemory;
    if (sequences.items.len == 0) return null;

    const nb_seq = sequences.items.len;

    var total_lit_len: usize = 0;
    var last_pos: usize = 0;
    for (sequences.items) |seq| {
        total_lit_len += seq.lit_length;
        last_pos += seq.lit_length + seq.match_length;
    }
    const trailing_lit = src.len - last_pos;
    total_lit_len += trailing_lit;

    const max_block_out = src.len * 2 + 64;
    var block_buf = allocator.alloc(u8, max_block_out) catch return error.OutOfMemory;

    var out_pos: usize = 0;

    // --- Literals Section (raw mode) ---
    const lit_hdr_size = writeLiteralsHeader(block_buf[out_pos..], total_lit_len);
    out_pos += lit_hdr_size;

    var copy_pos: usize = 0;
    for (sequences.items) |seq| {
        const ll: usize = seq.lit_length;
        if (ll > 0) {
            @memcpy(block_buf[out_pos..][0..ll], src[copy_pos..][0..ll]);
            out_pos += ll;
        }
        copy_pos += ll + seq.match_length;
    }
    if (trailing_lit > 0) {
        @memcpy(block_buf[out_pos..][0..trailing_lit], src[copy_pos..][0..trailing_lit]);
        out_pos += trailing_lit;
    }

    // --- Sequences Section ---
    if (nb_seq < 128) {
        block_buf[out_pos] = safe.castTo(u8, nb_seq) catch unreachable; // nb_seq < 128
        out_pos += 1;
    } else if (nb_seq < 0x7F00) {
        block_buf[out_pos] = safe.castTo(u8, (nb_seq >> 8) + 0x80) catch unreachable; // nb_seq < 0x7F00 → (nb_seq>>8)+0x80 < 0xFF
        block_buf[out_pos + 1] = safe.castTo(u8, nb_seq & 0xFF) catch unreachable; // masked to 8 bits
        out_pos += 2;
    } else {
        block_buf[out_pos] = 0xFF;
        std.mem.writeInt(u16, block_buf[out_pos + 1 ..][0..2], safe.castTo(u16, nb_seq - 0x7F00) catch unreachable, .little); // nb_seq bounded by BLOCK_SIZE_MAX/MIN_MATCH
        out_pos += 3;
    }

    block_buf[out_pos] = 0; // compression modes: all predefined
    out_pos += 1;

    // --- FSE Bitstream (written directly into remaining block_buf space) ---
    const bitstream_size = encodeSequences(sequences.items, block_buf[out_pos..]) orelse {
        allocator.free(block_buf);
        return null;
    };
    out_pos += bitstream_size;

    if (out_pos >= src.len) {
        allocator.free(block_buf);
        return null;
    }

    const result = allocator.realloc(block_buf, out_pos) catch {
        return block_buf[0..out_pos];
    };
    return result;
}

fn encodeSequences(sequences: []const Sequence, bitstream_buf: []u8) ?usize {
    const nb_seq = sequences.len;
    if (nb_seq == 0) return null;

    var bw = BitWriter.init(bitstream_buf);

    // Process last sequence first (init states, no FSE bits emitted)
    const last = sequences[nb_seq - 1];
    const last_off_base = if (last.offset == 0) @as(u32, 1) else std.math.add(u32, last.offset, 3) catch return null;
    const last_of_code = getOFCode(last_off_base);
    const last_ml_code = getMLCode(last.match_length);
    const last_ll_code = getLLCode(last.lit_length);

    var of_state = of_enc.initState(last_of_code);
    var ml_state = ml_enc.initState(last_ml_code);
    var ll_state = ll_enc.initState(last_ll_code);

    // Extra bits for last sequence
    bw.addBits(last.lit_length, getLLBits(last_ll_code));
    bw.addBits(last.match_length - 3, getMLBits(last_ml_code));
    bw.addBits(last_off_base, last_of_code);
    bw.flush();

    // Process remaining sequences in reverse
    if (nb_seq > 1) {
        var n: usize = nb_seq - 2;
        while (true) {
            const seq = sequences[n];
            const off_base = if (seq.offset == 0) @as(u32, 1) else std.math.add(u32, seq.offset, 3) catch return null;
            const of_code = getOFCode(off_base);
            const ml_code = getMLCode(seq.match_length);
            const ll_code = getLLCode(seq.lit_length);

            // FSE encode (order: OF, ML, LL)
            of_enc.encodeSymbol(&bw, &of_state, of_code);
            ml_enc.encodeSymbol(&bw, &ml_state, ml_code);
            ll_enc.encodeSymbol(&bw, &ll_state, ll_code);
            bw.flush();

            // Extra bits (order: LL, ML, OF)
            bw.addBits(seq.lit_length, getLLBits(ll_code));
            bw.addBits(seq.match_length - 3, getMLBits(ml_code));
            bw.addBits(off_base, of_code);
            bw.flush();

            if (n == 0) break;
            n -= 1;
        }
    }

    // Flush final FSE states (order: ML, OF, LL)
    MLEncTable.flushState(&bw, ml_state, zstd_lib.default_accuracy_log.match);
    OFEncTable.flushState(&bw, of_state, zstd_lib.default_accuracy_log.offset);
    LLEncTable.flushState(&bw, ll_state, zstd_lib.default_accuracy_log.literal);

    return bw.close();
}

fn writeLiteralsHeader(dst: []u8, size: usize) usize {
    if (size <= 31) {
        // 1-byte header: bits[1:0]=0 (raw), bits[7:3]=size
        dst[0] = safe.castTo(u8, size << 3) catch unreachable; // size<=31 → size<<3 <= 248
        return 1;
    } else if (size <= 4095) {
        // 2-byte header: bits[1:0]=0 (raw), bits[3:2]=1 (size_format), bits[15:4]=size
        const val = safe.castTo(u16, (size << 4) | (1 << 2)) catch unreachable; // size<=4095 → val <= 65524
        std.mem.writeInt(u16, dst[0..2], val, .little);
        return 2;
    } else {
        // 3-byte header: bits[1:0]=0 (raw), bits[3:2]=3, bits[23:4]=size
        const val = safe.castTo(u32, (size << 4) | (3 << 2)) catch unreachable; // size bounded by BLOCK_SIZE_MAX
        dst[0] = @truncate(val);
        dst[1] = @truncate(val >> 8);
        dst[2] = @truncate(val >> 16);
        return 3;
    }
}

// =========================================================================
// Frame Writer / Public API
// =========================================================================

pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    // Max output: frame header + blocks (each with 3-byte header + data)
    const num_blocks = if (data.len == 0) 1 else (data.len + BLOCK_SIZE_MAX - 1) / BLOCK_SIZE_MAX;
    const max_output = 4 + 14 + num_blocks * (3 + BLOCK_SIZE_MAX) + data.len;
    const frame_buf = allocator.alloc(u8, max_output) catch return error.OutOfMemory;
    errdefer allocator.free(frame_buf);

    var pos: usize = 0;

    // Magic number
    std.mem.writeInt(u32, frame_buf[pos..][0..4], ZSTD_MAGIC, .little);
    pos += 4;

    // Frame header
    pos += writeFrameHeader(frame_buf[pos..], data.len);

    // Blocks
    if (data.len == 0) {
        // Empty last raw block
        writeBlockHeader(frame_buf[pos..], 0, 0, true);
        pos += 3;
    } else {
        var block_start: usize = 0;
        while (block_start < data.len) {
            const block_end = @min(block_start + BLOCK_SIZE_MAX, data.len);
            const block_data = data[block_start..block_end];
            const is_last = (block_end == data.len);

            const compressed_block = encodeBlock(allocator, block_data) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => null,
            };

            if (compressed_block) |cb| {
                defer allocator.free(cb);
                writeBlockHeader(frame_buf[pos..], cb.len, 2, is_last);
                pos += 3;
                @memcpy(frame_buf[pos..][0..cb.len], cb);
                pos += cb.len;
            } else {
                // Raw block
                writeBlockHeader(frame_buf[pos..], block_data.len, 0, is_last);
                pos += 3;
                @memcpy(frame_buf[pos..][0..block_data.len], block_data);
                pos += block_data.len;
            }

            block_start = block_end;
        }
    }

    // Resize to actual size
    const result = allocator.realloc(frame_buf, pos) catch {
        return frame_buf[0..pos];
    };
    return result;
}

fn writeFrameHeader(dst: []u8, content_size: usize) usize {
    // Frame_Header_Descriptor: Single_Segment=1, no checksum, no dict
    const fcs = fcsParams(content_size);
    dst[0] = (@as(u8, fcs.flag) << 6) | 0x20;

    switch (fcs.flag) {
        0 => {
            dst[1] = safe.castTo(u8, content_size) catch unreachable; // fcsParams guarantees size <= 255
            return 2;
        },
        1 => {
            std.mem.writeInt(u16, dst[1..3], safe.castTo(u16, content_size - 256) catch unreachable, .little); // size <= 65791 → val <= 65535
            return 3;
        },
        2 => {
            std.mem.writeInt(u32, dst[1..5], safe.castTo(u32, content_size) catch unreachable, .little); // size <= maxInt(u32)
            return 5;
        },
        3 => {
            std.mem.writeInt(u64, dst[1..9], safe.castTo(u64, content_size) catch unreachable, .little); // usize fits in u64
            return 9;
        },
    }
}

fn fcsParams(content_size: usize) struct { flag: u2 } {
    if (content_size <= 255) return .{ .flag = 0 };
    if (content_size <= 65791) return .{ .flag = 1 };
    if (content_size <= std.math.maxInt(u32)) return .{ .flag = 2 };
    return .{ .flag = 3 };
}

fn writeBlockHeader(dst: []u8, size: usize, block_type: u2, is_last: bool) void {
    // size <= BLOCK_SIZE_MAX (128KB), size<<3 fits in 21 bits, total fits in u24
    const val: u24 = safe.castTo(u24, (size << 3) | (@as(usize, block_type) << 1) | @intFromBool(is_last)) catch unreachable;
    dst[0] = @truncate(val);
    dst[1] = @truncate(val >> 8);
    dst[2] = @truncate(val >> 16);
}

// =========================================================================
// Decompression (pure Zig via std.compress.zstd)
// =========================================================================

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    if (compressed.len == 0) return error.DecompressionError;

    var out: std.io.Writer.Allocating = std.io.Writer.Allocating.initCapacity(allocator, uncompressed_size) catch return error.OutOfMemory;
    errdefer out.deinit();

    var in: std.io.Reader = .fixed(compressed);
    var zstd_stream: std.compress.zstd.Decompress = .init(&in, &.{}, .{});

    _ = zstd_stream.reader.streamRemaining(&out.writer) catch return error.DecompressionError;

    return out.toOwnedSlice() catch return error.OutOfMemory;
}

// =========================================================================
// Tests
// =========================================================================

test "zstd round-trip" {
    const allocator = std.testing.allocator;
    const original = "Hello, World! This is a test of zstd compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd compress empty data" {
    const allocator = std.testing.allocator;
    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "zstd round-trip repetitive data" {
    const allocator = std.testing.allocator;
    const original = "abcdefgh" ** 1000;

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd round-trip small data" {
    const allocator = std.testing.allocator;
    const original = "abc";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd round-trip multi-block data" {
    const allocator = std.testing.allocator;
    // 256KB > BLOCK_SIZE_MAX (128KB), forces at least 2 blocks
    const original = "ABCDEFGHIJKLMNOP" ** (256 * 1024 / 16);

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd round-trip incompressible data" {
    const allocator = std.testing.allocator;
    // Pseudo-random bytes that defeat LZ matching — exercises raw block fallback
    var buf: [4096]u8 = undefined;
    var x: u32 = 0xDEADBEEF;
    for (&buf) |*b| {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        b.* = @truncate(x);
    }

    const compressed = try compress(allocator, &buf);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, buf.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &buf, decompressed);
}

test "zstd round-trip exactly MIN_MATCH bytes" {
    const allocator = std.testing.allocator;
    const original = "abcd"; // exactly MIN_MATCH (4) bytes

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd decompress ignores mismatched uncompressed_size hint" {
    const allocator = std.testing.allocator;
    const original = "test data";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Wrong size hint is ignored — actual decompressed bytes are returned
    const result = try decompress(allocator, compressed, original.len + 1);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(original, result);
}

test "zstd decompress rejects empty compressed data" {
    const allocator = std.testing.allocator;

    const result = decompress(allocator, "", 100);
    try std.testing.expectError(error.DecompressionError, result);
}

test "zstd rep offsets improve columnar data compression" {
    const allocator = std.testing.allocator;

    // Simulate columnar data: repeating 8-byte pattern where rep[0] should fire
    const original = "abcdefgh" ** 1000;

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Rep offsets should give significantly better compression than raw matches
    // Without rep offsets, each match emits a full offset (8); with rep offsets,
    // subsequent matches use off_base=1 which encodes much smaller.
    try std.testing.expect(compressed.len < original.len / 3);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}
