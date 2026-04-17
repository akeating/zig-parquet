//! Gzip compression/decompression for Parquet
//!
//! Compression uses dynamic Huffman deflate blocks (RFC 1951, level 9) wrapped in gzip container (RFC 1952).
//! Level-9 features: lazy LZ77 matching with hash chains, dynamic Huffman trees, bit-packed encoding.
//! Decompression uses std.compress.flate.

const std = @import("std");
const safe = @import("../safe.zig");

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};


// Gzip constants (RFC 1952)
const GZIP_MAGIC: u16 = 0x1f8b;
const GZIP_METHOD: u8 = 8; // deflate
const GZIP_MTIME: u32 = 0;
const GZIP_XFL: u8 = 0;
const GZIP_OS: u8 = 255; // unknown

// CRC-32 polynomial (Ethernet standard)
const CRC32Table = crc32_table_init();

fn crc32_table_init() [256]u32 {
    @setEvalBranchQuota(3000);
    var table: [256]u32 = undefined;
    for (0..256) |i| {
        var crc: u32 = @intCast(i);
        for (0..8) |_| {
            if ((crc & 1) != 0) {
                crc = (crc >> 1) ^ 0xedb88320;
            } else {
                crc = crc >> 1;
            }
        }
        table[i] = crc;
    }
    return table;
}

fn crc32Update(crc: u32, data: []const u8) u32 {
    var result = crc;
    for (data) |byte| {
        result = (result >> 8) ^ CRC32Table[(result ^ byte) & 0xff];
    }
    return result;
}

// =========================================================================
// Deflate Constants and Structures
// =========================================================================

const WBITS: u5 = 15;
const WINDOW_SIZE: usize = 1 << WBITS;
const WMASK: usize = WINDOW_SIZE - 1;
const HASH_BITS: u5 = 15;
const HASH_SIZE: usize = 1 << HASH_BITS;
const H_SHIFT: u4 = 5;
const MIN_MATCH: usize = 3;
const MAX_MATCH: usize = 258;
const MAX_DIST: usize = WINDOW_SIZE - MAX_MATCH - 1;

const LITERALS: usize = 256;
const LENGTH_CODES: usize = 29;
const DISTANCES: usize = 30;
const LIT_LEN_CODES: usize = LITERALS + 1 + LENGTH_CODES;
const END_BLOCK: usize = 256;

const MAX_BL_BITS: u5 = 7;
const BL_CODES: usize = 19;

const BitWriter = struct {
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    container: u64 = 0,
    bit_pos: u6 = 0,

    fn writeBits(self: *BitWriter, bits: u32, nbits: u5) !void {
        if (nbits == 0) return;

        self.container |= @as(u64, @intCast(bits)) << @intCast(self.bit_pos);
        self.bit_pos += nbits;

        while (self.bit_pos >= 8) {
            const byte: u8 = @truncate(self.container);
            try self.buf.append(self.allocator, byte);
            self.container >>= 8;
            self.bit_pos -= 8;
        }
    }

    fn byteAlign(self: *BitWriter) !void {
        if (self.bit_pos > 0) {
            try self.writeBits(0, 8 - self.bit_pos);
        }
    }

    fn flush(self: *BitWriter) !void {
        if (self.bit_pos > 0) {
            const byte: u8 = @truncate(self.container);
            try self.buf.append(self.allocator, byte);
            self.container = 0;
            self.bit_pos = 0;
        }
    }
};

const Token = union(enum) {
    literal: u8,
    match: struct {
        length: u16,
        distance: u16,
    },
};

// Length code lookup (match lengths 3-258 → codes 257-285)
fn lengthCode(len: u16) u9 {
    return if (len < 11)
        257 + @as(u9, @intCast(len - 3))
    else if (len < 19)
        265 + @as(u9, @intCast((len - 11) >> 1))
    else if (len < 35)
        269 + @as(u9, @intCast((len - 19) >> 2))
    else if (len < 67)
        273 + @as(u9, @intCast((len - 35) >> 3))
    else if (len < 131)
        277 + @as(u9, @intCast((len - 67) >> 4))
    else if (len < 258)
        281 + @as(u9, @intCast((len - 131) >> 5))
    else
        285;
}

fn lengthBase(code: u9) u16 {
    // RFC 1951 §3.2.5: codes 257-264 have 0 extra bits (bases 3-10)
    return switch (code) {
        257...264 => 3 + @as(u16, code - 257),
        265...268 => 11 + @as(u16, (code - 265) * 2),
        269...272 => 19 + @as(u16, (code - 269) * 4),
        273...276 => 35 + @as(u16, (code - 273) * 8),
        277...280 => 67 + @as(u16, (code - 277) * 16),
        281...284 => 131 + @as(u16, (code - 281) * 32),
        285 => 258,
        else => 0,
    };
}

fn lengthExtra(code: u9) u5 {
    // RFC 1951 §3.2.5: codes 257-264 have 0 extra bits
    return switch (code) {
        257...264 => 0,
        265...268 => 1,
        269...272 => 2,
        273...276 => 3,
        277...280 => 4,
        281...284 => 5,
        285 => 0,
        else => 0,
    };
}

// Distance code lookup (distances 1-32768 → codes 0-29)
fn distanceCode(dist: u16) u8 {
    return if (dist < 5)
        @as(u8, @intCast(dist - 1))
    else if (dist < 9)
        4 + @as(u8, @intCast((dist - 5) >> 1))
    else if (dist < 17)
        6 + @as(u8, @intCast((dist - 9) >> 2))
    else if (dist < 33)
        8 + @as(u8, @intCast((dist - 17) >> 3))
    else if (dist < 65)
        10 + @as(u8, @intCast((dist - 33) >> 4))
    else if (dist < 129)
        12 + @as(u8, @intCast((dist - 65) >> 5))
    else if (dist < 257)
        14 + @as(u8, @intCast((dist - 129) >> 6))
    else if (dist < 513)
        16 + @as(u8, @intCast((dist - 257) >> 7))
    else if (dist < 1025)
        18 + @as(u8, @intCast((dist - 513) >> 8))
    else if (dist < 2049)
        20 + @as(u8, @intCast((dist - 1025) >> 9))
    else if (dist < 4097)
        22 + @as(u8, @intCast((dist - 2049) >> 10))
    else if (dist < 8193)
        24 + @as(u8, @intCast((dist - 4097) >> 11))
    else if (dist < 16385)
        26 + @as(u8, @intCast((dist - 8193) >> 12))
    else
        28 + @as(u8, @intCast((dist - 16385) >> 13));
}

fn distanceBase(code: u8) u16 {
    return switch (code) {
        0...3 => 1 + @as(u16, code),
        4...5 => 5 + (@as(u16, code - 4) * 2),
        6...7 => 9 + (@as(u16, code - 6) * 4),
        8...9 => 17 + (@as(u16, code - 8) * 8),
        10...11 => 33 + (@as(u16, code - 10) * 16),
        12...13 => 65 + (@as(u16, code - 12) * 32),
        14...15 => 129 + (@as(u16, code - 14) * 64),
        16...17 => 257 + (@as(u16, code - 16) * 128),
        18...19 => 513 + (@as(u16, code - 18) * 256),
        20...21 => 1025 + (@as(u16, code - 20) * 512),
        22...23 => 2049 + (@as(u16, code - 22) * 1024),
        24...25 => 4097 + (@as(u16, code - 24) * 2048),
        26...27 => 8193 + (@as(u16, code - 26) * 4096),
        28...29 => 16385 + (@as(u16, code - 28) * 8192),
        else => 0,
    };
}

fn distanceExtra(code: u8) u5 {
    return switch (code) {
        0...3 => 0,
        4...5 => 1,
        6...7 => 2,
        8...9 => 3,
        10...11 => 4,
        12...13 => 5,
        14...15 => 6,
        16...17 => 7,
        18...19 => 8,
        20...21 => 9,
        22...23 => 10,
        24...25 => 11,
        26...27 => 12,
        28...29 => 13,
        else => 0,
    };
}

// =========================================================================
// Huffman Tree Builder
// =========================================================================

const HuffmanNode = struct {
    freq: u32,
    symbol: usize = 0,
    left: ?usize = null,
    right: ?usize = null,
};

const HuffmanTree = struct {
    freq: std.ArrayList(u32),
    code: std.ArrayList(u32),
    len: std.ArrayList(u5),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, max_symbols: usize) !HuffmanTree {
        var tree: HuffmanTree = .{
            .freq = .empty,
            .code = .empty,
            .len = .empty,
            .allocator = allocator,
        };
        try tree.freq.resize(allocator, max_symbols);
        try tree.code.resize(allocator, max_symbols);
        try tree.len.resize(allocator, max_symbols);
        @memset(tree.freq.items, 0);
        @memset(tree.len.items, 0);
        return tree;
    }

    fn deinit(self: *HuffmanTree) void {
        self.freq.deinit(self.allocator);
        self.code.deinit(self.allocator);
        self.len.deinit(self.allocator);
    }

    fn findMinPos(nodes: []const HuffmanNode, heap: []const usize) usize {
        var min_pos: usize = 0;
        for (1..heap.len) |i| {
            if (nodes[heap[i]].freq < nodes[heap[min_pos]].freq) {
                min_pos = i;
            }
        }
        return min_pos;
    }

    fn build(self: *HuffmanTree, max_bits: u5) !void {
        var nodes: std.ArrayList(HuffmanNode) = .empty;
        defer nodes.deinit(self.allocator);

        // Create leaf nodes for non-zero frequencies
        var heap: std.ArrayList(usize) = .empty;
        defer heap.deinit(self.allocator);

        for (self.freq.items, 0..) |f, i| {
            if (f > 0) {
                try nodes.append(self.allocator, .{ .freq = f, .symbol = i });
                try heap.append(self.allocator, nodes.items.len - 1);
            }
        }

        // Ensure at least 2 nodes (pkzip requirement)
        const max_sym = self.len.items.len;
        if (heap.items.len == 0) {
            try nodes.append(self.allocator, .{ .freq = 1, .symbol = max_sym });
            try heap.append(self.allocator, 0);
        }
        if (heap.items.len == 1) {
            try nodes.append(self.allocator, .{ .freq = 1, .symbol = max_sym });
            try heap.append(self.allocator, nodes.items.len - 1);
        }

        // Build tree via min-heap merging (correct heap extraction)
        while (heap.items.len > 1) {
            // Extract minimum 1
            const p1 = findMinPos(nodes.items, heap.items);
            const left = heap.items[p1];
            heap.items[p1] = heap.items[heap.items.len - 1];
            _ = heap.pop();

            // Extract minimum 2 (heap is now 1 shorter)
            const p2 = findMinPos(nodes.items, heap.items);
            const right = heap.items[p2];
            heap.items[p2] = heap.items[heap.items.len - 1];
            _ = heap.pop();

            // Merge and push parent
            try nodes.append(self.allocator, .{
                .freq = nodes.items[left].freq +| nodes.items[right].freq,
                .left = left,
                .right = right,
            });
            try heap.append(self.allocator, nodes.items.len - 1);
        }

        // Assign code lengths by tree traversal
        if (heap.items.len > 0) {
            var len_count: std.ArrayList(u32) = .empty;
            defer len_count.deinit(self.allocator);
            try len_count.resize(self.allocator, max_bits + 1);
            @memset(len_count.items, 0);

            try self.assignLengths(&nodes, heap.items[0], 0, max_bits, &len_count);
        }
    }

    fn assignLengths(self: *HuffmanTree, nodes: *std.ArrayList(HuffmanNode), node_idx: usize, depth: u32, max_bits: u5, len_count: *std.ArrayList(u32)) !void {
        const node = nodes.items[node_idx];

        if (node.left == null and node.right == null) {
            // Leaf node (symbol)
            const sym_idx = node.symbol;
            if (sym_idx < self.len.items.len and self.freq.items[sym_idx] > 0) {
                self.len.items[sym_idx] = @intCast(@min(depth, max_bits));
                if (depth <= max_bits) {
                    len_count.items[@intCast(depth)] += 1;
                }
            }
        } else {
            if (node.left) |left_idx| {
                try self.assignLengths(nodes, left_idx, depth + 1, max_bits, len_count);
            }
            if (node.right) |right_idx| {
                try self.assignLengths(nodes, right_idx, depth + 1, max_bits, len_count);
            }
        }
    }

    fn genCodes(self: *HuffmanTree) void {
        // RFC 1951 §3.2.2 canonical Huffman code assignment
        // Step 1: count symbols per bit-length
        var bl_count = [_]u32{0} ** 16;
        for (self.len.items) |len| {
            if (len > 0) bl_count[len] += 1;
        }

        // Step 2: compute starting code for each bit-length
        var next_code = [_]u32{0} ** 16;
        var code: u32 = 0;
        for (1..16) |bits| {
            code = (code + bl_count[bits - 1]) << 1;
            next_code[bits] = code;
        }

        // Step 3: assign canonical codes, bit-reversed for LSB-first deflate
        for (self.len.items, 0..) |len, i| {
            if (len > 0) {
                self.code.items[i] = bitReverse(next_code[len], len);
                next_code[len] += 1;
            } else {
                self.code.items[i] = 0;
            }
        }
    }


    fn bitReverse(val: u32, nbits: u5) u32 {
        var result: u32 = 0;
        var v = val;
        var n = nbits;
        while (n > 0) : (n -= 1) {
            result = (result << 1) | (v & 1);
            v >>= 1;
        }
        return result;
    }
};

// =========================================================================
// LZ77 Lazy Matcher
// =========================================================================

fn updateHash(h: u32, c: u8) u32 {
    return (((h << H_SHIFT) ^ @as(u32, c)) & @as(u32, HASH_SIZE - 1));
}

const Match = struct {
    length: u32,
    distance: u32,
};

const Deflater = struct {
    window: [2 * WINDOW_SIZE]u8,
    head: [HASH_SIZE]u16,
    prev: [WINDOW_SIZE]u16,
    ins_h: u32 = 0,
    strstart: u32 = 0,
    window_end: u32 = 0,
    prev_length: u32 = 0,
    prev_match: u32 = 0,
    match_available: bool = false,

    // Heap-allocated: the struct is ~192 KiB (window + head + prev), too big for the stack.
    fn create(allocator: std.mem.Allocator) !*Deflater {
        const d = try allocator.create(Deflater);
        d.* = .{
            .window = undefined,
            .head = [_]u16{0} ** HASH_SIZE,
            .prev = [_]u16{0} ** WINDOW_SIZE,
        };
        return d;
    }

    fn destroy(d: *Deflater, allocator: std.mem.Allocator) void {
        allocator.destroy(d);
    }

    // Copy as much remaining input as fits into the free space above window_end.
    fn fill(d: *Deflater, data: []const u8, data_pos: *usize) void {
        const space: usize = 2 * WINDOW_SIZE - d.window_end;
        const n = @min(space, data.len - data_pos.*);
        if (n == 0) return;
        @memcpy(d.window[d.window_end .. d.window_end + n], data[data_pos.* .. data_pos.* + n]);
        d.window_end += @intCast(n);
        data_pos.* += n;
    }

    // Drop the lower WINDOW_SIZE bytes and shift the upper half down to make room for more input.
    // Hash chains retarget to the new positions; chain entries that pointed into the dropped
    // half are zeroed (0 means "no prior match").
    fn slide(d: *Deflater) void {
        const w: u16 = @intCast(WINDOW_SIZE);
        @memcpy(d.window[0..WINDOW_SIZE], d.window[WINDOW_SIZE .. 2 * WINDOW_SIZE]);
        for (&d.head) |*h| h.* = if (h.* >= w) h.* - w else 0;
        for (&d.prev) |*p| p.* = if (p.* >= w) p.* - w else 0;
        d.strstart -= @intCast(WINDOW_SIZE);
        d.window_end -= @intCast(WINDOW_SIZE);
    }

    fn insertString(d: *Deflater, s: u32) u16 {
        if (s + MIN_MATCH > d.window_end) return 0;
        d.ins_h = updateHash(d.ins_h, d.window[s + MIN_MATCH - 1]);
        const h = d.ins_h;
        const head = d.head[h];
        d.prev[s & WMASK] = head;
        d.head[h] = @intCast(s);
        return head;
    }

    fn longestMatch(d: *Deflater, cur_match: u16, max_chain: u16, lookahead: u32) Match {
        var best_len: u32 = MIN_MATCH - 1;
        var best_dist: u32 = 0;
        var chain_len: u16 = max_chain;
        var match_idx = cur_match;
        const max_len = @min(@as(u32, MAX_MATCH), lookahead);

        if (max_len < MIN_MATCH) return .{ .length = 0, .distance = 0 };

        while (chain_len > 0 and match_idx > 0) {
            const dist = d.strstart - match_idx;
            if (dist > MAX_DIST) break;

            // Quick length check (last byte of current best)
            if (best_len < max_len and d.window[match_idx + best_len] != d.window[d.strstart + best_len]) {
                chain_len -= 1;
                match_idx = d.prev[match_idx & WMASK];
                continue;
            }

            // Full match check (bounded to remaining data)
            var len: u32 = 0;
            while (len < max_len and d.window[match_idx + len] == d.window[d.strstart + len]) {
                len += 1;
            }

            if (len > best_len) {
                best_len = len;
                best_dist = dist;

                if (len >= 32 or len == max_len) break; // good_length or max reached
            }

            chain_len -= 1;
            match_idx = d.prev[match_idx & WMASK];
        }

        return .{ .length = best_len, .distance = best_dist };
    }

    fn tokenize(d: *Deflater, allocator: std.mem.Allocator, data: []const u8) !std.ArrayList(Token) {
        var tokens: std.ArrayList(Token) = .empty;
        errdefer tokens.deinit(allocator);

        const max_chain: u16 = 4096;
        d.prev_length = MIN_MATCH - 1;
        d.prev_match = 0;
        d.match_available = false;

        var data_pos: usize = 0;
        d.fill(data, &data_pos);

        while (true) {
            // Slide when strstart has moved past the lower half and there's still input to fetch.
            // Guard with strstart > WINDOW_SIZE (not >=) so the pending match_available literal
            // at strstart-1 stays in the preserved upper half across the slide.
            if (d.strstart > WINDOW_SIZE and data_pos < data.len and d.window_end == 2 * WINDOW_SIZE) {
                d.slide();
                d.fill(data, &data_pos);
            }
            if (d.strstart >= d.window_end) break;

            const hash_head = d.insertString(d.strstart);
            var len: u32 = MIN_MATCH - 1;
            var match_dist: u32 = 0;

            const lookahead = d.window_end - d.strstart;
            if (hash_head > 0 and d.strstart > hash_head and d.strstart - hash_head <= MAX_DIST and lookahead >= MIN_MATCH) {
                const m = d.longestMatch(hash_head, max_chain, lookahead);
                len = m.length;
                match_dist = m.distance;
            }

            // Lazy evaluation
            if (d.prev_length >= MIN_MATCH and len <= d.prev_length) {
                try tokens.append(allocator, .{ .match = .{
                    .length = @intCast(d.prev_length),
                    .distance = @intCast(d.prev_match),
                } });

                // Insert intermediate strings (match starts at strstart-1, covers prev_length bytes)
                var i: u32 = 2;
                while (i < d.prev_length) : (i += 1) {
                    d.strstart += 1;
                    if (d.strstart < d.window_end) {
                        _ = d.insertString(d.strstart);
                    }
                }
                d.match_available = false;
                d.prev_length = MIN_MATCH - 1;
            } else if (d.match_available) {
                try tokens.append(allocator, .{ .literal = d.window[d.strstart - 1] });
                d.prev_length = len;
                if (len >= MIN_MATCH) {
                    d.prev_match = match_dist;
                }
            } else {
                d.match_available = true;
                d.prev_length = len;
                if (len >= MIN_MATCH) {
                    d.prev_match = match_dist;
                }
            }

            d.strstart += 1;
        }

        // Flush held literal
        if (d.match_available and d.strstart > 0) {
            try tokens.append(allocator, .{ .literal = d.window[d.strstart - 1] });
        }

        return tokens;
    }
};

// =========================================================================
// RLE Tree Descriptor Encoder
// =========================================================================

const BL_ORDER = [19]u8{ 16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15 };

fn sendAllTrees(
    allocator: std.mem.Allocator,
    bw: *BitWriter,
    lit_tree: *HuffmanTree,
    dist_tree: *HuffmanTree,
    nlit: usize,
    ndist: usize,
) !void {
    // 1. Collect all bit-lengths to RLE-encode
    var lens = std.ArrayList(u8).empty;
    defer lens.deinit(allocator);
    for (lit_tree.len.items[0..nlit]) |l| try lens.append(allocator, l);
    for (dist_tree.len.items[0..ndist]) |l| try lens.append(allocator, l);

    // 2. RLE encode into (code, extra_bits, extra_val) triples
    const RleEntry = struct { code: u8, extra_bits: u5, extra_val: u32 };
    var rle = std.ArrayList(RleEntry).empty;
    defer rle.deinit(allocator);

    var i: usize = 0;
    while (i < lens.items.len) {
        const cur = lens.items[i];
        // Count run length
        var run: usize = 1;
        while (i + run < lens.items.len and lens.items[i + run] == cur and run < 138) {
            run += 1;
        }
        if (cur == 0) {
            // Zero run: use code 17 (3-10) or 18 (11-138)
            var j: usize = 0;
            while (j < run) {
                if (run - j >= 11) {
                    const n = @min(run - j, 138);
                    try rle.append(allocator, .{ .code = 18, .extra_bits = 7, .extra_val = @intCast(n - 11) });
                    j += n;
                } else if (run - j >= 3) {
                    const n = @min(run - j, 10);
                    try rle.append(allocator, .{ .code = 17, .extra_bits = 3, .extra_val = @intCast(n - 3) });
                    j += n;
                } else {
                    try rle.append(allocator, .{ .code = 0, .extra_bits = 0, .extra_val = 0 });
                    j += 1;
                }
            }
        } else {
            // Non-zero: emit first literally, then code 16 for repeats
            try rle.append(allocator, .{ .code = cur, .extra_bits = 0, .extra_val = 0 });
            var j: usize = 1;
            while (j < run) {
                const n = @min(run - j, 6);
                if (n >= 3) {
                    try rle.append(allocator, .{ .code = 16, .extra_bits = 2, .extra_val = @intCast(n - 3) });
                    j += n;
                } else {
                    try rle.append(allocator, .{ .code = cur, .extra_bits = 0, .extra_val = 0 });
                    j += 1;
                }
            }
        }
        i += run;
    }

    // 3. Count CL symbol frequencies and build bl_tree
    var bl_tree = try HuffmanTree.init(allocator, BL_CODES);
    defer bl_tree.deinit();
    for (rle.items) |e| bl_tree.freq.items[e.code] += 1;
    try bl_tree.build(MAX_BL_BITS);
    bl_tree.genCodes();

    // 4. Determine HCLEN: last non-zero in BL_ORDER (minimum 4)
    var nblcodes: usize = 4;
    var k: usize = BL_CODES;
    while (k > 4) : (k -= 1) {
        if (bl_tree.len.items[BL_ORDER[k - 1]] > 0) {
            nblcodes = k;
            break;
        }
    }

    // 5. Write HCLEN and the CL alphabet lengths
    try bw.writeBits(@intCast(nblcodes - 4), 4);
    for (BL_ORDER[0..nblcodes]) |sym| {
        try bw.writeBits(bl_tree.len.items[sym], 3);
    }

    // 6. Write RLE-encoded bit-length sequences
    for (rle.items) |e| {
        const code_val = bl_tree.code.items[e.code];
        const code_bits = bl_tree.len.items[e.code];
        try bw.writeBits(code_val, code_bits);
        if (e.extra_bits > 0) {
            try bw.writeBits(e.extra_val, e.extra_bits);
        }
    }
}

// =========================================================================
// Block Encoder
// =========================================================================

fn encodeBlock(allocator: std.mem.Allocator, bw: *BitWriter, tokens: []const Token, is_final: bool) !void {
    // Count frequencies
    var lit_freq: std.ArrayList(u32) = .empty;
    defer lit_freq.deinit(allocator);
    try lit_freq.resize(allocator, LIT_LEN_CODES);
    @memset(lit_freq.items, 0);

    var dist_freq: std.ArrayList(u32) = .empty;
    defer dist_freq.deinit(allocator);
    try dist_freq.resize(allocator, DISTANCES);
    @memset(dist_freq.items, 0);

    for (tokens) |token| {
        switch (token) {
            .literal => |lit| {
                lit_freq.items[lit] += 1;
            },
            .match => |m| {
                const len_code = lengthCode(m.length);
                const dist_code = distanceCode(m.distance);
                lit_freq.items[len_code] += 1;
                dist_freq.items[dist_code] += 1;
            },
        }
    }

    // Ensure at least one code
    lit_freq.items[END_BLOCK] += 1;

    // Build trees
    var lit_tree = try HuffmanTree.init(allocator, LIT_LEN_CODES);
    defer lit_tree.deinit();
    @memcpy(lit_tree.freq.items, lit_freq.items);
    try lit_tree.build(15);
    lit_tree.genCodes();

    var dist_tree = try HuffmanTree.init(allocator, DISTANCES);
    defer dist_tree.deinit();
    @memcpy(dist_tree.freq.items, dist_freq.items);
    // Guard: ensure at least one distance symbol (RFC 1951 requires HDIST >= 0, so >= 1 code)
    var dist_sum: u32 = 0;
    for (dist_freq.items) |f| dist_sum += f;
    if (dist_sum == 0) {
        dist_tree.freq.items[0] = 1;
    }
    try dist_tree.build(15);
    dist_tree.genCodes();

    // Compute nlit and ndist: highest used code indices (minimum required counts)
    var nlit: usize = 257;
    {
        var j: usize = LIT_LEN_CODES;
        while (j > 257) : (j -= 1) {
            if (lit_tree.len.items[j - 1] > 0) {
                nlit = j;
                break;
            }
        }
    }

    var ndist: usize = 1;
    {
        var j: usize = DISTANCES;
        while (j > 1) : (j -= 1) {
            if (dist_tree.len.items[j - 1] > 0) {
                ndist = j;
                break;
            }
        }
    }

    // Write block header
    try bw.writeBits(if (is_final) 1 else 0, 1); // BFINAL
    try bw.writeBits(2, 2); // BTYPE = 10 (dynamic)
    try bw.writeBits(@intCast(nlit - 257), 5); // HLIT
    try bw.writeBits(@intCast(ndist - 1), 5); // HDIST

    // Send Huffman tree descriptors with RLE encoding
    try sendAllTrees(allocator, bw, &lit_tree, &dist_tree, nlit, ndist);

    // Encode tokens
    for (tokens) |token| {
        switch (token) {
            .literal => |lit| {
                const code = lit_tree.code.items[lit];
                const len = lit_tree.len.items[lit];
                try bw.writeBits(code, len);
            },
            .match => |m| {
                const len_code = lengthCode(m.length);
                const len_code_val = lit_tree.code.items[len_code];
                const len_code_bits = lit_tree.len.items[len_code];
                try bw.writeBits(len_code_val, len_code_bits);

                const extra_len_bits = lengthExtra(len_code);
                if (extra_len_bits > 0) {
                    const extra_len = m.length -| lengthBase(len_code);
                    try bw.writeBits(@intCast(extra_len), extra_len_bits);
                }

                const dist_code = distanceCode(m.distance);
                const dist_code_val = dist_tree.code.items[dist_code];
                const dist_code_bits = dist_tree.len.items[dist_code];
                try bw.writeBits(dist_code_val, dist_code_bits);

                const extra_dist_bits = distanceExtra(dist_code);
                if (extra_dist_bits > 0) {
                    const extra_dist = m.distance - distanceBase(dist_code);
                    try bw.writeBits(@intCast(extra_dist), extra_dist_bits);
                }
            },
        }
    }

    // End of block
    const eob_code = lit_tree.code.items[END_BLOCK];
    const eob_bits = lit_tree.len.items[END_BLOCK];
    try bw.writeBits(eob_code, eob_bits);
}

fn countNonZero(items: []const u5) u32 {
    var count: u32 = 0;
    for (items) |len| {
        if (len > 0) count += 1;
    }
    return count;
}

// =========================================================================
// Public API
// =========================================================================

pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    // Write gzip header
    try out.appendSlice(allocator, &[_]u8{ 0x1f, 0x8b }); // magic
    try out.append(allocator, GZIP_METHOD);
    try out.append(allocator, 0); // flags
    try out.appendSlice(allocator, &std.mem.toBytes(GZIP_MTIME));
    try out.append(allocator, GZIP_XFL);
    try out.append(allocator, GZIP_OS);

    // Compress using level-9 deflate (sliding-window LZ77, single final dynamic-Huffman block).
    const deflater = try Deflater.create(allocator);
    defer deflater.destroy(allocator);
    var tokens = try deflater.tokenize(allocator, data);
    defer tokens.deinit(allocator);

    // Encode tokens to bitstream
    var bw = BitWriter{ .buf = &out, .allocator = allocator };
    try encodeBlock(allocator, &bw, tokens.items, true);
    try bw.flush();

    // Compute CRC-32 of uncompressed data
    var crc: u32 = 0xffffffff;
    crc = crc32Update(crc, data);
    crc ^= 0xffffffff;

    // Write gzip footer
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u32, crc)));
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u32, @as(u32, @intCast(data.len)))));

    return out.toOwnedSlice(allocator);
}

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {

    var out: std.Io.Writer.Allocating = std.Io.Writer.Allocating.initCapacity(allocator, uncompressed_size) catch return error.OutOfMemory;
    errdefer out.deinit();

    // Handle concatenated gzip members (RFC 1952 §2.2)
    // Use .gzip container mode so std.compress.flate handles header/footer parsing.
    var in: std.Io.Reader = .fixed(compressed);

    while (true) {
        // Check if there's another gzip member (need at least magic + method = 3 bytes)
        if (in.seek >= in.end) break;
        const remaining = compressed[in.seek..];
        if (remaining.len < 10) break;
        if (remaining[0] != 0x1f or remaining[1] != 0x8b) break;

        // Decompress one gzip member (header + deflate + footer handled by .gzip mode)
        var gz: std.compress.flate.Decompress = .init(&in, .gzip, &[_]u8{});
        _ = gz.reader.streamRemaining(&out.writer) catch return error.DecompressionError;
    }

    return out.toOwnedSlice() catch return error.OutOfMemory;
}

// =========================================================================
// Tests
// =========================================================================

test "gzip round-trip" {
    const allocator = std.testing.allocator;
    const original = "Hello, World! This is a test of gzip compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "gzip empty data" {
    const allocator = std.testing.allocator;
    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "gzip magic bytes" {
    const allocator = std.testing.allocator;
    const original = "test data";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len >= 2);
    try std.testing.expectEqual(@as(u8, 0x1f), compressed[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), compressed[1]);
}

test "gzip large data" {
    const allocator = std.testing.allocator;
    var buf: [10000]u8 = undefined;
    for (0..buf.len) |i| {
        buf[i] = @intCast(i % 256);
    }

    const compressed = try compress(allocator, &buf);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, buf.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &buf, decompressed);
}

test "gzip concatenated streams" {
    const allocator = std.testing.allocator;
    const part1 = "Hello, World!";
    const part2 = " This is part two.";

    const compressed1 = try compress(allocator, part1);
    defer allocator.free(compressed1);
    const compressed2 = try compress(allocator, part2);
    defer allocator.free(compressed2);

    // Concatenate two gzip streams
    const concatenated = try allocator.alloc(u8, compressed1.len + compressed2.len);
    defer allocator.free(concatenated);
    @memcpy(concatenated[0..compressed1.len], compressed1);
    @memcpy(concatenated[compressed1.len..], compressed2);

    const total_len = part1.len + part2.len;
    const decompressed = try decompress(allocator, concatenated, total_len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, part1 ++ part2, decompressed);
}

test "gzip decompress ignores oversized size hint" {
    const allocator = std.testing.allocator;
    const original = "Hello, World!";
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);
    const result = try decompress(allocator, compressed, original.len + 100);
    defer allocator.free(result);
    try std.testing.expectEqualSlices(u8, original, result);
}

