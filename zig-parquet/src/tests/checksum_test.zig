//! Page Checksum Tests
//!
//! Tests for CRC32 checksum functionality in Parquet pages.

const std = @import("std");
const parquet_reader = @import("../core/parquet_reader.zig");
const column_writer = @import("../core/column_writer.zig");
const format = @import("../core/format.zig");
const types = @import("../core/types.zig");
const thrift = @import("../core/thrift/mod.zig");

const PageIterator = parquet_reader.PageIterator;
const ChecksumOptions = parquet_reader.ChecksumOptions;
const crc = std.hash.crc;

test "computePageCrc returns i32 checksum" {
    // Verify the CRC32 helper function works correctly
    const data = "hello world";
    const checksum = column_writer.computePageCrc(data);

    // CRC32 IEEE polynomial should give consistent result
    try std.testing.expect(checksum != 0);

    // Same data should give same CRC
    const checksum2 = column_writer.computePageCrc(data);
    try std.testing.expectEqual(checksum, checksum2);

    // Different data should give different CRC
    const checksum3 = column_writer.computePageCrc("goodbye");
    try std.testing.expect(checksum != checksum3);
}

test "CRC32 matches expected value" {
    // Test that our CRC32 implementation matches expected values
    // "123456789" has a well-known CRC32 checksum
    const test_data = "123456789";
    const checksum = crc.Crc32.hash(test_data);

    // CRC32 IEEE polynomial: "123456789" -> 0xCBF43926
    try std.testing.expectEqual(@as(u32, 0xCBF43926), checksum);

    // Verify our i32 bitcast works correctly
    const signed_checksum = column_writer.computePageCrc(test_data);
    try std.testing.expectEqual(@as(i32, @bitCast(@as(u32, 0xCBF43926))), signed_checksum);
}

test "page header CRC field is correctly serialized" {
    const allocator = std.testing.allocator;

    // Create a simple page header with CRC
    const test_crc: i32 = 0x12345678;
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = 100,
        .compressed_page_size = 100,
        .crc = test_crc,
        .data_page_header = .{
            .num_values = 10,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize it
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const serialized = thrift_writer.getWritten();

    // Parse it back
    var thrift_reader = thrift.CompactReader.init(serialized);
    const parsed = try format.PageHeader.parse(allocator, &thrift_reader);
    defer parquet_reader.freePageHeaderContents(allocator, &parsed);

    // Verify CRC was preserved
    try std.testing.expect(parsed.crc != null);
    try std.testing.expectEqual(test_crc, parsed.crc.?);
}

test "page header without CRC serializes correctly" {
    const allocator = std.testing.allocator;

    // Create a page header without CRC
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = 100,
        .compressed_page_size = 100,
        .crc = null, // No CRC
        .data_page_header = .{
            .num_values = 10,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize it
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const serialized = thrift_writer.getWritten();

    // Parse it back
    var thrift_reader = thrift.CompactReader.init(serialized);
    const parsed = try format.PageHeader.parse(allocator, &thrift_reader);
    defer parquet_reader.freePageHeaderContents(allocator, &parsed);

    // Verify CRC is still null
    try std.testing.expectEqual(@as(?i32, null), parsed.crc);
}

test "PageIterator validates checksum - success case" {
    const allocator = std.testing.allocator;

    // Create page data with a valid CRC
    const page_body = "test page data";
    const expected_crc = column_writer.computePageCrc(page_body);

    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page_body.len),
        .compressed_page_size = @intCast(page_body.len),
        .crc = expected_crc,
        .data_page_header = .{
            .num_values = 1,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize header + body
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const header_bytes = thrift_writer.getWritten();

    // Combine header + body into page_data
    var page_data = try allocator.alloc(u8, header_bytes.len + page_body.len);
    defer allocator.free(page_data);
    @memcpy(page_data[0..header_bytes.len], header_bytes);
    @memcpy(page_data[header_bytes.len..], page_body);

    // Read with checksum validation
    var page_iter = PageIterator.initWithOptions(allocator, page_data, .{
        .validate_page_checksum = true,
        .strict_checksum = false,
    });

    const page_info = try page_iter.next();
    try std.testing.expect(page_info != null);

    const page = page_info.?;
    defer parquet_reader.freePageHeaderContents(allocator, &page.header);

    try std.testing.expect(page.header.crc != null);
    try std.testing.expectEqual(expected_crc, page.header.crc.?);
}

test "PageIterator validates checksum - corruption detected" {
    const allocator = std.testing.allocator;

    // Create page data with an INCORRECT CRC
    const page_body = "test page data";
    const wrong_crc: i32 = @bitCast(@as(u32, 0xDEADBEEF)); // Wrong CRC

    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page_body.len),
        .compressed_page_size = @intCast(page_body.len),
        .crc = wrong_crc,
        .data_page_header = .{
            .num_values = 1,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize header + body
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const header_bytes = thrift_writer.getWritten();

    // Combine header + body into page_data
    var page_data = try allocator.alloc(u8, header_bytes.len + page_body.len);
    defer allocator.free(page_data);
    @memcpy(page_data[0..header_bytes.len], header_bytes);
    @memcpy(page_data[header_bytes.len..], page_body);

    // Read with checksum validation - should fail
    var page_iter = PageIterator.initWithOptions(allocator, page_data, .{
        .validate_page_checksum = true,
        .strict_checksum = false,
    });

    const result_or_error = page_iter.next();
    try std.testing.expectError(error.PageChecksumMismatch, result_or_error);
}

test "PageIterator strict mode rejects missing CRC" {
    const allocator = std.testing.allocator;

    // Create page data WITHOUT CRC
    const page_body = "test page data";

    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page_body.len),
        .compressed_page_size = @intCast(page_body.len),
        .crc = null, // No CRC
        .data_page_header = .{
            .num_values = 1,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize header + body
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const header_bytes = thrift_writer.getWritten();

    // Combine header + body into page_data
    var page_data = try allocator.alloc(u8, header_bytes.len + page_body.len);
    defer allocator.free(page_data);
    @memcpy(page_data[0..header_bytes.len], header_bytes);
    @memcpy(page_data[header_bytes.len..], page_body);

    // Read with strict checksum mode - should fail
    var page_iter = PageIterator.initWithOptions(allocator, page_data, .{
        .validate_page_checksum = true,
        .strict_checksum = true, // Strict mode
    });

    const result_or_error = page_iter.next();
    try std.testing.expectError(error.MissingPageChecksum, result_or_error);
}

test "PageIterator non-strict mode allows missing CRC" {
    const allocator = std.testing.allocator;

    // Create page data WITHOUT CRC
    const page_body = "test page data";

    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page_body.len),
        .compressed_page_size = @intCast(page_body.len),
        .crc = null, // No CRC
        .data_page_header = .{
            .num_values = 1,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize header + body
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const header_bytes = thrift_writer.getWritten();

    // Combine header + body into page_data
    var page_data = try allocator.alloc(u8, header_bytes.len + page_body.len);
    defer allocator.free(page_data);
    @memcpy(page_data[0..header_bytes.len], header_bytes);
    @memcpy(page_data[header_bytes.len..], page_body);

    // Read with validation enabled but NOT strict - should succeed
    var page_iter = PageIterator.initWithOptions(allocator, page_data, .{
        .validate_page_checksum = true,
        .strict_checksum = false, // Non-strict
    });

    const page_info = try page_iter.next();
    try std.testing.expect(page_info != null);

    const page = page_info.?;
    defer parquet_reader.freePageHeaderContents(allocator, &page.header);

    // CRC should still be null
    try std.testing.expectEqual(@as(?i32, null), page.header.crc);
}

test "PageIterator validation disabled ignores CRC" {
    const allocator = std.testing.allocator;

    // Create page data with WRONG CRC - but validation disabled
    const page_body = "test page data";
    const wrong_crc: i32 = 0x0BADCAFE; // Wrong CRC (fits in i32)

    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page_body.len),
        .compressed_page_size = @intCast(page_body.len),
        .crc = wrong_crc,
        .data_page_header = .{
            .num_values = 1,
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize header + body
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try page_header.serialize(&thrift_writer);
    const header_bytes = thrift_writer.getWritten();

    // Combine header + body into page_data
    var page_data = try allocator.alloc(u8, header_bytes.len + page_body.len);
    defer allocator.free(page_data);
    @memcpy(page_data[0..header_bytes.len], header_bytes);
    @memcpy(page_data[header_bytes.len..], page_body);

    // Read with validation DISABLED - should succeed even with wrong CRC
    var page_iter = PageIterator.init(allocator, page_data);

    const page_info = try page_iter.next();
    try std.testing.expect(page_info != null);

    const page = page_info.?;
    defer parquet_reader.freePageHeaderContents(allocator, &page.header);

    // CRC should still be present (but not validated)
    try std.testing.expect(page.header.crc != null);
}
