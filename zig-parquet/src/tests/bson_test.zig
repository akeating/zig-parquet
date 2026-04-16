//! BSON Logical Type Tests
//!
//! Tests for BSON logical type support:
//! - Round-trip write/read with logical type preservation
//! - Schema generation with correct converted_type and logical_type
//! - DynamicReader logical type detection API

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const DynamicReader = @import("../core/dynamic_reader.zig").DynamicReader;
const SchemaNode = @import("../core/schema.zig").SchemaNode;

test "round-trip BSON logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_bson.parquet";

    // Write with BSON logical type
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.bson("data", false),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        // BSON documents (as raw bytes - these are example BSON structures)
        // Simple BSON document: {"x": 1} = 0x0c000000 0x10 "x\0" 0x01000000 0x00
        const bson_doc1 = "\x0c\x00\x00\x00\x10x\x00\x01\x00\x00\x00\x00";
        // Empty BSON document: {} = 0x05000000 0x00
        const bson_doc2 = "\x05\x00\x00\x00\x00";
        // BSON document with string: {"s": "hi"} = 0x10000000 0x02 "s\0" 0x03000000 "hi\0" 0x00
        const bson_doc3 = "\x10\x00\x00\x00\x02s\x00\x03\x00\x00\x00hi\x00\x00";

        const values = [_][]const u8{ bson_doc1, bson_doc2, bson_doc3 };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1]; // First column (schema[0] is root)

        // Verify physical type is BYTE_ARRAY
        try std.testing.expectEqual(format.PhysicalType.byte_array, col_schema.type_.?);

        // Verify logical type is BSON
        try std.testing.expect(col_schema.logical_type != null);
        switch (col_schema.logical_type.?) {
            .bson => {}, // Expected
            else => return error.WrongLogicalType,
        }

        // Verify converted_type is BSON (20) if present
        // Note: converted_type may be null if the reader doesn't populate it
        if (col_schema.converted_type) |ct| {
            try std.testing.expectEqual(@as(i32, format.ConvertedType.BSON), ct);
        }
    }
}

test "BSON schema generation via SchemaNode" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "bson_schema_node.parquet";

    // Verify SchemaNode.bson() creates correct node
    const bson_node = SchemaNode.bson();
    try std.testing.expect(bson_node.byte_array.logical != null);
    try std.testing.expect(bson_node.byte_array.logical.? == .bson);

    // Write using ColumnDef.bson()
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.bson("document", false),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        const values = [_][]const u8{"\x05\x00\x00\x00\x00"}; // Empty BSON doc
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const schema = reader.getSchema();
        var found_bson = false;

        for (schema) |elem| {
            if (std.mem.eql(u8, elem.name, "document")) {
                found_bson = true;
                try std.testing.expect(elem.logical_type != null);
                try std.testing.expect(elem.logical_type.? == .bson);
            }
        }
        try std.testing.expect(found_bson);
    }
}

test "DynamicReader BSON detection via isColumnType" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "dynamic_bson.parquet";

    // Write file with multiple column types including BSON
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.string("text_col", false),
            parquet.ColumnDef.bson("bson_col", false),
            parquet.ColumnDef.json("json_col", false),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        const text_values = [_][]const u8{"hello"};
        const bson_values = [_][]const u8{"\x05\x00\x00\x00\x00"};
        const json_values = [_][]const u8{"{}"};

        try writer.writeColumn([]const u8, 0, &text_values);
        try writer.writeColumn([]const u8, 1, &bson_values);
        try writer.writeColumn([]const u8, 2, &json_values);
        try writer.close();
    }

    // Read with DynamicReader and test logical type detection
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        // Column 0: string
        try std.testing.expect(reader.isColumnType(0, .string));
        try std.testing.expect(!reader.isColumnType(0, .bson));
        try std.testing.expect(!reader.isColumnType(0, .json));

        // Column 1: BSON
        try std.testing.expect(reader.isColumnType(1, .bson));
        try std.testing.expect(!reader.isColumnType(1, .string));
        try std.testing.expect(!reader.isColumnType(1, .json));

        // Column 2: JSON
        try std.testing.expect(reader.isColumnType(2, .json));
        try std.testing.expect(!reader.isColumnType(2, .bson));
        try std.testing.expect(!reader.isColumnType(2, .string));

        // Test getColumnLogicalType
        const bson_type = reader.getColumnLogicalType(1);
        try std.testing.expect(bson_type != null);
        try std.testing.expect(bson_type.? == .bson);

        // Out of bounds returns null
        try std.testing.expect(reader.getColumnLogicalType(100) == null);
    }
}
