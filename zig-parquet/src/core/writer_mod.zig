//! Parquet Writer Components
//!
//! This module contains the building blocks for writing Parquet files.

pub const page_writer = @import("page_writer.zig");
pub const column_writer = @import("column_writer.zig");
pub const column_write_list = @import("column_write_list.zig");
pub const column_write_struct = @import("column_write_struct.zig");
pub const column_write_map = @import("column_write_map.zig");
pub const list_encoder = @import("list_encoder.zig");
pub const map_encoder = @import("map_encoder.zig");
pub const row_writer = @import("row_writer.zig");
pub const column_def = @import("column_def.zig");
pub const statistics = @import("statistics.zig");
pub const write_target = @import("write_target.zig");

// Re-export commonly used types
pub const FlattenedList = list_encoder.FlattenedList;
pub const flattenList = list_encoder.flattenList;
pub const FlattenedMap = map_encoder.FlattenedMap;
pub const flattenMap = map_encoder.flattenMap;
pub const RowWriter = row_writer.RowWriter;
pub const RowWriterOptions = row_writer.RowWriterOptions;
pub const ColumnDef = column_def.ColumnDef;
pub const StructField = column_def.StructField;
pub const StatisticsBuilder = statistics.StatisticsBuilder;
pub const ByteArrayStatisticsBuilder = statistics.ByteArrayStatisticsBuilder;
pub const WriteTarget = write_target.WriteTarget;
pub const WriteTargetWriter = write_target.WriteTargetWriter;
pub const BackendCleanup = @import("parquet_reader.zig").BackendCleanup;

test {
    _ = page_writer;
    _ = column_writer;
    _ = column_write_list;
    _ = column_write_struct;
    _ = column_write_map;
    _ = list_encoder;
    _ = map_encoder;
    _ = row_writer;
    _ = column_def;
    _ = statistics;
    _ = write_target;
}
