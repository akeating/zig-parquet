//! Parquet format types
//!
//! These types mirror the Thrift definitions from the Parquet format specification.
//! Reference: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift

// Re-export all types from submodules
pub const types = @import("format/types.zig");
pub const schema = @import("format/schema.zig");
pub const statistics = @import("format/statistics.zig");
pub const column = @import("format/column.zig");
pub const row_group = @import("format/row_group.zig");
pub const page = @import("format/page.zig");
pub const metadata = @import("format/metadata.zig");
pub const logical_types = @import("format/logical_types.zig");
pub const schema_utils = @import("format/schema_utils.zig");

// Convenience re-exports for common types
pub const PhysicalType = types.PhysicalType;
pub const RepetitionType = types.RepetitionType;
pub const Encoding = types.Encoding;
pub const CompressionCodec = types.CompressionCodec;
pub const PageType = types.PageType;

pub const LogicalType = logical_types.LogicalType;
pub const TimeUnit = logical_types.TimeUnit;
pub const TimestampType = logical_types.TimestampType;
pub const TimeType = logical_types.TimeType;
pub const DecimalType = logical_types.DecimalType;
pub const IntType = logical_types.IntType;
pub const EdgeInterpolationAlgorithm = logical_types.EdgeInterpolationAlgorithm;
pub const GeometryType = logical_types.GeometryType;
pub const GeographyType = logical_types.GeographyType;

pub const SchemaElement = schema.SchemaElement;
pub const Statistics = statistics.Statistics;
pub const BoundingBox = statistics.BoundingBox;
pub const GeospatialStatistics = statistics.GeospatialStatistics;
pub const ColumnMetaData = column.ColumnMetaData;
pub const ColumnChunk = column.ColumnChunk;
pub const RowGroup = row_group.RowGroup;
pub const KeyValue = row_group.KeyValue;
pub const FileMetaData = metadata.FileMetaData;
pub const PageHeader = page.PageHeader;
pub const DataPageHeader = page.DataPageHeader;
pub const DataPageHeaderV2 = page.DataPageHeaderV2;
pub const DictionaryPageHeader = page.DictionaryPageHeader;

// Schema utilities
pub const ColumnInfo = schema_utils.ColumnInfo;
pub const getColumnInfo = schema_utils.getColumnInfo;
pub const maxDefinitionLevel = schema_utils.maxDefinitionLevel;
pub const maxRepetitionLevel = schema_utils.maxRepetitionLevel;
pub const computeBitWidth = schema_utils.computeBitWidth;

// File format constants
pub const PARQUET_MAGIC: *const [4]u8 = "PAR1";

/// Converted type constants from Parquet specification.
/// These indicate the logical type that corresponds to a physical type.
/// Reference: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
pub const ConvertedType = struct {
    pub const UTF8: i32 = 0;
    pub const MAP: i32 = 1;
    pub const MAP_KEY_VALUE: i32 = 2;
    pub const LIST: i32 = 3;
    pub const ENUM: i32 = 4;
    pub const DECIMAL: i32 = 5;
    pub const DATE: i32 = 6;
    pub const TIME_MILLIS: i32 = 7;
    pub const TIME_MICROS: i32 = 8;
    pub const TIMESTAMP_MILLIS: i32 = 9;
    pub const TIMESTAMP_MICROS: i32 = 10;
    pub const UINT_8: i32 = 11;
    pub const UINT_16: i32 = 12;
    pub const UINT_32: i32 = 13;
    pub const UINT_64: i32 = 14;
    pub const INT_8: i32 = 15;
    pub const INT_16: i32 = 16;
    pub const INT_32: i32 = 17;
    pub const INT_64: i32 = 18;
    pub const JSON: i32 = 19;
    pub const BSON: i32 = 20;
    pub const INTERVAL: i32 = 21;
};
