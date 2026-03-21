//! Reader components
//!
//! Low-level components used by the Parquet reader.

pub const column_decoder = @import("column_decoder.zig");
pub const list_decoder = @import("list_decoder.zig");
pub const map_decoder = @import("map_decoder.zig");
pub const dynamic_reader = @import("dynamic_reader.zig");
pub const seekable_reader = @import("seekable_reader.zig");
pub const parquet_reader = @import("parquet_reader.zig");

// Re-export commonly used types
pub const DecodeContext = column_decoder.DecodeContext;
pub const decodeColumn = column_decoder.decodeColumn;
pub const decodeColumnDynamic = column_decoder.decodeColumnDynamic;
pub const DynamicDecodeResult = column_decoder.DynamicDecodeResult;
pub const assembleList = list_decoder.assembleList;
pub const freeListColumn = list_decoder.freeListColumn;
pub const ListColumn = list_decoder.ListColumn;
pub const assembleMap = map_decoder.assembleMap;
pub const freeMapColumn = map_decoder.freeMapColumn;
pub const MapColumn = map_decoder.MapColumn;
pub const MapEntry = map_decoder.MapEntry;
pub const DynamicReader = dynamic_reader.DynamicReader;
pub const DynamicReaderError = dynamic_reader.DynamicReaderError;
pub const DynamicReaderOptions = dynamic_reader.DynamicReaderOptions;
pub const ChecksumOptions = parquet_reader.ChecksumOptions;
pub const BackendCleanup = parquet_reader.BackendCleanup;
pub const SeekableReader = seekable_reader.SeekableReader;
pub const PageIterator = parquet_reader.PageIterator;
pub const PageInfo = parquet_reader.PageInfo;
pub const freePageHeaderContents = parquet_reader.freePageHeaderContents;
pub const DictionarySet = parquet_reader.DictionarySet;
