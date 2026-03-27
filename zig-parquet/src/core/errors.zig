//! Categorized error sets for the Parquet library.
//!
//! Errors fall into three categories:
//! - Transport/IO: originate in transport adapters (SeekableReader, WriteTarget)
//! - Format/Data: originate in core during parsing, decoding, compression
//! - Caller: originate in core's public method validation (API misuse)

/// Transport and I/O errors from SeekableReader / WriteTarget / system calls.
pub const TransportError = error{
    InputOutput,
    Unseekable,
    WriteError,
    BrokenPipe,
    Unexpected,
    Overflow,
    InvalidArgument,
    SystemResources,
    OperationAborted,
    NotOpenForReading,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    IsDir,
    AccessDenied,
    FileTooBig,
    NoSpaceLeft,
    DeviceBusy,
    WouldBlock,
    NetworkError,
    NoDevice,
};

/// Format errors from invalid Parquet file structure.
pub const FormatError = error{
    InvalidMagic,
    FileTooSmall,
    FooterTooLarge,
    InvalidPageData,
    InvalidPageSize,
    InvalidRowCount,
    InvalidTypeLength,
    InvalidPhysicalType,
    InvalidRepetitionType,
    InvalidPageType,
    InvalidSchema,
    UnsupportedType,
};

/// Decoding errors from encoding/decoding failures including Thrift parsing.
pub const DecodingError = error{
    EndOfData,
    InvalidFieldType,
    InvalidListType,
    VarIntTooLong,
    IntegerOverflow,
    LengthTooLong,
    ListTooLong,
    InvalidEncoding,
    InvalidBitWidth,
    InvalidBitPackedLength,
    UnsupportedEncoding,
};

/// Compression codec errors.
pub const CompressionError = error{
    UnsupportedCompression,
    DecompressionError,
    InvalidCompressionCodec,
    InvalidCompressionState,
    CompressionError,
};

/// Checksum verification errors.
pub const ChecksumError = error{
    PageChecksumMismatch,
    MissingPageChecksum,
};

/// Caller/API misuse errors (invalid state, wrong types, bad indices).
pub const CallerError = error{
    InvalidState,
    InvalidColumnIndex,
    ColumnAlreadyWritten,
    ColumnNotWritten,
    TypeMismatch,
    InvalidFixedLength,
    RowCountMismatch,
    ValueTooLarge,
    TooManyRows,
};

/// Resource allocation errors.
pub const ResourceError = error{
    OutOfMemory,
    AllocationLimitExceeded,
};

/// Edge interpolation errors (geospatial).
pub const GeoError = error{
    InvalidEdgeInterpolationAlgorithm,
};

/// Composed error set for reader operations.
pub const ReaderError = TransportError || FormatError || DecodingError || CompressionError || ChecksumError || ResourceError || GeoError;

/// Composed error set for writer operations.
pub const WriterError = ResourceError || CallerError || CompressionError || error{
    WriteError,
    IntegerOverflow,
    UnsupportedEncoding,
};

/// Composed error set for DynamicReader operations.
/// Same as ReaderError — avoids the `else => SchemaParseError` coalescing problem
/// by being a proper superset of all errors parseFooter can return.
pub const DynamicReaderError = ReaderError;
