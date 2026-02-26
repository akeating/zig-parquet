//! C ABI error codes and per-handle error context.
//!
//! Maps Zig error unions to stable integer status codes for C callers.
//! Error codes are assigned explicit integer values and are stable across
//! library versions. New codes may be added but existing codes will not
//! change meaning.

const std = @import("std");

pub const ZP_OK: c_int = 0;
pub const ZP_ERROR_INVALID_DATA: c_int = 1;
pub const ZP_ERROR_NOT_PARQUET: c_int = 2;
pub const ZP_ERROR_IO: c_int = 3;
pub const ZP_ERROR_OUT_OF_MEMORY: c_int = 4;
pub const ZP_ERROR_UNSUPPORTED: c_int = 5;
pub const ZP_ERROR_INVALID_ARGUMENT: c_int = 6;
pub const ZP_ERROR_INVALID_STATE: c_int = 7;
pub const ZP_ERROR_CHECKSUM: c_int = 8;
pub const ZP_ERROR_SCHEMA: c_int = 9;
pub const ZP_ERROR_HANDLE_LIMIT: c_int = 10;
pub const ZP_ROW_END: c_int = 11;

// Value type constants for the row-oriented API
// Physical types (0-9)
pub const ZP_TYPE_NULL: c_int = 0;
pub const ZP_TYPE_BOOL: c_int = 1;
pub const ZP_TYPE_INT32: c_int = 2;
pub const ZP_TYPE_INT64: c_int = 3;
pub const ZP_TYPE_FLOAT: c_int = 4;
pub const ZP_TYPE_DOUBLE: c_int = 5;
pub const ZP_TYPE_BYTES: c_int = 6;
pub const ZP_TYPE_LIST: c_int = 7;
pub const ZP_TYPE_MAP: c_int = 8;
pub const ZP_TYPE_STRUCT: c_int = 9;

// Logical types (10+)
pub const ZP_TYPE_STRING: c_int = 10;
pub const ZP_TYPE_DATE: c_int = 11;
pub const ZP_TYPE_TIMESTAMP_MILLIS: c_int = 12;
pub const ZP_TYPE_TIMESTAMP_MICROS: c_int = 13;
pub const ZP_TYPE_TIMESTAMP_NANOS: c_int = 14;
pub const ZP_TYPE_TIME_MILLIS: c_int = 15;
pub const ZP_TYPE_TIME_MICROS: c_int = 16;
pub const ZP_TYPE_TIME_NANOS: c_int = 17;
pub const ZP_TYPE_INT8: c_int = 18;
pub const ZP_TYPE_INT16: c_int = 19;
pub const ZP_TYPE_UINT8: c_int = 20;
pub const ZP_TYPE_UINT16: c_int = 21;
pub const ZP_TYPE_UINT32: c_int = 22;
pub const ZP_TYPE_UINT64: c_int = 23;
pub const ZP_TYPE_UUID: c_int = 24;
pub const ZP_TYPE_JSON: c_int = 25;
pub const ZP_TYPE_ENUM: c_int = 26;
pub const ZP_TYPE_DECIMAL: c_int = 27;
pub const ZP_TYPE_FLOAT16: c_int = 28;
pub const ZP_TYPE_BSON: c_int = 29;
pub const ZP_TYPE_INTERVAL: c_int = 30;
pub const ZP_TYPE_GEOMETRY: c_int = 31;
pub const ZP_TYPE_GEOGRAPHY: c_int = 32;

/// Per-handle error context with human-readable message.
/// Valid until the next API call on the same handle.
pub const ErrorContext = struct {
    code: c_int = ZP_OK,
    message_buf: [512]u8 = undefined,
    message_len: usize = 0,

    pub fn setOk(self: *ErrorContext) void {
        self.code = ZP_OK;
        self.message_len = 0;
    }

    pub fn setError(self: *ErrorContext, code: c_int, msg: []const u8) void {
        self.code = code;
        const len = @min(msg.len, self.message_buf.len - 1);
        @memcpy(self.message_buf[0..len], msg[0..len]);
        self.message_buf[len] = 0;
        self.message_len = len;
    }

    pub fn setErrorFmt(self: *ErrorContext, code: c_int, comptime fmt: []const u8, args: anytype) void {
        self.code = code;
        const slice = std.fmt.bufPrint(&self.message_buf, fmt, args) catch blk: {
            const trunc = "(truncated)";
            const end = self.message_buf.len - trunc.len - 1;
            @memcpy(self.message_buf[end .. end + trunc.len], trunc);
            self.message_buf[self.message_buf.len - 1] = 0;
            self.message_len = self.message_buf.len - 1;
            break :blk self.message_buf[0 .. self.message_buf.len - 1];
        };
        self.message_buf[slice.len] = 0;
        self.message_len = slice.len;
    }

    /// Return a null-terminated C string pointer into the message buffer.
    pub fn message(self: *const ErrorContext) [*:0]const u8 {
        if (self.message_len == 0) return "";
        return @ptrCast(self.message_buf[0..self.message_len :0]);
    }
};

/// Map a Zig error to a stable C ABI error code.
pub fn mapError(err: anyerror) c_int {
    return switch (err) {
        // ZP_ERROR_NOT_PARQUET (2)
        error.InvalidMagic, error.FileTooSmall => ZP_ERROR_NOT_PARQUET,

        // ZP_ERROR_IO (3)
        error.InputOutput,
        error.WriteError,
        error.BrokenPipe,
        error.Unexpected,
        error.NotOpenForReading,
        error.ConnectionResetByPeer,
        error.ConnectionTimedOut,
        error.IsDir,
        error.AccessDenied,
        error.FileTooBig,
        error.NoSpaceLeft,
        error.DeviceBusy,
        error.WouldBlock,
        error.NetworkError,
        error.NoDevice,
        error.Unseekable,
        error.SystemResources,
        error.OperationAborted,
        => ZP_ERROR_IO,

        // ZP_ERROR_OUT_OF_MEMORY (4)
        error.OutOfMemory, error.AllocationLimitExceeded => ZP_ERROR_OUT_OF_MEMORY,

        // ZP_ERROR_UNSUPPORTED (5)
        error.UnsupportedCompression, error.UnsupportedEncoding, error.UnsupportedType => ZP_ERROR_UNSUPPORTED,

        // ZP_ERROR_INVALID_ARGUMENT (6)
        error.InvalidColumnIndex,
        error.TypeMismatch,
        error.InvalidArgument,
        error.InvalidFixedLength,
        error.ValueTooLarge,
        error.TooManyRows,
        error.TooManyValues,
        => ZP_ERROR_INVALID_ARGUMENT,

        // ZP_ERROR_INVALID_STATE (7)
        error.InvalidState,
        error.ColumnAlreadyWritten,
        error.ColumnNotWritten,
        => ZP_ERROR_INVALID_STATE,

        // ZP_ERROR_CHECKSUM (8)
        error.PageChecksumMismatch, error.MissingPageChecksum => ZP_ERROR_CHECKSUM,

        // ZP_ERROR_SCHEMA (9)
        error.InvalidSchema,
        error.RowCountMismatch,
        error.InvalidRepetitionType,
        => ZP_ERROR_SCHEMA,

        // ZP_ERROR_INVALID_DATA (1) -- catch-all for format/decoding errors
        error.InvalidPageData,
        error.InvalidPageSize,
        error.InvalidRowCount,
        error.InvalidTypeLength,
        error.InvalidPhysicalType,
        error.InvalidPageType,
        error.EndOfData,
        error.InvalidFieldType,
        error.InvalidListType,
        error.VarIntTooLong,
        error.IntegerOverflow,
        error.Overflow,
        error.LengthTooLong,
        error.ListTooLong,
        error.InvalidEncoding,
        error.InvalidBitWidth,
        error.InvalidBitPackedLength,
        error.DecompressionError,
        error.CompressionError,
        error.InvalidCompressionCodec,
        error.InvalidCompressionState,
        error.FooterTooLarge,
        error.InvalidEdgeInterpolationAlgorithm,
        => ZP_ERROR_INVALID_DATA,

        else => ZP_ERROR_INVALID_DATA,
    };
}

/// Format a Zig error into a human-readable message string.
pub fn errorMessage(err: anyerror) []const u8 {
    return @errorName(err);
}
