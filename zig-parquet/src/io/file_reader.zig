//! FileReader - file-based SeekableReader backend
//!
//! Provides lazy reading - only reads data when requested, not at init time.

const std = @import("std");
const SeekableReader = @import("../core/seekable_reader.zig").SeekableReader;

pub const FileReader = struct {
    file: std.fs.File,
    file_size: u64,

    /// Create a FileReader from an open file handle.
    /// The file must be seekable and remain open while the FileReader is in use.
    pub fn init(file: std.fs.File) SeekableReader.Error!FileReader {
        const file_size = file.getEndPos() catch return error.Unseekable;
        return .{ .file = file, .file_size = file_size };
    }

    /// Get a SeekableReader interface for this file.
    /// The FileReader must outlive the returned SeekableReader.
    pub fn reader(self: *FileReader) SeekableReader {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = SeekableReader.VTable{
        .readAt = readAt,
        .size = getSize,
    };

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *FileReader = @ptrCast(@alignCast(ctx));
        self.file.seekTo(offset) catch return error.Unseekable;
        return self.file.read(buf) catch return error.InputOutput;
    }

    fn getSize(ctx: *anyopaque) u64 {
        const self: *FileReader = @ptrCast(@alignCast(ctx));
        return self.file_size;
    }
};
