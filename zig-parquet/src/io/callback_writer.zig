//! CallbackWriter - callback-backed WriteTarget implementation
//!
//! Enables writing to caller-provided function pointers, bridging the
//! WriteTarget vtable to external IO callbacks. This is the primary
//! transport for C ABI, WASM host, and custom IO backends.
//!
//! ## Example
//! ```zig
//! var cb = CallbackWriter{
//!     .ctx = @ptrCast(&my_sink),
//!     .write_fn = myWrite,
//!     .close_fn = myClose,
//! };
//! const target = cb.target();
//! var writer = try Writer.initWithTarget(allocator, target, columns);
//! ```

const write_target = @import("../core/write_target.zig");
const WriteTarget = write_target.WriteTarget;
const WriteError = write_target.WriteError;

/// Callback-backed sequential writer.
///
/// The caller owns the context pointer and guarantees it outlives the
/// CallbackWriter. The library never frees the callback context.
pub const CallbackWriter = struct {
    ctx: *anyopaque,
    write_fn: *const fn (ctx: *anyopaque, data: []const u8) WriteError!void,
    close_fn: ?*const fn (ctx: *anyopaque) WriteError!void = null,

    const vtable = WriteTarget.VTable{
        .write = callbackWrite,
        .close = callbackClose,
    };

    fn callbackWrite(ptr: *anyopaque, data: []const u8) WriteError!void {
        const self: *CallbackWriter = @ptrCast(@alignCast(ptr));
        return self.write_fn(self.ctx, data);
    }

    fn callbackClose(ptr: *anyopaque) WriteError!void {
        const self: *CallbackWriter = @ptrCast(@alignCast(ptr));
        if (self.close_fn) |f| return f(self.ctx);
    }

    /// Get a WriteTarget interface backed by these callbacks.
    /// The CallbackWriter must outlive the returned WriteTarget.
    pub fn target(self: *CallbackWriter) WriteTarget {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};
