const compat = @import("../ut/compat.zig");

pub const module_name = "trx";
pub const trx_id_t = compat.ib_uint64_t;
pub const undo_no_t = compat.Dulint;
pub const roll_ptr_t = compat.Dulint;

pub const trx_t = struct {
    id: trx_id_t = 0,
};

pub const purge = @import("purge.zig");
pub const rec = @import("rec.zig");
