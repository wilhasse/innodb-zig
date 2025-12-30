const compat = @import("../ut/compat.zig");

pub const module_name = "trx";
pub const trx_id_t = compat.ib_uint64_t;

pub const trx_t = struct {
    id: trx_id_t = 0,
};
