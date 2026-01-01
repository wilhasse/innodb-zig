const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const mach = @import("../mach/mod.zig");
const que = @import("../que/mod.zig");
const pars = @import("../pars/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "eval";

pub const ulint = compat.ulint;
pub const lint = compat.lint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const que_node_t = que.que_node_t;
pub const func_node_t = pars.func_node_t;
pub const sym_node_t = pars.sym_node_t;
pub const que_thr_t = que.que_thr_t;
pub const if_node_t = pars.if_node_t;
pub const elsif_node_t = pars.elsif_node_t;
pub const while_node_t = pars.while_node_t;
pub const assign_node_t = pars.assign_node_t;
pub const for_node_t = pars.for_node_t;
pub const exit_node_t = pars.exit_node_t;
pub const return_node_t = pars.return_node_t;
pub const dfield_t = data.dfield_t;

const UNIV_SQL_NULL: ulint = @as(ulint, compat.UNIV_SQL_NULL);

var eval_rnd: ulint = 128367121;
var eval_dummy: byte = 0;

fn ut_ad(cond: bool) void {
    std.debug.assert(cond);
}

fn ut_a(cond: bool) void {
    std.debug.assert(cond);
}

fn ut_error() noreturn {
    @panic("ut_error");
}

fn ut_memcmp(a: [*]const byte, b: [*]const byte, len: ulint) i32 {
    const n = @as(usize, @intCast(len));
    const ord = std.mem.order(byte, a[0..n], b[0..n]);
    return switch (ord) {
        .lt => -1,
        .eq => 0,
        .gt => 1,
    };
}

fn ut_memcpy(dst: [*]byte, src: [*]const byte, len: ulint) void {
    const n = @as(usize, @intCast(len));
    std.mem.copyForwards(byte, dst[0..n], src[0..n]);
}

fn mem_alloc(size: ulint) [*]byte {
    const n = @as(usize, @intCast(size));
    const buf = std.heap.page_allocator.alloc(byte, n) catch @panic("mem_alloc failed");
    return buf.ptr;
}

fn mem_free(ptr: [*]byte, size: ulint) void {
    const n = @as(usize, @intCast(size));
    std.heap.page_allocator.free(ptr[0..n]);
}

fn ut_rnd_gen_next_ulint(rnd: ulint) ulint {
    const UT_RND1: ulint = 151117737;
    const UT_RND2: ulint = 119785373;
    const UT_RND3: ulint = 85689495;
    const UT_SUM_RND2: ulint = 98781234;
    const UT_SUM_RND3: ulint = 126792457;
    const UT_SUM_RND4: ulint = 63498502;
    const UT_XOR_RND1: ulint = 187678878;
    const UT_XOR_RND2: ulint = 143537923;

    const n_bits = 8 * @sizeOf(ulint);
    var out = rnd;
    out = UT_RND2 * out + UT_SUM_RND3;
    out = UT_XOR_RND1 ^ out;
    out = (out << 20) + (out >> (n_bits - 20));
    out = UT_RND3 * out + UT_SUM_RND4;
    out = UT_XOR_RND2 ^ out;
    out = (out << 20) + (out >> (n_bits - 20));
    out = UT_RND1 * out + UT_SUM_RND2;
    return out;
}

fn ut_time() ulint {
    return @as(ulint, @intCast(std.time.timestamp()));
}

fn ib_logger(_: anytype, comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

const ib_stream = struct {}{};

fn dfield_print(dfield: *const dfield_t) void {
    const len = data.dfield_get_len(dfield);
    if (len == UNIV_SQL_NULL) {
        std.debug.print("NULL", .{});
        return;
    }
    const ptr = data.dfield_get_data(dfield) orelse {
        std.debug.print("<null>", .{});
        return;
    };
    const n = @as(usize, @intCast(len));
    const bytes = @as([*]const byte, @ptrCast(ptr))[0..n];
    std.debug.print("{s}", .{bytes});
}

fn cmp_dfield_dfield(dfield1: *const dfield_t, dfield2: *const dfield_t) i32 {
    const len1 = data.dfield_get_len(dfield1);
    const len2 = data.dfield_get_len(dfield2);
    if (len1 == UNIV_SQL_NULL or len2 == UNIV_SQL_NULL) {
        if (len1 == len2) {
            return 0;
        }
        return if (len1 == UNIV_SQL_NULL) -1 else 1;
    }

    const type1 = data.dfield_get_type(dfield1);
    if (data.dtype_get_mtype(type1) == data.DATA_INT and len1 == 4 and len2 == 4) {
        const ptr1 = data.dfield_get_data(dfield1).?;
        const ptr2 = data.dfield_get_data(dfield2).?;
        const v1 = mach.mach_read_from_4(@as([*]const byte, @ptrCast(ptr1)));
        const v2 = mach.mach_read_from_4(@as([*]const byte, @ptrCast(ptr2)));
        if (v1 < v2) return -1;
        if (v1 > v2) return 1;
        return 0;
    }

    const n1 = @as(usize, @intCast(len1));
    const n2 = @as(usize, @intCast(len2));
    const ptr1 = data.dfield_get_data(dfield1) orelse return -1;
    const ptr2 = data.dfield_get_data(dfield2) orelse return 1;
    const min_len = if (n1 < n2) n1 else n2;
    if (min_len > 0) {
        const ord = std.mem.order(
            byte,
            @as([*]const byte, @ptrCast(ptr1))[0..min_len],
            @as([*]const byte, @ptrCast(ptr2))[0..min_len],
        );
        switch (ord) {
            .lt => return -1,
            .gt => return 1,
            .eq => {},
        }
    }
    if (n1 < n2) return -1;
    if (n1 > n2) return 1;
    return 0;
}

pub fn eval_node_alloc_val_buf(node: *que_node_t, size: ulint) [*]byte {
    ut_ad(que.que_node_get_type(node) == que.QUE_NODE_SYMBOL or que.que_node_get_type(node) == que.QUE_NODE_FUNC);

    const dfield = que.que_node_get_val(node);
    const data_ptr = data.dfield_get_data(dfield);
    const dummy_ptr: *const anyopaque = @ptrCast(&eval_dummy);
    if (data_ptr != null and data_ptr.? != dummy_ptr) {
        const old_size = que.que_node_get_val_buf_size(node);
        if (old_size > 0) {
            mem_free(@as([*]byte, @ptrCast(@constCast(data_ptr.?))), old_size);
        }
    }

    var out: [*]byte = undefined;
    if (size == 0) {
        out = @ptrCast(&eval_dummy);
    } else {
        out = mem_alloc(size);
    }

    que.que_node_set_val_buf_size(node, size);
    data.dfield_set_data(dfield, out, size);
    return out;
}

pub fn eval_node_free_val_buf(node: *que_node_t) void {
    ut_ad(que.que_node_get_type(node) == que.QUE_NODE_SYMBOL or que.que_node_get_type(node) == que.QUE_NODE_FUNC);

    const dfield = que.que_node_get_val(node);
    const data_ptr = data.dfield_get_data(dfield);
    if (que.que_node_get_val_buf_size(node) > 0) {
        ut_a(data_ptr != null);
        mem_free(@as([*]byte, @ptrCast(@constCast(data_ptr.?))), que.que_node_get_val_buf_size(node));
    }
    que.que_node_set_val_buf_size(node, 0);
}

pub fn eval_node_ensure_val_buf(node: *que_node_t, size: ulint) [*]byte {
    const dfield = que.que_node_get_val(node);
    data.dfield_set_len(dfield, size);

    const data_ptr = data.dfield_get_data(dfield);
    if (data_ptr == null or que.que_node_get_val_buf_size(node) < size) {
        return eval_node_alloc_val_buf(node, size);
    }
    return @as([*]byte, @ptrCast(@constCast(data_ptr.?)));
}

pub fn eval_sym(sym_node: *sym_node_t) void {
    ut_ad(que.que_node_get_type(&sym_node.common) == que.QUE_NODE_SYMBOL);
    if (sym_node.indirection) |indir| {
        data.dfield_copy_data(que.que_node_get_val(&sym_node.common), que.que_node_get_val(&indir.common));
    }
}

pub fn eval_exp(exp_node: *que_node_t) void {
    if (que.que_node_get_type(exp_node) == que.QUE_NODE_SYMBOL) {
        eval_sym(@as(*sym_node_t, @ptrCast(exp_node)));
        return;
    }
    eval_func(@as(*func_node_t, @ptrCast(exp_node)));
}

pub fn eval_node_set_int_val(node: *que_node_t, val: lint) void {
    const dfield = que.que_node_get_val(node);
    var data_ptr = data.dfield_get_data(dfield);
    if (data_ptr == null) {
        data_ptr = @ptrCast(eval_node_alloc_val_buf(node, 4));
    }
    ut_ad(data.dfield_get_len(dfield) == 4);
    const signed32: i32 = @as(i32, @truncate(val));
    const raw: u32 = @bitCast(signed32);
    mach.mach_write_to_4(@as([*]byte, @ptrCast(@constCast(data_ptr.?))), @as(ulint, raw));
}

pub fn eval_node_get_int_val(node: *que_node_t) lint {
    const dfield = que.que_node_get_val(node);
    ut_ad(data.dfield_get_len(dfield) == 4);
    const data_ptr = data.dfield_get_data(dfield).?;
    const raw = @as(u32, @intCast(mach.mach_read_from_4(@as([*]const byte, @ptrCast(data_ptr)))));
    const signed32: i32 = @bitCast(raw);
    return @as(lint, signed32);
}

pub fn eval_node_get_ibool_val(node: *que_node_t) ibool {
    const dfield = que.que_node_get_val(node);
    const data_ptr = data.dfield_get_data(dfield);
    ut_ad(data_ptr != null);
    return mach.mach_read_from_1(@as([*]const byte, @ptrCast(data_ptr.?)));
}

pub fn eval_node_get_bool_val(node: *que_node_t) ibool {
    eval_exp(node);
    return eval_node_get_ibool_val(node);
}

pub fn eval_node_set_ibool_val(func_node: *func_node_t, val: ibool) void {
    const dfield = que.que_node_get_val(&func_node.common);
    var data_ptr = data.dfield_get_data(dfield);
    if (data_ptr == null) {
        data_ptr = @ptrCast(eval_node_alloc_val_buf(&func_node.common, 1));
    }
    ut_ad(data.dfield_get_len(dfield) == 1);
    mach.mach_write_to_1(@as([*]byte, @ptrCast(@constCast(data_ptr.?))), val);
}

pub fn eval_node_copy_and_alloc_val(node: *que_node_t, str: [*]const byte, len: ulint) void {
    if (len == UNIV_SQL_NULL) {
        data.dfield_set_len(que.que_node_get_val(node), len);
        return;
    }
    const data_ptr = eval_node_ensure_val_buf(node, len);
    ut_memcpy(data_ptr, str, len);
}

pub fn eval_node_copy_val(node1: *que_node_t, node2: *que_node_t) void {
    const dfield2 = que.que_node_get_val(node2);
    const data_ptr = data.dfield_get_data(dfield2) orelse {
        data.dfield_set_len(que.que_node_get_val(node1), UNIV_SQL_NULL);
        return;
    };
    eval_node_copy_and_alloc_val(node1, @as([*]const byte, @ptrCast(data_ptr)), data.dfield_get_len(dfield2));
}

pub fn eval_cmp(cmp_node: *func_node_t) ibool {
    ut_ad(que.que_node_get_type(&cmp_node.common) == que.QUE_NODE_FUNC);
    const arg1 = cmp_node.args.?;
    const arg2 = que.que_node_get_next(arg1).?;
    const res = cmp_dfield_dfield(que.que_node_get_val(arg1), que.que_node_get_val(arg2));

    var val: ibool = compat.TRUE;
    const func = cmp_node.func;
    if (func == '=') {
        if (res != 0) val = compat.FALSE;
    } else if (func == '<') {
        if (res != -1) val = compat.FALSE;
    } else if (func == pars.PARS_LE_TOKEN) {
        if (res == 1) val = compat.FALSE;
    } else if (func == pars.PARS_NE_TOKEN) {
        if (res == 0) val = compat.FALSE;
    } else if (func == pars.PARS_GE_TOKEN) {
        if (res == -1) val = compat.FALSE;
    } else {
        ut_ad(func == '>');
        if (res != 1) val = compat.FALSE;
    }

    eval_node_set_ibool_val(cmp_node, val);
    return val;
}

pub fn eval_logical(logical_node: *func_node_t) void {
    ut_ad(que.que_node_get_type(&logical_node.common) == que.QUE_NODE_FUNC);
    const arg1 = logical_node.args.?;
    const arg2 = que.que_node_get_next(arg1);

    const val1 = eval_node_get_ibool_val(arg1);
    var val2: ibool = 0;
    if (arg2) |node| {
        val2 = eval_node_get_ibool_val(node);
    }

    const func = logical_node.func;
    var val: ibool = 0;
    if (func == pars.PARS_AND_TOKEN) {
        val = val1 & val2;
    } else if (func == pars.PARS_OR_TOKEN) {
        val = val1 | val2;
    } else if (func == pars.PARS_NOT_TOKEN) {
        val = compat.TRUE - val1;
    } else {
        ut_error();
    }

    eval_node_set_ibool_val(logical_node, val);
}

pub fn eval_arith(arith_node: *func_node_t) void {
    ut_ad(que.que_node_get_type(&arith_node.common) == que.QUE_NODE_FUNC);
    const arg1 = arith_node.args.?;
    const arg2 = que.que_node_get_next(arg1);

    const val1 = eval_node_get_int_val(arg1);
    var val2: lint = 0;
    if (arg2) |node| {
        val2 = eval_node_get_int_val(node);
    }

    const func = arith_node.func;
    var val: lint = 0;
    if (func == '+') {
        val = val1 + val2;
    } else if (func == '-' and arg2 != null) {
        val = val1 - val2;
    } else if (func == '-') {
        val = -val1;
    } else if (func == '*') {
        val = val1 * val2;
    } else {
        ut_ad(func == '/');
        val = val1 / val2;
    }

    eval_node_set_int_val(&arith_node.common, val);
}

pub fn eval_aggregate(node: *func_node_t) void {
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_FUNC);
    var val = eval_node_get_int_val(&node.common);

    const func = node.func;
    if (func == pars.PARS_COUNT_TOKEN) {
        val += 1;
    } else {
        ut_ad(func == pars.PARS_SUM_TOKEN);
        const arg = node.args.?;
        const arg_val = eval_node_get_int_val(arg);
        val += arg_val;
    }
    eval_node_set_int_val(&node.common, val);
}

fn eval_predefined_2(func_node: *func_node_t) void {
    ut_ad(que.que_node_get_type(&func_node.common) == que.QUE_NODE_FUNC);
    const arg1 = func_node.args;
    const arg2 = if (arg1) |arg| que.que_node_get_next(arg) else null;

    const func = func_node.func;
    if (func == pars.PARS_PRINTF_TOKEN) {
        var arg = arg1;
        while (arg) |node| {
            dfield_print(que.que_node_get_val(node));
            arg = que.que_node_get_next(node);
        }
        ib_logger(ib_stream, "\n", .{});
    } else if (func == pars.PARS_ASSERT_TOKEN) {
        if (!eval_node_get_ibool_val(arg1.?)) {
            ib_logger(ib_stream, "SQL assertion fails in a stored procedure!\n", .{});
        }
        ut_a(eval_node_get_ibool_val(arg1.?));
    } else if (func == pars.PARS_RND_TOKEN) {
        const len1 = @as(ulint, @intCast(eval_node_get_int_val(arg1.?)));
        const len2 = @as(ulint, @intCast(eval_node_get_int_val(arg2.?)));
        ut_ad(len2 >= len1);
        var int_val: lint = 0;
        if (len2 > len1) {
            int_val = @as(lint, @intCast(len1 + (eval_rnd % (len2 - len1 + 1))));
        } else {
            int_val = @as(lint, @intCast(len1));
        }
        eval_rnd = ut_rnd_gen_next_ulint(eval_rnd);
        eval_node_set_int_val(&func_node.common, int_val);
    } else if (func == pars.PARS_RND_STR_TOKEN) {
        const len1 = @as(ulint, @intCast(eval_node_get_int_val(arg1.?)));
        const buf = eval_node_ensure_val_buf(&func_node.common, len1);
        var i: ulint = 0;
        while (i < len1) : (i += 1) {
            buf[i] = @as(byte, @intCast(97 + (eval_rnd % 3)));
            eval_rnd = ut_rnd_gen_next_ulint(eval_rnd);
        }
    } else {
        ut_error();
    }
}

fn eval_notfound(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const arg2 = que.que_node_get_next(arg1);
    _ = arg2;

    ut_ad(func_node.func == pars.PARS_NOTFOUND_TOKEN);
    const cursor = @as(*sym_node_t, @ptrCast(arg1));
    ut_ad(que.que_node_get_type(&cursor.common) == que.QUE_NODE_SYMBOL);

    var sel_node: *row.sel_node_t = undefined;
    if (cursor.token_type == .SYM_LIT) {
        const val_ptr = data.dfield_get_data(que.que_node_get_val(&cursor.common)).?;
        ut_ad(ut_memcmp(@as([*]const byte, @ptrCast(val_ptr)), "SQL", 3) == 0);
        sel_node = cursor.sym_table.?.query_graph.?.last_sel_node.?;
    } else {
        sel_node = cursor.alias.?.cursor_def.?;
    }

    const ibool_val: ibool = if (sel_node.state == .SEL_NODE_NO_MORE_ROWS) compat.TRUE else compat.FALSE;
    eval_node_set_ibool_val(func_node, ibool_val);
}

fn eval_substr(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const arg2 = que.que_node_get_next(arg1).?;
    ut_ad(func_node.func == pars.PARS_SUBSTR_TOKEN);
    const arg3 = que.que_node_get_next(arg2).?;

    const str1 = data.dfield_get_data(que.que_node_get_val(arg1)).?;
    const len1 = @as(ulint, @intCast(eval_node_get_int_val(arg2)));
    const len2 = @as(ulint, @intCast(eval_node_get_int_val(arg3)));
    const dfield = que.que_node_get_val(&func_node.common);
    data.dfield_set_data(dfield, @as([*]const byte, @ptrCast(str1)) + len1, len2);
}

fn eval_replstr(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const arg2 = que.que_node_get_next(arg1).?;
    ut_ad(que.que_node_get_type(arg1) == que.QUE_NODE_SYMBOL);
    const arg3 = que.que_node_get_next(arg2).?;
    const arg4 = que.que_node_get_next(arg3).?;

    const str1 = data.dfield_get_data(que.que_node_get_val(arg1)).?;
    const str2 = data.dfield_get_data(que.que_node_get_val(arg2)).?;
    const len1 = @as(ulint, @intCast(eval_node_get_int_val(arg3)));
    const len2 = @as(ulint, @intCast(eval_node_get_int_val(arg4)));

    if (data.dfield_get_len(que.que_node_get_val(arg1)) < len1 + len2 or
        data.dfield_get_len(que.que_node_get_val(arg2)) < len2)
    {
        ut_error();
    }

    ut_memcpy(@as([*]byte, @ptrCast(@constCast(str1))) + len1, @as([*]const byte, @ptrCast(str2)), len2);
}

fn eval_instr(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const arg2 = que.que_node_get_next(arg1).?;

    const dfield1 = que.que_node_get_val(arg1);
    const dfield2 = que.que_node_get_val(arg2);
    const str1 = data.dfield_get_data(dfield1).?;
    const str2 = data.dfield_get_data(dfield2).?;
    const len1 = data.dfield_get_len(dfield1);
    const len2 = data.dfield_get_len(dfield2);

    if (len2 == 0) {
        ut_error();
    }

    const match_char = @as([*]const byte, @ptrCast(str2))[0];
    var int_val: lint = 0;

    var i: ulint = 0;
    while (i < len1) : (i += 1) {
        if (@as([*]const byte, @ptrCast(str1))[i] == match_char) {
            if (i + len2 > len1) {
                break;
            }
            var j: ulint = 1;
            while (true) : (j += 1) {
                if (j == len2) {
                    int_val = @as(lint, @intCast(i + 1));
                    eval_node_set_int_val(&func_node.common, int_val);
                    return;
                }
                if (@as([*]const byte, @ptrCast(str1))[i + j] != @as([*]const byte, @ptrCast(str2))[j]) {
                    break;
                }
            }
        }
    }

    eval_node_set_int_val(&func_node.common, int_val);
}

fn eval_binary_to_number(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const dfield = que.que_node_get_val(arg1);
    const str1 = data.dfield_get_data(dfield).?;
    const len1 = data.dfield_get_len(dfield);
    if (len1 > 4) {
        ut_error();
    }

    var int_val: ulint = 0;
    var str2: [*]const byte = undefined;
    if (len1 == 4) {
        str2 = @as([*]const byte, @ptrCast(str1));
    } else {
        const tmp = @as([*]byte, @ptrCast(&int_val));
        ut_memcpy(tmp + (4 - len1), @as([*]const byte, @ptrCast(str1)), len1);
        str2 = tmp;
    }

    eval_node_copy_and_alloc_val(&func_node.common, str2, 4);
}

fn eval_concat(func_node: *func_node_t) void {
    var arg = func_node.args;
    var len: ulint = 0;
    while (arg) |node| {
        len += data.dfield_get_len(que.que_node_get_val(node));
        arg = que.que_node_get_next(node);
    }

    const data_ptr = eval_node_ensure_val_buf(&func_node.common, len);
    var offset: ulint = 0;
    arg = func_node.args;
    while (arg) |node| {
        const dfield = que.que_node_get_val(node);
        const len1 = data.dfield_get_len(dfield);
        ut_memcpy(data_ptr + offset, @as([*]const byte, @ptrCast(data.dfield_get_data(dfield).?)), len1);
        offset += len1;
        arg = que.que_node_get_next(node);
    }
}

fn eval_to_binary(func_node: *func_node_t) void {
    const arg1 = func_node.args.?;
    const str1 = data.dfield_get_data(que.que_node_get_val(arg1)).?;
    if (data.dtype_get_mtype(que.que_node_get_data_type(arg1)) != data.DATA_INT) {
        const len = data.dfield_get_len(que.que_node_get_val(arg1));
        data.dfield_set_data(que.que_node_get_val(&func_node.common), @as([*]const byte, @ptrCast(str1)), len);
        return;
    }

    const arg2 = que.que_node_get_next(arg1).?;
    const len1 = @as(ulint, @intCast(eval_node_get_int_val(arg2)));
    if (len1 > 4) {
        ut_error();
    }
    data.dfield_set_data(que.que_node_get_val(&func_node.common), @as([*]const byte, @ptrCast(str1)) + (4 - len1), len1);
}

fn parse_int_from_dfield(dfield: *const dfield_t) lint {
    const len = data.dfield_get_len(dfield);
    if (len == UNIV_SQL_NULL) {
        return 0;
    }
    const ptr = data.dfield_get_data(dfield) orelse return 0;
    const bytes = @as([*]const byte, @ptrCast(ptr))[0..@as(usize, @intCast(len))];
    var i: usize = 0;
    while (i < bytes.len and std.ascii.isWhitespace(bytes[i])) : (i += 1) {}
    var sign: i64 = 1;
    if (i < bytes.len and bytes[i] == '-') {
        sign = -1;
        i += 1;
    } else if (i < bytes.len and bytes[i] == '+') {
        i += 1;
    }
    var val: i64 = 0;
    while (i < bytes.len) : (i += 1) {
        const c = bytes[i];
        if (c == 0) break;
        if (c < '0' or c > '9') break;
        val = val * 10 + @as(i64, @intCast(c - '0'));
    }
    return @as(lint, @intCast(val * sign));
}

fn eval_predefined(func_node: *func_node_t) void {
    const func = func_node.func;
    const arg1 = func_node.args.?;

    if (func == pars.PARS_LENGTH_TOKEN) {
        const int_val = @as(lint, @intCast(data.dfield_get_len(que.que_node_get_val(arg1))));
        eval_node_set_int_val(&func_node.common, int_val);
        return;
    }

    if (func == pars.PARS_TO_CHAR_TOKEN) {
        const int_val = eval_node_get_int_val(arg1);
        var int_len: usize = 0;
        var uint_val: ulint = 0;

        if (int_val == 0) {
            int_len = 1;
        } else {
            if (int_val < 0) {
                uint_val = @as(ulint, @intCast(-int_val - 1)) + 1;
                int_len += 1;
            } else {
                uint_val = @as(ulint, @intCast(int_val));
            }
            var tmp = uint_val;
            while (tmp > 0) : (tmp /= 10) {
                int_len += 1;
            }
        }

        const buf = eval_node_ensure_val_buf(&func_node.common, @as(ulint, @intCast(int_len + 1)));
        buf[int_len] = 0;

        if (int_val == 0) {
            buf[0] = '0';
        } else {
            if (int_val < 0) {
                buf[0] = '-';
                uint_val = @as(ulint, @intCast(-int_val - 1)) + 1;
            } else {
                uint_val = @as(ulint, @intCast(int_val));
            }
            var tmp_idx = int_len;
            while (uint_val > 0) : (uint_val /= 10) {
                tmp_idx -= 1;
                buf[tmp_idx] = @as(byte, @intCast('0' + (uint_val % 10)));
            }
        }

        data.dfield_set_len(que.que_node_get_val(&func_node.common), @as(ulint, @intCast(int_len)));
        return;
    }

    if (func == pars.PARS_TO_NUMBER_TOKEN) {
        const int_val = parse_int_from_dfield(que.que_node_get_val(arg1));
        eval_node_set_int_val(&func_node.common, int_val);
        return;
    }

    if (func == pars.PARS_SYSDATE_TOKEN) {
        const int_val = @as(lint, @intCast(ut_time()));
        eval_node_set_int_val(&func_node.common, int_val);
        return;
    }

    eval_predefined_2(func_node);
}

pub fn eval_func(func_node: *func_node_t) void {
    ut_ad(que.que_node_get_type(&func_node.common) == que.QUE_NODE_FUNC);
    const class = func_node.class;
    const func = func_node.func;

    var arg = func_node.args;
    while (arg) |node| {
        eval_exp(node);
        if (data.dfield_is_null(que.que_node_get_val(node)) == compat.TRUE and
            class != pars.PARS_FUNC_CMP and
            func != pars.PARS_NOTFOUND_TOKEN and
            func != pars.PARS_PRINTF_TOKEN)
        {
            ut_error();
        }
        arg = que.que_node_get_next(node);
    }

    if (class == pars.PARS_FUNC_CMP) {
        _ = eval_cmp(func_node);
    } else if (class == pars.PARS_FUNC_ARITH) {
        eval_arith(func_node);
    } else if (class == pars.PARS_FUNC_AGGREGATE) {
        eval_aggregate(func_node);
    } else if (class == pars.PARS_FUNC_PREDEFINED) {
        if (func == pars.PARS_NOTFOUND_TOKEN) {
            eval_notfound(func_node);
        } else if (func == pars.PARS_SUBSTR_TOKEN) {
            eval_substr(func_node);
        } else if (func == pars.PARS_REPLSTR_TOKEN) {
            eval_replstr(func_node);
        } else if (func == pars.PARS_INSTR_TOKEN) {
            eval_instr(func_node);
        } else if (func == pars.PARS_BINARY_TO_NUMBER_TOKEN) {
            eval_binary_to_number(func_node);
        } else if (func == pars.PARS_CONCAT_TOKEN) {
            eval_concat(func_node);
        } else if (func == pars.PARS_TO_BINARY_TOKEN) {
            eval_to_binary(func_node);
        } else {
            eval_predefined(func_node);
        }
    } else {
        ut_ad(class == pars.PARS_FUNC_LOGICAL);
        eval_logical(func_node);
    }
}

pub fn if_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*if_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_IF);

    const parent = que.que_node_get_parent(&node.common);
    if (thr.prev_node == parent) {
        eval_exp(node.cond.?);
        if (eval_node_get_ibool_val(node.cond.?) == compat.TRUE) {
            thr.run_node = node.stat_list;
        } else if (node.else_part != null) {
            thr.run_node = node.else_part;
        } else if (node.elsif_list != null) {
            var elsif_node = node.elsif_list;
            while (elsif_node) |cur| {
                eval_exp(cur.cond.?);
                if (eval_node_get_ibool_val(cur.cond.?) == compat.TRUE) {
                    thr.run_node = cur.stat_list;
                    break;
                }
                const next = que.que_node_get_next(&cur.common);
                elsif_node = if (next) |ptr| @as(*elsif_node_t, @ptrCast(ptr)) else null;
                if (elsif_node == null) {
                    thr.run_node = null;
                    break;
                }
            }
        } else {
            thr.run_node = null;
        }
    } else {
        ut_ad(thr.prev_node != null);
        ut_ad(que.que_node_get_next(thr.prev_node.?) == null);
        thr.run_node = null;
    }

    if (thr.run_node == null) {
        thr.run_node = parent;
    }
    return thr;
}

pub fn while_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*while_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_WHILE);

    const parent = que.que_node_get_parent(&node.common);
    ut_ad(thr.prev_node == parent or (thr.prev_node != null and que.que_node_get_next(thr.prev_node.?) == null));

    eval_exp(node.cond.?);
    if (eval_node_get_ibool_val(node.cond.?) == compat.TRUE) {
        thr.run_node = node.stat_list;
    } else {
        thr.run_node = parent;
    }

    return thr;
}

pub fn assign_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*assign_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_ASSIGNMENT);

    eval_exp(node.val.?);
    eval_node_copy_val(&node.@"var".?.alias.?.common, node.val.?);
    thr.run_node = que.que_node_get_parent(&node.common);
    return thr;
}

pub fn for_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*for_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_FOR);

    const parent = que.que_node_get_parent(&node.common);
    var loop_var_value: lint = 0;

    if (thr.prev_node != parent) {
        ut_ad(thr.prev_node != null);
        thr.run_node = que.que_node_get_next(thr.prev_node.?);
        if (thr.run_node != null) {
            return thr;
        }
        loop_var_value = 1 + eval_node_get_int_val(&node.loop_var.?.common);
    } else {
        eval_exp(node.loop_start_limit.?);
        eval_exp(node.loop_end_limit.?);
        loop_var_value = eval_node_get_int_val(node.loop_start_limit.?);
        node.loop_end_value = eval_node_get_int_val(node.loop_end_limit.?);
    }

    if (loop_var_value > node.loop_end_value) {
        thr.run_node = parent;
    } else {
        eval_node_set_int_val(&node.loop_var.?.common, loop_var_value);
        thr.run_node = node.stat_list;
    }

    return thr;
}

pub fn exit_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*exit_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_EXIT);

    const loop_node = que.que_node_get_containing_loop_node(&node.common);
    ut_a(loop_node != null);
    thr.run_node = que.que_node_get_parent(loop_node.?);
    return thr;
}

pub fn return_step(thr: *que_thr_t) *que_thr_t {
    ut_ad(thr.run_node != null);
    const node = @as(*return_node_t, @ptrCast(thr.run_node.?));
    ut_ad(que.que_node_get_type(&node.common) == que.QUE_NODE_RETURN);

    var parent: *que_node_t = @ptrCast(&node.common);
    while (que.que_node_get_type(parent) != que.QUE_NODE_PROC) {
        parent = que.que_node_get_parent(parent).?;
    }
    ut_a(parent != null);
    thr.run_node = que.que_node_get_parent(parent);
    return thr;
}

fn set_int_type(node: *que_node_t) void {
    node.val.type = .{
        .mtype = data.DATA_INT,
        .prtype = 0,
        .len = 4,
        .mbminlen = 0,
        .mbmaxlen = 0,
    };
}

fn set_bool_node(node: *que_node_t, val: ibool) void {
    const buf = eval_node_alloc_val_buf(node, 1);
    mach.mach_write_to_1(buf, val);
}

test "eval arithmetic and logical operators" {
    var arg1 = sym_node_t{};
    var arg2 = sym_node_t{};
    set_int_type(&arg1.common);
    set_int_type(&arg2.common);
    eval_node_set_int_val(&arg1.common, 10);
    eval_node_set_int_val(&arg2.common, 3);
    defer eval_node_free_val_buf(&arg1.common);
    defer eval_node_free_val_buf(&arg2.common);

    arg1.common.brother = &arg2.common;
    var add = func_node_t{ .func = '+', .class = pars.PARS_FUNC_ARITH, .args = &arg1.common };
    eval_func(&add);
    defer eval_node_free_val_buf(&add.common);
    try std.testing.expectEqual(@as(lint, 13), eval_node_get_int_val(&add.common));

    var b1 = sym_node_t{};
    var b2 = sym_node_t{};
    set_bool_node(&b1.common, compat.TRUE);
    set_bool_node(&b2.common, compat.FALSE);
    defer eval_node_free_val_buf(&b1.common);
    defer eval_node_free_val_buf(&b2.common);

    b1.common.brother = &b2.common;
    var logical = func_node_t{ .func = pars.PARS_AND_TOKEN, .class = pars.PARS_FUNC_LOGICAL, .args = &b1.common };
    eval_func(&logical);
    defer eval_node_free_val_buf(&logical.common);
    try std.testing.expectEqual(@as(ibool, compat.FALSE), eval_node_get_ibool_val(&logical.common));
}

test "eval comparison and predefined length" {
    var arg1 = sym_node_t{};
    var arg2 = sym_node_t{};
    set_int_type(&arg1.common);
    set_int_type(&arg2.common);
    eval_node_set_int_val(&arg1.common, 5);
    eval_node_set_int_val(&arg2.common, 7);
    defer eval_node_free_val_buf(&arg1.common);
    defer eval_node_free_val_buf(&arg2.common);

    arg1.common.brother = &arg2.common;
    var cmp = func_node_t{ .func = '=', .class = pars.PARS_FUNC_CMP, .args = &arg1.common };
    eval_func(&cmp);
    defer eval_node_free_val_buf(&cmp.common);
    try std.testing.expectEqual(@as(ibool, compat.FALSE), eval_node_get_ibool_val(&cmp.common));

    var str_node = sym_node_t{};
    const text = "hello";
    data.dfield_set_data(que.que_node_get_val(&str_node.common), text, text.len);
    var len_fn = func_node_t{ .func = pars.PARS_LENGTH_TOKEN, .class = pars.PARS_FUNC_PREDEFINED, .args = &str_node.common };
    eval_func(&len_fn);
    defer eval_node_free_val_buf(&len_fn.common);
    try std.testing.expectEqual(@as(lint, 5), eval_node_get_int_val(&len_fn.common));
}

test "eval to_char and concat" {
    var int_node = sym_node_t{};
    set_int_type(&int_node.common);
    eval_node_set_int_val(&int_node.common, -12);
    defer eval_node_free_val_buf(&int_node.common);

    var to_char = func_node_t{ .func = pars.PARS_TO_CHAR_TOKEN, .class = pars.PARS_FUNC_PREDEFINED, .args = &int_node.common };
    eval_func(&to_char);
    defer eval_node_free_val_buf(&to_char.common);
    const dfield = que.que_node_get_val(&to_char.common);
    const len = data.dfield_get_len(dfield);
    const ptr = data.dfield_get_data(dfield).?;
    const slice = @as([*]const byte, @ptrCast(ptr))[0..@as(usize, @intCast(len))];
    try std.testing.expectEqualStrings("-12", slice);

    var s1 = sym_node_t{};
    var s2 = sym_node_t{};
    const a = "ab";
    const b = "cd";
    data.dfield_set_data(que.que_node_get_val(&s1.common), a, a.len);
    data.dfield_set_data(que.que_node_get_val(&s2.common), b, b.len);
    s1.common.brother = &s2.common;
    var concat = func_node_t{ .func = pars.PARS_CONCAT_TOKEN, .class = pars.PARS_FUNC_PREDEFINED, .args = &s1.common };
    eval_func(&concat);
    defer eval_node_free_val_buf(&concat.common);
    const out_dfield = que.que_node_get_val(&concat.common);
    const out_len = data.dfield_get_len(out_dfield);
    const out_ptr = data.dfield_get_data(out_dfield).?;
    const out = @as([*]const byte, @ptrCast(out_ptr))[0..@as(usize, @intCast(out_len))];
    try std.testing.expectEqualStrings("abcd", out);
}

test "eval procedure steps" {
    var parent_proc = pars.proc_node_t{};
    var cond = sym_node_t{};
    set_bool_node(&cond.common, compat.TRUE);
    defer eval_node_free_val_buf(&cond.common);

    var stmt = sym_node_t{};
    var if_node = if_node_t{
        .cond = &cond.common,
        .stat_list = &stmt.common,
    };
    if_node.common.parent = &parent_proc.common;

    var thr = que_thr_t{
        .run_node = &if_node.common,
        .prev_node = &parent_proc.common,
    };
    _ = if_step(&thr);
    try std.testing.expect(thr.run_node == &stmt.common);

    var while_node = while_node_t{
        .cond = &cond.common,
        .stat_list = &stmt.common,
    };
    while_node.common.parent = &parent_proc.common;
    thr = .{ .run_node = &while_node.common, .prev_node = &parent_proc.common };
    _ = while_step(&thr);
    try std.testing.expect(thr.run_node == &stmt.common);

    var dest = sym_node_t{};
    set_int_type(&dest.common);
    defer eval_node_free_val_buf(&dest.common);
    var var_node = sym_node_t{ .alias = &dest };

    var value = sym_node_t{};
    set_int_type(&value.common);
    eval_node_set_int_val(&value.common, 7);
    defer eval_node_free_val_buf(&value.common);

    var assign = assign_node_t{
        .@"var" = &var_node,
        .val = &value.common,
    };
    assign.common.parent = &parent_proc.common;
    thr = .{ .run_node = &assign.common, .prev_node = &parent_proc.common };
    _ = assign_step(&thr);
    try std.testing.expectEqual(@as(lint, 7), eval_node_get_int_val(&dest.common));

    var loop_var = sym_node_t{};
    set_int_type(&loop_var.common);
    defer eval_node_free_val_buf(&loop_var.common);
    var start = sym_node_t{};
    var end = sym_node_t{};
    set_int_type(&start.common);
    set_int_type(&end.common);
    eval_node_set_int_val(&start.common, 1);
    eval_node_set_int_val(&end.common, 2);
    defer eval_node_free_val_buf(&start.common);
    defer eval_node_free_val_buf(&end.common);

    var for_node = for_node_t{
        .loop_var = &loop_var,
        .loop_start_limit = &start.common,
        .loop_end_limit = &end.common,
        .stat_list = &stmt.common,
    };
    for_node.common.parent = &parent_proc.common;
    thr = .{ .run_node = &for_node.common, .prev_node = &parent_proc.common };
    _ = for_step(&thr);
    try std.testing.expect(thr.run_node == &stmt.common);
    try std.testing.expectEqual(@as(lint, 1), eval_node_get_int_val(&loop_var.common));

    var loop_parent = while_node_t{};
    loop_parent.common.parent = &parent_proc.common;
    var exit_node = exit_node_t{};
    exit_node.common.parent = &loop_parent.common;
    thr = .{ .run_node = &exit_node.common, .prev_node = &loop_parent.common };
    _ = exit_step(&thr);
    try std.testing.expect(thr.run_node == &parent_proc.common);

    var outer = pars.proc_node_t{};
    var ret = return_node_t{};
    ret.common.parent = &outer.common;
    thr = .{ .run_node = &ret.common, .prev_node = &outer.common };
    _ = return_step(&thr);
    try std.testing.expect(thr.run_node == null);
}

test "eval predicate from parsed graph" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const graph = try pars.core.pars_sql("a = 1 AND b <> 2", arena.allocator());
    const root = graph.thrs_head.?.child.?;
    const root_func: *func_node_t = @ptrCast(root);
    const left_func: *func_node_t = @ptrCast(root_func.args.?);
    const right_func: *func_node_t = @ptrCast(root_func.args.?.brother.?);

    const a_node: *sym_node_t = @ptrCast(left_func.args.?);
    const b_node: *sym_node_t = @ptrCast(right_func.args.?);

    eval_node_set_int_val(&a_node.common, 1);
    eval_node_set_int_val(&b_node.common, 3);
    try std.testing.expectEqual(@as(ibool, compat.TRUE), eval_node_get_bool_val(root));

    eval_node_set_int_val(&b_node.common, 2);
    try std.testing.expectEqual(@as(ibool, compat.FALSE), eval_node_get_bool_val(root));
}
