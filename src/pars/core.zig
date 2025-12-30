const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const pars = @import("mod.zig");
const que = @import("../que/mod.zig");

pub const PARS_STAR_DENOTER: compat.ulint = 12345678;

pub var pars_print_lexed: compat.ibool = compat.FALSE;
pub var pars_sym_tab_global: ?*pars.sym_tab_t = null;

pub var pars_to_char_token = pars.pars_res_word_t{ .code = pars.PARS_TO_CHAR_TOKEN };
pub var pars_to_number_token = pars.pars_res_word_t{ .code = pars.PARS_TO_NUMBER_TOKEN };
pub var pars_to_binary_token = pars.pars_res_word_t{ .code = pars.PARS_TO_BINARY_TOKEN };
pub var pars_binary_to_number_token = pars.pars_res_word_t{ .code = pars.PARS_BINARY_TO_NUMBER_TOKEN };
pub var pars_substr_token = pars.pars_res_word_t{ .code = pars.PARS_SUBSTR_TOKEN };
pub var pars_replstr_token = pars.pars_res_word_t{ .code = pars.PARS_REPLSTR_TOKEN };
pub var pars_concat_token = pars.pars_res_word_t{ .code = pars.PARS_CONCAT_TOKEN };
pub var pars_instr_token = pars.pars_res_word_t{ .code = pars.PARS_INSTR_TOKEN };
pub var pars_length_token = pars.pars_res_word_t{ .code = pars.PARS_LENGTH_TOKEN };
pub var pars_sysdate_token = pars.pars_res_word_t{ .code = pars.PARS_SYSDATE_TOKEN };
pub var pars_printf_token = pars.pars_res_word_t{ .code = pars.PARS_PRINTF_TOKEN };
pub var pars_assert_token = pars.pars_res_word_t{ .code = pars.PARS_ASSERT_TOKEN };
pub var pars_rnd_token = pars.pars_res_word_t{ .code = pars.PARS_RND_TOKEN };
pub var pars_rnd_str_token = pars.pars_res_word_t{ .code = pars.PARS_RND_STR_TOKEN };
pub var pars_count_token = pars.pars_res_word_t{ .code = pars.PARS_COUNT_TOKEN };
pub var pars_sum_token = pars.pars_res_word_t{ .code = pars.PARS_SUM_TOKEN };
pub var pars_distinct_token = pars.pars_res_word_t{ .code = pars.PARS_DISTINCT_TOKEN };
pub var pars_binary_token = pars.pars_res_word_t{ .code = pars.PARS_BINARY_TOKEN };
pub var pars_blob_token = pars.pars_res_word_t{ .code = pars.PARS_BLOB_TOKEN };
pub var pars_int_token = pars.pars_res_word_t{ .code = pars.PARS_INT_TOKEN };
pub var pars_char_token = pars.pars_res_word_t{ .code = pars.PARS_CHAR_TOKEN };
pub var pars_float_token = pars.pars_res_word_t{ .code = pars.PARS_FLOAT_TOKEN };
pub var pars_update_token = pars.pars_res_word_t{ .code = pars.PARS_UPDATE_TOKEN };
pub var pars_asc_token = pars.pars_res_word_t{ .code = pars.PARS_ASC_TOKEN };
pub var pars_desc_token = pars.pars_res_word_t{ .code = pars.PARS_DESC_TOKEN };
pub var pars_open_token = pars.pars_res_word_t{ .code = pars.PARS_OPEN_TOKEN };
pub var pars_close_token = pars.pars_res_word_t{ .code = pars.PARS_CLOSE_TOKEN };
pub var pars_share_token = pars.pars_res_word_t{ .code = pars.PARS_SHARE_TOKEN };
pub var pars_unique_token = pars.pars_res_word_t{ .code = pars.PARS_UNIQUE_TOKEN };
pub var pars_clustered_token = pars.pars_res_word_t{ .code = pars.PARS_CLUSTERED_TOKEN };

pub var pars_star_denoter: compat.ulint = PARS_STAR_DENOTER;

fn ut_a(cond: bool) void {
    if (!cond) {
        @panic("ut_a");
    }
}

fn ut_error() noreturn {
    @panic("ut_error");
}

pub fn pars_var_init() void {
    pars_print_lexed = compat.FALSE;
    pars_sym_tab_global = null;

    ut_a(pars_to_char_token.code == pars.PARS_TO_CHAR_TOKEN);
    ut_a(pars_to_number_token.code == pars.PARS_TO_NUMBER_TOKEN);
    ut_a(pars_to_binary_token.code == pars.PARS_TO_BINARY_TOKEN);
    ut_a(pars_binary_to_number_token.code == pars.PARS_BINARY_TO_NUMBER_TOKEN);
    ut_a(pars_substr_token.code == pars.PARS_SUBSTR_TOKEN);
    ut_a(pars_replstr_token.code == pars.PARS_REPLSTR_TOKEN);
    ut_a(pars_concat_token.code == pars.PARS_CONCAT_TOKEN);
    ut_a(pars_instr_token.code == pars.PARS_INSTR_TOKEN);
    ut_a(pars_length_token.code == pars.PARS_LENGTH_TOKEN);
    ut_a(pars_sysdate_token.code == pars.PARS_SYSDATE_TOKEN);
    ut_a(pars_printf_token.code == pars.PARS_PRINTF_TOKEN);
    ut_a(pars_assert_token.code == pars.PARS_ASSERT_TOKEN);
    ut_a(pars_rnd_token.code == pars.PARS_RND_TOKEN);
    ut_a(pars_rnd_str_token.code == pars.PARS_RND_STR_TOKEN);
    ut_a(pars_count_token.code == pars.PARS_COUNT_TOKEN);
    ut_a(pars_sum_token.code == pars.PARS_SUM_TOKEN);
    ut_a(pars_distinct_token.code == pars.PARS_DISTINCT_TOKEN);
    ut_a(pars_binary_token.code == pars.PARS_BINARY_TOKEN);
    ut_a(pars_blob_token.code == pars.PARS_BLOB_TOKEN);
    ut_a(pars_int_token.code == pars.PARS_INT_TOKEN);
    ut_a(pars_char_token.code == pars.PARS_CHAR_TOKEN);
    ut_a(pars_float_token.code == pars.PARS_FLOAT_TOKEN);
    ut_a(pars_update_token.code == pars.PARS_UPDATE_TOKEN);
    ut_a(pars_asc_token.code == pars.PARS_ASC_TOKEN);
    ut_a(pars_desc_token.code == pars.PARS_DESC_TOKEN);
    ut_a(pars_open_token.code == pars.PARS_OPEN_TOKEN);
    ut_a(pars_close_token.code == pars.PARS_CLOSE_TOKEN);
    ut_a(pars_share_token.code == pars.PARS_SHARE_TOKEN);
    ut_a(pars_unique_token.code == pars.PARS_UNIQUE_TOKEN);
    ut_a(pars_clustered_token.code == pars.PARS_CLUSTERED_TOKEN);

    pars_star_denoter = PARS_STAR_DENOTER;
}

fn pars_func_get_class(func: i32) compat.ulint {
    return switch (func) {
        '+', '-', '*', '/' => pars.PARS_FUNC_ARITH,
        '=', '<', '>', pars.PARS_GE_TOKEN, pars.PARS_LE_TOKEN, pars.PARS_NE_TOKEN => pars.PARS_FUNC_CMP,
        pars.PARS_AND_TOKEN, pars.PARS_OR_TOKEN, pars.PARS_NOT_TOKEN => pars.PARS_FUNC_LOGICAL,
        pars.PARS_COUNT_TOKEN, pars.PARS_SUM_TOKEN => pars.PARS_FUNC_AGGREGATE,
        pars.PARS_TO_CHAR_TOKEN,
        pars.PARS_TO_NUMBER_TOKEN,
        pars.PARS_TO_BINARY_TOKEN,
        pars.PARS_BINARY_TO_NUMBER_TOKEN,
        pars.PARS_SUBSTR_TOKEN,
        pars.PARS_CONCAT_TOKEN,
        pars.PARS_LENGTH_TOKEN,
        pars.PARS_INSTR_TOKEN,
        pars.PARS_SYSDATE_TOKEN,
        pars.PARS_NOTFOUND_TOKEN,
        pars.PARS_PRINTF_TOKEN,
        pars.PARS_ASSERT_TOKEN,
        pars.PARS_RND_TOKEN,
        pars.PARS_RND_STR_TOKEN,
        pars.PARS_REPLSTR_TOKEN,
        => pars.PARS_FUNC_PREDEFINED,
        else => pars.PARS_FUNC_OTHER,
    };
}

fn pars_func_low(func: i32, arg: ?*que.que_node_t) *pars.func_node_t {
    const node = std.heap.page_allocator.create(pars.func_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_FUNC;
    data.dfield_set_data(&node.common.val, null, 0);
    node.common.val_buf_size = 0;
    node.func = func;
    node.class = pars_func_get_class(func);
    node.args = arg;
    return node;
}

pub fn pars_func(res_word: *const pars.pars_res_word_t, arg: ?*que.que_node_t) *pars.func_node_t {
    return pars_func_low(res_word.code, arg);
}

pub fn pars_op(func: i32, arg1: *que.que_node_t, arg2: ?*que.que_node_t) *pars.func_node_t {
    arg1.brother = null;
    if (arg2) |arg| {
        arg1.brother = arg;
        arg.brother = null;
    }
    return pars_func_low(func, arg1);
}

pub fn pars_order_by(column: *pars.sym_node_t, asc: *const pars.pars_res_word_t) *pars.order_node_t {
    const node = std.heap.page_allocator.create(pars.order_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_ORDER;
    node.column = column;
    if (asc == &pars_asc_token) {
        node.asc = compat.TRUE;
    } else {
        ut_a(asc == &pars_desc_token);
        node.asc = compat.FALSE;
    }
    return node;
}

test "pars var init sets star denoter" {
    pars_star_denoter = 0;
    pars_var_init();
    try std.testing.expectEqual(PARS_STAR_DENOTER, pars_star_denoter);
}

test "pars func class mapping" {
    try std.testing.expectEqual(pars.PARS_FUNC_ARITH, pars_func_get_class('+'));
    try std.testing.expectEqual(pars.PARS_FUNC_CMP, pars_func_get_class('='));
    try std.testing.expectEqual(pars.PARS_FUNC_LOGICAL, pars_func_get_class(pars.PARS_AND_TOKEN));
    try std.testing.expectEqual(pars.PARS_FUNC_AGGREGATE, pars_func_get_class(pars.PARS_SUM_TOKEN));
    try std.testing.expectEqual(pars.PARS_FUNC_PREDEFINED, pars_func_get_class(pars.PARS_TO_CHAR_TOKEN));
    try std.testing.expectEqual(pars.PARS_FUNC_OTHER, pars_func_get_class(999));
}

test "pars op builds arg list" {
    var arg1 = que.que_node_t{};
    var arg2 = que.que_node_t{};
    const node = pars_op('+', &arg1, &arg2);
    defer std.heap.page_allocator.destroy(node);
    try std.testing.expectEqual(@as(i32, '+'), node.func);
    try std.testing.expectEqual(pars.PARS_FUNC_ARITH, node.class);
    try std.testing.expect(node.args == &arg1);
    try std.testing.expect(arg1.brother == &arg2);
    try std.testing.expect(arg2.brother == null);
}

test "pars order by asc" {
    var column = pars.sym_node_t{};
    const node = pars_order_by(&column, &pars_asc_token);
    defer std.heap.page_allocator.destroy(node);
    try std.testing.expect(node.column == &column);
    try std.testing.expectEqual(compat.TRUE, node.asc);
}
