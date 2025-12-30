const std = @import("std");
const compat = @import("../ut/compat.zig");
const dict = @import("../dict/mod.zig");
const page = @import("../page/mod.zig");
const pars = @import("mod.zig");
const que = @import("../que/mod.zig");
const row = @import("../row/mod.zig");

pub const OPT_EQUAL: compat.ulint = 1;
pub const OPT_COMPARISON: compat.ulint = 2;

pub const OPT_NOT_COND: compat.ulint = 1;
pub const OPT_END_COND: compat.ulint = 2;
pub const OPT_TEST_COND: compat.ulint = 3;
pub const OPT_SCROLL_COND: compat.ulint = 4;

fn ut_a(cond: bool) void {
    if (!cond) {
        @panic("ut_a");
    }
}

fn ut_error() noreturn {
    @panic("ut_error");
}

pub fn opt_search_plan(sel_node: *row.sel_node_t) void {
    _ = sel_node;
}

pub fn opt_find_all_cols(
    copy_val: compat.ibool,
    index: *dict.dict_index_t,
    col_list: *pars.sym_node_list_t,
    plan: ?*row.plan_t,
    exp: ?*que.que_node_t,
) void {
    _ = copy_val;
    _ = index;
    _ = col_list;
    _ = plan;
    _ = exp;
}

pub fn opt_print_query_plan(sel_node: *row.sel_node_t) void {
    _ = sel_node;
}

fn opt_invert_cmp_op(op: i32) i32 {
    return switch (op) {
        '<' => '>',
        '>' => '<',
        '=' => '=',
        pars.PARS_LE_TOKEN => pars.PARS_GE_TOKEN,
        pars.PARS_GE_TOKEN => pars.PARS_LE_TOKEN,
        else => ut_error(),
    };
}

pub fn opt_calc_n_fields_from_goodness(goodness: compat.ulint) compat.ulint {
    return ((goodness % 1024) + 2) / 4;
}

pub fn opt_op_to_search_mode(asc: compat.ibool, op: i32) compat.ulint {
    const is_asc = asc != 0;
    if (op == '=') {
        return if (is_asc) page.PAGE_CUR_GE else page.PAGE_CUR_LE;
    }
    if (op == '<') {
        ut_a(!is_asc);
        return page.PAGE_CUR_L;
    }
    if (op == '>') {
        ut_a(is_asc);
        return page.PAGE_CUR_G;
    }
    if (op == pars.PARS_GE_TOKEN) {
        ut_a(is_asc);
        return page.PAGE_CUR_GE;
    }
    if (op == pars.PARS_LE_TOKEN) {
        ut_a(!is_asc);
        return page.PAGE_CUR_LE;
    }
    ut_error();
}

test "opt invert comparison operator" {
    try std.testing.expectEqual(@as(i32, '>'), opt_invert_cmp_op('<'));
    try std.testing.expectEqual(@as(i32, '<'), opt_invert_cmp_op('>'));
    try std.testing.expectEqual(@as(i32, '='), opt_invert_cmp_op('='));
    try std.testing.expectEqual(pars.PARS_GE_TOKEN, opt_invert_cmp_op(pars.PARS_LE_TOKEN));
    try std.testing.expectEqual(pars.PARS_LE_TOKEN, opt_invert_cmp_op(pars.PARS_GE_TOKEN));
}

test "opt calc n fields from goodness" {
    try std.testing.expectEqual(@as(compat.ulint, 0), opt_calc_n_fields_from_goodness(0));
    try std.testing.expectEqual(@as(compat.ulint, 1), opt_calc_n_fields_from_goodness(4));
    try std.testing.expectEqual(@as(compat.ulint, 2), opt_calc_n_fields_from_goodness(6));
}

test "opt op to search mode" {
    try std.testing.expectEqual(page.PAGE_CUR_GE, opt_op_to_search_mode(compat.TRUE, '='));
    try std.testing.expectEqual(page.PAGE_CUR_LE, opt_op_to_search_mode(compat.FALSE, '='));
    try std.testing.expectEqual(page.PAGE_CUR_G, opt_op_to_search_mode(compat.TRUE, '>'));
    try std.testing.expectEqual(page.PAGE_CUR_L, opt_op_to_search_mode(compat.FALSE, '<'));
    try std.testing.expectEqual(page.PAGE_CUR_GE, opt_op_to_search_mode(compat.TRUE, pars.PARS_GE_TOKEN));
    try std.testing.expectEqual(page.PAGE_CUR_LE, opt_op_to_search_mode(compat.FALSE, pars.PARS_LE_TOKEN));
}
