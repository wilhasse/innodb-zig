const compat = @import("../ut/compat.zig");
const que = @import("../que/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "pars";
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");

pub const ulint = compat.ulint;
pub const lint = compat.lint;

pub const PARS_FUNC_ARITH: ulint = 1;
pub const PARS_FUNC_LOGICAL: ulint = 2;
pub const PARS_FUNC_CMP: ulint = 3;
pub const PARS_FUNC_PREDEFINED: ulint = 4;
pub const PARS_FUNC_AGGREGATE: ulint = 5;

pub const PARS_INT_LIT: i32 = 258;
pub const PARS_STR_LIT: i32 = 260;
pub const PARS_ID_TOKEN: i32 = 264;
pub const PARS_AND_TOKEN: i32 = 265;
pub const PARS_OR_TOKEN: i32 = 266;
pub const PARS_NOT_TOKEN: i32 = 267;
pub const PARS_GE_TOKEN: i32 = 268;
pub const PARS_LE_TOKEN: i32 = 269;
pub const PARS_NE_TOKEN: i32 = 270;
pub const PARS_SUM_TOKEN: i32 = 291;
pub const PARS_COUNT_TOKEN: i32 = 292;
pub const PARS_NOTFOUND_TOKEN: i32 = 325;
pub const PARS_TO_CHAR_TOKEN: i32 = 326;
pub const PARS_TO_NUMBER_TOKEN: i32 = 327;
pub const PARS_TO_BINARY_TOKEN: i32 = 328;
pub const PARS_BINARY_TO_NUMBER_TOKEN: i32 = 329;
pub const PARS_SUBSTR_TOKEN: i32 = 330;
pub const PARS_REPLSTR_TOKEN: i32 = 331;
pub const PARS_CONCAT_TOKEN: i32 = 332;
pub const PARS_INSTR_TOKEN: i32 = 333;
pub const PARS_LENGTH_TOKEN: i32 = 334;
pub const PARS_SYSDATE_TOKEN: i32 = 335;
pub const PARS_PRINTF_TOKEN: i32 = 336;
pub const PARS_ASSERT_TOKEN: i32 = 337;
pub const PARS_RND_TOKEN: i32 = 338;
pub const PARS_RND_STR_TOKEN: i32 = 339;

pub const sym_tab_entry = enum(ulint) {
    SYM_VAR = 91,
    SYM_IMPLICIT_VAR,
    SYM_LIT,
    SYM_TABLE,
    SYM_COLUMN,
    SYM_CURSOR,
    SYM_PROCEDURE_NAME,
    SYM_INDEX,
    SYM_FUNCTION,
};

pub const sym_tab_t = struct {
    query_graph: ?*que.que_t = null,
};

pub const sym_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_SYMBOL },
    indirection: ?*sym_node_t = null,
    alias: ?*sym_node_t = null,
    token_type: sym_tab_entry = .SYM_VAR,
    sym_table: ?*sym_tab_t = null,
    cursor_def: ?*row.sel_node_t = null,
};

pub const func_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_FUNC },
    func: i32 = 0,
    class: ulint = 0,
    args: ?*que.que_node_t = null,
};

pub const proc_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_PROC },
};

pub const elsif_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_ELSIF },
    cond: ?*que.que_node_t = null,
    stat_list: ?*que.que_node_t = null,
};

pub const if_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_IF },
    cond: ?*que.que_node_t = null,
    stat_list: ?*que.que_node_t = null,
    else_part: ?*que.que_node_t = null,
    elsif_list: ?*elsif_node_t = null,
};

pub const while_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_WHILE },
    cond: ?*que.que_node_t = null,
    stat_list: ?*que.que_node_t = null,
};

pub const assign_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_ASSIGNMENT },
    @"var": ?*sym_node_t = null,
    val: ?*que.que_node_t = null,
};

pub const for_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_FOR },
    loop_var: ?*sym_node_t = null,
    loop_start_limit: ?*que.que_node_t = null,
    loop_end_limit: ?*que.que_node_t = null,
    loop_end_value: lint = 0,
    stat_list: ?*que.que_node_t = null,
};

pub const exit_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_EXIT },
};

pub const return_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_RETURN },
};
