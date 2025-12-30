const compat = @import("../ut/compat.zig");
const que = @import("../que/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "pars";
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");
pub const opt = @import("opt.zig");
pub const core = @import("core.zig");

pub const ulint = compat.ulint;
pub const lint = compat.lint;

pub const PARS_FUNC_ARITH: ulint = 1;
pub const PARS_FUNC_LOGICAL: ulint = 2;
pub const PARS_FUNC_CMP: ulint = 3;
pub const PARS_FUNC_PREDEFINED: ulint = 4;
pub const PARS_FUNC_AGGREGATE: ulint = 5;
pub const PARS_FUNC_OTHER: ulint = 6;

pub const PARS_INT_LIT: i32 = 258;
pub const PARS_FLOAT_LIT: i32 = 259;
pub const PARS_STR_LIT: i32 = 260;
pub const PARS_FIXBINARY_LIT: i32 = 261;
pub const PARS_BLOB_LIT: i32 = 262;
pub const PARS_NULL_LIT: i32 = 263;
pub const PARS_ID_TOKEN: i32 = 264;
pub const PARS_AND_TOKEN: i32 = 265;
pub const PARS_OR_TOKEN: i32 = 266;
pub const PARS_NOT_TOKEN: i32 = 267;
pub const PARS_GE_TOKEN: i32 = 268;
pub const PARS_LE_TOKEN: i32 = 269;
pub const PARS_NE_TOKEN: i32 = 270;
pub const PARS_PROCEDURE_TOKEN: i32 = 271;
pub const PARS_IN_TOKEN: i32 = 272;
pub const PARS_OUT_TOKEN: i32 = 273;
pub const PARS_BINARY_TOKEN: i32 = 274;
pub const PARS_BLOB_TOKEN: i32 = 275;
pub const PARS_INT_TOKEN: i32 = 276;
pub const PARS_INTEGER_TOKEN: i32 = 277;
pub const PARS_FLOAT_TOKEN: i32 = 278;
pub const PARS_CHAR_TOKEN: i32 = 279;
pub const PARS_IS_TOKEN: i32 = 280;
pub const PARS_BEGIN_TOKEN: i32 = 281;
pub const PARS_END_TOKEN: i32 = 282;
pub const PARS_IF_TOKEN: i32 = 283;
pub const PARS_THEN_TOKEN: i32 = 284;
pub const PARS_ELSE_TOKEN: i32 = 285;
pub const PARS_ELSIF_TOKEN: i32 = 286;
pub const PARS_LOOP_TOKEN: i32 = 287;
pub const PARS_WHILE_TOKEN: i32 = 288;
pub const PARS_RETURN_TOKEN: i32 = 289;
pub const PARS_SELECT_TOKEN: i32 = 290;
pub const PARS_SUM_TOKEN: i32 = 291;
pub const PARS_COUNT_TOKEN: i32 = 292;
pub const PARS_DISTINCT_TOKEN: i32 = 293;
pub const PARS_FROM_TOKEN: i32 = 294;
pub const PARS_WHERE_TOKEN: i32 = 295;
pub const PARS_FOR_TOKEN: i32 = 296;
pub const PARS_DDOT_TOKEN: i32 = 297;
pub const PARS_READ_TOKEN: i32 = 298;
pub const PARS_ORDER_TOKEN: i32 = 299;
pub const PARS_BY_TOKEN: i32 = 300;
pub const PARS_ASC_TOKEN: i32 = 301;
pub const PARS_DESC_TOKEN: i32 = 302;
pub const PARS_INSERT_TOKEN: i32 = 303;
pub const PARS_INTO_TOKEN: i32 = 304;
pub const PARS_VALUES_TOKEN: i32 = 305;
pub const PARS_UPDATE_TOKEN: i32 = 306;
pub const PARS_SET_TOKEN: i32 = 307;
pub const PARS_DELETE_TOKEN: i32 = 308;
pub const PARS_CURRENT_TOKEN: i32 = 309;
pub const PARS_OF_TOKEN: i32 = 310;
pub const PARS_CREATE_TOKEN: i32 = 311;
pub const PARS_TABLE_TOKEN: i32 = 312;
pub const PARS_INDEX_TOKEN: i32 = 313;
pub const PARS_UNIQUE_TOKEN: i32 = 314;
pub const PARS_CLUSTERED_TOKEN: i32 = 315;
pub const PARS_DOES_NOT_FIT_IN_MEM_TOKEN: i32 = 316;
pub const PARS_ON_TOKEN: i32 = 317;
pub const PARS_ASSIGN_TOKEN: i32 = 318;
pub const PARS_DECLARE_TOKEN: i32 = 319;
pub const PARS_CURSOR_TOKEN: i32 = 320;
pub const PARS_SQL_TOKEN: i32 = 321;
pub const PARS_OPEN_TOKEN: i32 = 322;
pub const PARS_FETCH_TOKEN: i32 = 323;
pub const PARS_CLOSE_TOKEN: i32 = 324;
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
pub const PARS_ROW_PRINTF_TOKEN: i32 = 340;
pub const PARS_COMMIT_TOKEN: i32 = 341;
pub const PARS_ROLLBACK_TOKEN: i32 = 342;
pub const PARS_WORK_TOKEN: i32 = 343;
pub const PARS_UNSIGNED_TOKEN: i32 = 344;
pub const PARS_EXIT_TOKEN: i32 = 345;
pub const PARS_FUNCTION_TOKEN: i32 = 346;
pub const PARS_LOCK_TOKEN: i32 = 347;
pub const PARS_SHARE_TOKEN: i32 = 348;
pub const PARS_MODE_TOKEN: i32 = 349;

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

pub const pars_res_word_t = struct {
    code: i32 = 0,
};

pub const sym_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_SYMBOL },
    indirection: ?*sym_node_t = null,
    alias: ?*sym_node_t = null,
    token_type: sym_tab_entry = .SYM_VAR,
    sym_table: ?*sym_tab_t = null,
    cursor_def: ?*row.sel_node_t = null,
};

pub const sym_node_list_t = struct {
    head: ?*sym_node_t = null,
};

pub const func_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_FUNC },
    func: i32 = 0,
    class: ulint = 0,
    args: ?*que.que_node_t = null,
};

pub const order_node_t = struct {
    common: que.que_common_t = .{ .type = que.QUE_NODE_ORDER },
    column: ?*sym_node_t = null,
    asc: compat.ibool = compat.TRUE,
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
