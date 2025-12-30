const std = @import("std");
const pars = @import("mod.zig");
const lexer = @import("lexer.zig");

pub const BinaryOp = enum {
    and_op,
    or_op,
    eq,
    ne,
    ge,
    le,
};

pub const Expr = union(enum) {
    ident: []const u8,
    int_lit: i64,
    str_lit: []const u8,
    binary: BinaryExpr,

    pub fn deinit(self: *Expr, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .binary => |*bin| {
                bin.left.deinit(allocator);
                bin.right.deinit(allocator);
                allocator.destroy(bin.left);
                allocator.destroy(bin.right);
            },
            .str_lit => |s| allocator.free(s),
            else => {},
        }
        self.* = .{ .ident = "" };
    }
};

pub const BinaryExpr = struct {
    op: BinaryOp,
    left: *Expr,
    right: *Expr,
};

pub const Parser = struct {
    lex: lexer.Lexer,
    current: lexer.Token,
    allocator: std.mem.Allocator,

    pub fn init(input: []const u8, allocator: std.mem.Allocator) !Parser {
        var lex = lexer.Lexer.init(input, allocator);
        const tok = try lex.nextToken();
        return .{ .lex = lex, .current = tok, .allocator = allocator };
    }

    pub fn deinit(self: *Parser) void {
        self.current.deinit(self.allocator);
    }

    pub fn parseExpression(self: *Parser) !*Expr {
        return self.parseOr();
    }

    fn parseOr(self: *Parser) !*Expr {
        var left = try self.parseAnd();
        while (self.current.kind == pars.PARS_OR_TOKEN) {
            try self.advance();
            const right = try self.parseAnd();
            left = try self.makeBinary(.or_op, left, right);
        }
        return left;
    }

    fn parseAnd(self: *Parser) !*Expr {
        var left = try self.parseComparison();
        while (self.current.kind == pars.PARS_AND_TOKEN) {
            try self.advance();
            const right = try self.parseComparison();
            left = try self.makeBinary(.and_op, left, right);
        }
        return left;
    }

    fn parseComparison(self: *Parser) !*Expr {
        var left = try self.parsePrimary();
        const kind = self.current.kind;
        if (kind == '=' or kind == pars.PARS_NE_TOKEN or kind == pars.PARS_GE_TOKEN or kind == pars.PARS_LE_TOKEN) {
            try self.advance();
            const right = try self.parsePrimary();
            const op: BinaryOp = switch (kind) {
                '=' => .eq,
                pars.PARS_NE_TOKEN => .ne,
                pars.PARS_GE_TOKEN => .ge,
                else => .le,
            };
            left = try self.makeBinary(op, left, right);
        }
        return left;
    }

    fn parsePrimary(self: *Parser) !*Expr {
        const tok = self.current;
        if (tok.kind == pars.PARS_ID_TOKEN) {
            try self.advance();
            return self.allocExpr(.{ .ident = tok.lexeme });
        }
        if (tok.kind == pars.PARS_INT_LIT) {
            try self.advance();
            return self.allocExpr(.{ .int_lit = tok.int_value orelse 0 });
        }
        if (tok.kind == pars.PARS_STR_LIT) {
            const str_val = tok.str_value orelse "";
            self.current.str_value = null;
            try self.advance();
            return self.allocExpr(.{ .str_lit = str_val });
        }
        if (tok.kind == '(') {
            try self.advance();
            const expr = try self.parseExpression();
            if (self.current.kind != ')') {
                return error.ParseError;
            }
            try self.advance();
            return expr;
        }
        return error.ParseError;
    }

    fn makeBinary(self: *Parser, op: BinaryOp, left: *Expr, right: *Expr) !*Expr {
        const node = try self.allocator.create(Expr);
        node.* = .{ .binary = .{ .op = op, .left = left, .right = right } };
        return node;
    }

    fn allocExpr(self: *Parser, value: Expr) !*Expr {
        const node = try self.allocator.create(Expr);
        node.* = value;
        return node;
    }

    fn advance(self: *Parser) !void {
        self.current.deinit(self.allocator);
        self.current = try self.lex.nextToken();
    }
};

test "parser simple expression" {
    var parser = try Parser.init("a = 1 AND b <> 'x'", std.testing.allocator);
    defer parser.deinit();
    const expr = try parser.parseExpression();
    defer expr.deinit(std.testing.allocator);
    try std.testing.expect(expr.* == .binary);
    try std.testing.expect(expr.binary.op == .and_op);
}

test "parser identifier only" {
    var parser = try Parser.init("foo", std.testing.allocator);
    defer parser.deinit();
    const expr = try parser.parseExpression();
    defer expr.deinit(std.testing.allocator);
    try std.testing.expect(expr.* == .ident);
    try std.testing.expectEqualStrings("foo", expr.ident);
}
