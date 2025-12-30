const std = @import("std");
const pars = @import("mod.zig");

pub const Token = struct {
    kind: i32,
    lexeme: []const u8,
    int_value: ?i64 = null,
    str_value: ?[]u8 = null,

    pub fn deinit(self: *Token, allocator: std.mem.Allocator) void {
        if (self.str_value) |buf| {
            allocator.free(buf);
            self.str_value = null;
        }
    }
};

pub const Lexer = struct {
    input: []const u8,
    pos: usize = 0,
    allocator: std.mem.Allocator,

    pub fn init(input: []const u8, allocator: std.mem.Allocator) Lexer {
        return .{ .input = input, .allocator = allocator };
    }

    pub fn nextToken(self: *Lexer) !Token {
        self.skipWhitespaceAndComments();
        if (self.pos >= self.input.len) {
            return .{ .kind = 0, .lexeme = "" };
        }

        const start = self.pos;
        const ch = self.input[self.pos];

        if (isDigit(ch)) {
            return self.lexNumber();
        }

        if (isIdentStart(ch)) {
            return self.lexIdentOrKeyword();
        }

        if (ch == ':' or ch == '$') {
            return self.lexBoundIdent();
        }

        if (ch == '\'') {
            return self.lexString();
        }

        if (self.match(">=")) {
            return .{ .kind = pars.PARS_GE_TOKEN, .lexeme = self.input[start..self.pos] };
        }
        if (self.match("<=")) {
            return .{ .kind = pars.PARS_LE_TOKEN, .lexeme = self.input[start..self.pos] };
        }
        if (self.match("<>")) {
            return .{ .kind = pars.PARS_NE_TOKEN, .lexeme = self.input[start..self.pos] };
        }
        if (self.match("!=")) {
            return .{ .kind = pars.PARS_NE_TOKEN, .lexeme = self.input[start..self.pos] };
        }

        self.pos += 1;
        return .{ .kind = @as(i32, ch), .lexeme = self.input[start..self.pos] };
    }

    fn lexNumber(self: *Lexer) !Token {
        const start = self.pos;
        while (self.pos < self.input.len and isDigit(self.input[self.pos])) {
            self.pos += 1;
        }
        const slice = self.input[start..self.pos];
        const value = try std.fmt.parseInt(i64, slice, 10);
        return .{ .kind = pars.PARS_INT_LIT, .lexeme = slice, .int_value = value };
    }

    fn lexIdentOrKeyword(self: *Lexer) Token {
        const start = self.pos;
        while (self.pos < self.input.len and isIdentContinue(self.input[self.pos])) {
            self.pos += 1;
        }
        const slice = self.input[start..self.pos];
        if (std.ascii.eqlIgnoreCase(slice, "AND")) {
            return .{ .kind = pars.PARS_AND_TOKEN, .lexeme = slice };
        }
        if (std.ascii.eqlIgnoreCase(slice, "OR")) {
            return .{ .kind = pars.PARS_OR_TOKEN, .lexeme = slice };
        }
        if (std.ascii.eqlIgnoreCase(slice, "NOT")) {
            return .{ .kind = pars.PARS_NOT_TOKEN, .lexeme = slice };
        }

        return .{ .kind = pars.PARS_ID_TOKEN, .lexeme = slice };
    }

    fn lexBoundIdent(self: *Lexer) Token {
        const start = self.pos;
        self.pos += 1;
        while (self.pos < self.input.len and isIdentContinue(self.input[self.pos])) {
            self.pos += 1;
        }
        return .{ .kind = pars.PARS_ID_TOKEN, .lexeme = self.input[start..self.pos] };
    }

    fn lexString(self: *Lexer) !Token {
        const start = self.pos;
        self.pos += 1;
        var out = std.ArrayList(u8).init(self.allocator);
        errdefer out.deinit();

        while (self.pos < self.input.len) {
            const ch = self.input[self.pos];
            if (ch == '\'') {
                if (self.pos + 1 < self.input.len and self.input[self.pos + 1] == '\'') {
                    try out.append('\'');
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                const buf = try out.toOwnedSlice();
                return .{ .kind = pars.PARS_STR_LIT, .lexeme = self.input[start..self.pos], .str_value = buf };
            }
            try out.append(ch);
            self.pos += 1;
        }

        const buf = try out.toOwnedSlice();
        return .{ .kind = pars.PARS_STR_LIT, .lexeme = self.input[start..self.pos], .str_value = buf };
    }

    fn skipWhitespaceAndComments(self: *Lexer) void {
        while (self.pos < self.input.len) {
            const ch = self.input[self.pos];
            if (isSpace(ch)) {
                self.pos += 1;
                continue;
            }
            if (self.match("--")) {
                while (self.pos < self.input.len and self.input[self.pos] != '\n') {
                    self.pos += 1;
                }
                continue;
            }
            if (self.match("/*")) {
                while (self.pos + 1 < self.input.len) {
                    if (self.input[self.pos] == '*' and self.input[self.pos + 1] == '/') {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    fn match(self: *Lexer, text: []const u8) bool {
        if (self.pos + text.len > self.input.len) {
            return false;
        }
        if (!std.mem.eql(u8, self.input[self.pos .. self.pos + text.len], text)) {
            return false;
        }
        self.pos += text.len;
        return true;
    }
};

fn isSpace(ch: u8) bool {
    return ch == ' ' or ch == '\n' or ch == '\t' or ch == '\r';
}

fn isDigit(ch: u8) bool {
    return ch >= '0' and ch <= '9';
}

fn isIdentStart(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or ch == '_';
}

fn isIdentContinue(ch: u8) bool {
    return isIdentStart(ch) or isDigit(ch);
}

test "lexer keywords and identifiers" {
    var lex = Lexer.init("and OR not foo", std.testing.allocator);
    var tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_AND_TOKEN, tok.kind);
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_OR_TOKEN, tok.kind);
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_NOT_TOKEN, tok.kind);
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_ID_TOKEN, tok.kind);
}

test "lexer integer and string" {
    var lex = Lexer.init("123 'hi''there'", std.testing.allocator);
    var tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_INT_LIT, tok.kind);
    try std.testing.expectEqual(@as(i64, 123), tok.int_value.?);
    tok = try lex.nextToken();
    defer tok.deinit(std.testing.allocator);
    try std.testing.expectEqual(pars.PARS_STR_LIT, tok.kind);
    try std.testing.expectEqualStrings("hi'there", tok.str_value.?);
}

test "lexer operators" {
    var lex = Lexer.init("a >= 10 <= b <> c != d", std.testing.allocator);
    _ = try lex.nextToken();
    var tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_GE_TOKEN, tok.kind);
    _ = try lex.nextToken();
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_LE_TOKEN, tok.kind);
    _ = try lex.nextToken();
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_NE_TOKEN, tok.kind);
    _ = try lex.nextToken();
    tok = try lex.nextToken();
    try std.testing.expectEqual(pars.PARS_NE_TOKEN, tok.kind);
}
