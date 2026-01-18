//! Noise expression AST and parser
//!
//! Parses Factorio's Lua noise expression strings into an AST that can be compiled.

use std::collections::HashMap;

/// AST node for noise expressions
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal number
    Const(f64),
    /// String literal (for var() arguments)
    StringLiteral(String),
    /// Variable reference (x, y, distance, map_seed, etc.)
    Var(String),
    /// Reference to another named expression
    ExprRef(String),
    /// Binary operation
    BinOp(Box<Expr>, BinOp, Box<Expr>),
    /// Unary operation
    UnaryOp(UnaryOp, Box<Expr>),
    /// Function call with named arguments
    FunctionCall {
        name: String,
        args: HashMap<String, Expr>,
    },
    /// Function call with positional arguments
    Call {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Pow,
    Mod,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// Token for lexer
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    String(String),
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Eq,
    Plus,
    Minus,
    Star,
    Slash,
    Caret,
    Percent,
    Lt,
    Le,
    Gt,
    Ge,
    EqEq,
    Ne,
    And,
    Or,
    Eof,
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() || c == '\\' {
                self.next_char();
                // Handle \z continuation
                if c == '\\' {
                    if let Some('z') = self.peek_char() {
                        self.next_char();
                    }
                }
            } else if c == '-' && self.input[self.pos..].starts_with("--") {
                // Skip Lua comment
                while let Some(c) = self.peek_char() {
                    if c == '\n' {
                        break;
                    }
                    self.next_char();
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let c = match self.peek_char() {
            Some(c) => c,
            None => return Token::Eof,
        };

        // Numbers
        if c.is_ascii_digit() || (c == '.' && self.input[self.pos + 1..].chars().next().map_or(false, |c| c.is_ascii_digit())) {
            return self.read_number();
        }

        // Identifiers
        if c.is_alphabetic() || c == '_' {
            return self.read_ident();
        }

        // Strings
        if c == '\'' || c == '"' {
            return self.read_string(c);
        }

        self.next_char();

        match c {
            '(' => Token::LParen,
            ')' => Token::RParen,
            '{' => Token::LBrace,
            '}' => Token::RBrace,
            ',' => Token::Comma,
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '^' => Token::Caret,
            '%' => Token::Percent,
            '=' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::EqEq
                } else {
                    Token::Eq
                }
            }
            '<' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '>' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '~' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ne
                } else {
                    Token::Ident("~".to_string())
                }
            }
            _ => Token::Ident(c.to_string()),
        }
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+' {
                // Handle negative exponent properly
                if (c == '-' || c == '+') && self.pos > start {
                    let prev = self.input[..self.pos].chars().last();
                    if prev != Some('e') && prev != Some('E') {
                        break;
                    }
                }
                self.next_char();
            } else {
                break;
            }
        }
        let s = &self.input[start..self.pos];
        Token::Number(s.parse().unwrap_or(0.0))
    }

    fn read_ident(&mut self) -> Token {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' || c == ':' {
                self.next_char();
            } else if c == '-' {
                // Only include '-' in identifier if followed by a letter (like "red-desert")
                // Not if followed by digit/space/operator (that's subtraction)
                let rest = &self.input[self.pos + 1..];
                let next = rest.chars().next();
                if next.map_or(false, |n| n.is_alphabetic()) {
                    self.next_char();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        let s = &self.input[start..self.pos];

        // Handle keywords
        match s {
            "and" => Token::And,
            "or" => Token::Or,
            _ => Token::Ident(s.to_string()),
        }
    }

    fn read_string(&mut self, quote: char) -> Token {
        self.next_char(); // consume opening quote
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c == quote {
                break;
            }
            self.next_char();
        }
        let s = &self.input[start..self.pos];
        self.next_char(); // consume closing quote
        Token::String(s.to_string())
    }
}

/// Parser for noise expressions
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token();
        Self { lexer, current }
    }

    fn advance(&mut self) {
        self.current = self.lexer.next_token();
    }

    fn expect(&mut self, expected: Token) -> bool {
        if std::mem::discriminant(&self.current) == std::mem::discriminant(&expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Parse the full expression
    pub fn parse(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;
        while self.current == Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_comparison()?;
        while self.current == Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_additive()?;
        loop {
            let op = match &self.current {
                Token::Lt => BinOp::Lt,
                Token::Le => BinOp::Le,
                Token::Gt => BinOp::Gt,
                Token::Ge => BinOp::Ge,
                Token::EqEq => BinOp::Eq,
                Token::Ne => BinOp::Ne,
                _ => break,
            };
            self.advance();
            let right = self.parse_additive()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match &self.current {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_power()?;
        loop {
            let op = match &self.current {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_power()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_power(&mut self) -> Result<Expr, String> {
        let base = self.parse_unary()?;
        if self.current == Token::Caret {
            self.advance();
            let exp = self.parse_power()?; // right associative
            Ok(Expr::BinOp(Box::new(base), BinOp::Pow, Box::new(exp)))
        } else {
            Ok(base)
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        match &self.current {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp(UnaryOp::Neg, Box::new(expr)))
            }
            Token::Ident(s) if s == "not" => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(expr)))
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match &self.current.clone() {
            Token::Number(n) => {
                let n = *n;
                self.advance();
                Ok(Expr::Const(n))
            }
            Token::String(s) => {
                // Preserve string literals for var() and seed arguments
                let s = s.clone();
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();

                // Check for function call with {} or ()
                if self.current == Token::LBrace {
                    // Named arguments: func{a = 1, b = 2}
                    self.advance();
                    let args = self.parse_named_args()?;
                    if !self.expect(Token::RBrace) {
                        return Err("Expected '}'".to_string());
                    }
                    Ok(Expr::FunctionCall { name, args })
                } else if self.current == Token::LParen {
                    // Positional arguments: func(a, b, c)
                    self.advance();
                    let args = self.parse_positional_args()?;
                    if !self.expect(Token::RParen) {
                        return Err("Expected ')'".to_string());
                    }
                    // Handle built-in functions
                    Ok(Expr::Call { name, args })
                } else {
                    // Variable or expression reference
                    if is_builtin_var(&name) {
                        Ok(Expr::Var(name))
                    } else {
                        Ok(Expr::ExprRef(name))
                    }
                }
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse()?;
                if !self.expect(Token::RParen) {
                    return Err("Expected ')'".to_string());
                }
                Ok(expr)
            }
            _ => Err(format!("Unexpected token: {:?}", self.current)),
        }
    }

    fn parse_named_args(&mut self) -> Result<HashMap<String, Expr>, String> {
        let mut args = HashMap::new();

        if self.current == Token::RBrace {
            return Ok(args);
        }

        loop {
            // Get parameter name
            let name = match &self.current {
                Token::Ident(s) => s.clone(),
                _ => return Err(format!("Expected identifier, got {:?}", self.current)),
            };
            self.advance();

            // Expect '='
            if !self.expect(Token::Eq) {
                return Err("Expected '='".to_string());
            }

            // Parse value
            let value = self.parse()?;
            args.insert(name, value);

            // Check for comma or end
            if self.current == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(args)
    }

    fn parse_positional_args(&mut self) -> Result<Vec<Expr>, String> {
        let mut args = Vec::new();

        if self.current == Token::RParen {
            return Ok(args);
        }

        loop {
            let value = self.parse()?;
            args.push(value);

            if self.current == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(args)
    }
}

fn is_builtin_var(name: &str) -> bool {
    matches!(name,
        "x" | "y" | "distance" | "map_seed" | "pi" | "inf" |
        "cliff_richness" | "cliff_elevation_interval" | "starting_area_radius"
    )
}

/// Compute a seed value from a string (matching Factorio's behavior)
pub fn string_seed(s: &str) -> i64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(s.as_bytes());
    hasher.finalize() as i64
}

/// Parse an expression string
pub fn parse_expression(input: &str) -> Result<Expr, String> {
    let mut parser = Parser::new(input);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_number() {
        let expr = parse_expression("42").unwrap();
        match expr {
            Expr::Const(n) => assert_eq!(n, 42.0),
            _ => panic!("Expected Const"),
        }
    }

    #[test]
    fn test_parse_var() {
        let expr = parse_expression("x").unwrap();
        match expr {
            Expr::Var(s) => assert_eq!(s, "x"),
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_parse_binop() {
        let expr = parse_expression("1 + 2 * 3").unwrap();
        // Should parse as 1 + (2 * 3)
        match expr {
            Expr::BinOp(_, BinOp::Add, _) => {}
            _ => panic!("Expected Add at top level"),
        }
    }

    #[test]
    fn test_parse_function_call() {
        let expr = parse_expression("clamp(x, 0, 1)").unwrap();
        match expr {
            Expr::Call { name, args } => {
                assert_eq!(name, "clamp");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Call"),
        }
    }

    #[test]
    fn test_parse_named_function() {
        let expr = parse_expression("basis_noise{x = x, y = y, seed0 = 123}").unwrap();
        match expr {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name, "basis_noise");
                assert!(args.contains_key("x"));
                assert!(args.contains_key("y"));
                assert!(args.contains_key("seed0"));
            }
            _ => panic!("Expected FunctionCall"),
        }
    }

    #[test]
    fn test_parse_ident_minus_number() {
        // This pattern appears from Lua \z continuation stripping whitespace
        let expr = parse_expression("moisture_noise- 0.08").unwrap();
        match expr {
            Expr::BinOp(left, BinOp::Sub, right) => {
                match *left {
                    Expr::ExprRef(name) => assert_eq!(name, "moisture_noise"),
                    _ => panic!("Expected ExprRef"),
                }
                match *right {
                    Expr::Const(n) => assert!((n - 0.08).abs() < 0.001),
                    _ => panic!("Expected Const"),
                }
            }
            _ => panic!("Expected BinOp Sub"),
        }
    }

    #[test]
    fn test_hyphenated_ident_with_letters() {
        // Hyphenated names followed by letters work (like "dry-dirt")
        let expr = parse_expression("dry-dirt").unwrap();
        match expr {
            Expr::ExprRef(name) => assert_eq!(name, "dry-dirt"),
            _ => panic!("Expected ExprRef"),
        }
    }

    #[test]
    fn test_hyphen_followed_by_digit_is_subtraction() {
        // "red-desert-3" parses as "red-desert - 3" (subtraction)
        // This is correct - tile names with numbers need quotes in Lua
        let expr = parse_expression("foo-3").unwrap();
        match expr {
            Expr::BinOp(left, BinOp::Sub, right) => {
                match *left {
                    Expr::ExprRef(name) => assert_eq!(name, "foo"),
                    _ => panic!("Expected ExprRef"),
                }
                match *right {
                    Expr::Const(n) => assert_eq!(n, 3.0),
                    _ => panic!("Expected Const"),
                }
            }
            _ => panic!("Expected BinOp Sub"),
        }
    }
}
