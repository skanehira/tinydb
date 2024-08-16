use anyhow::{bail, Result};
use std::{iter::Peekable, str::Chars};

use crate::query::constant::Constant;

const KEYWORD: [&str; 18] = [
    "select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set",
    "create", "table", "int", "varchar", "view", "as", "index", "on",
];

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Symbol {
    Equal,
    Comma,
    Asterisk,
    LParen,
    RParen,
    Semicolon,
    Dot,
}

impl From<char> for Symbol {
    fn from(s: char) -> Self {
        match s {
            '=' => Symbol::Equal,
            ',' => Symbol::Comma,
            '*' => Symbol::Asterisk,
            '(' => Symbol::LParen,
            ')' => Symbol::RParen,
            ';' => Symbol::Semicolon,
            '.' => Symbol::Dot,
            _ => panic!("unexpected symbol: {}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Ident(String),
    Number(i32),
    Keyword(String),
    String(String),
    Symbol(Symbol),
}

impl Token {
    pub fn as_ident(&self) -> Result<String> {
        if let Token::Ident(ident) = self {
            return Ok(ident.clone());
        }
        bail!("Expected ident, found {:?}", self);
    }

    pub fn is_symbol(&self, symbol: &Symbol) -> bool {
        if matches!(self, Token::Symbol(_)) {
            if let Token::Symbol(s) = self {
                return *s == *symbol;
            }
        }
        false
    }

    pub fn is_keyword(&self, keyword: &str) -> bool {
        if matches!(self, Token::Keyword(_)) {
            if let Token::Keyword(k) = self {
                return k == keyword;
            }
        }
        false
    }

    pub fn as_constant(&self) -> Result<Constant> {
        match self {
            Token::Number(n) => Ok(Constant::Int(*n)),
            Token::String(s) => Ok(Constant::String(s.clone())),
            _ => bail!("Expected a constant, found {:?}", self),
        }
    }
}

pub struct Lexer<'a> {
    pub current_token: Option<Token>,
    pub peek_token: Option<Token>,
    input: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Lexer<'a> {
        let mut lexer = Lexer {
            current_token: None,
            peek_token: None,
            input: input.chars().peekable(),
        };
        lexer.next();
        lexer
    }

    pub fn peek(&self) -> Option<&Token> {
        self.peek_token.as_ref()
    }

    pub fn eat_ident(&mut self) -> Result<String> {
        let Some(ref token) = self.current_token else {
            bail!("Expected ident, found None");
        };

        let ident = match token {
            Token::Ident(ident) => ident.clone(),
            _ => bail!("Expected ident, found {:?}", token),
        };

        self.next();
        Ok(ident)
    }

    pub fn eat_symbol(&mut self, symbol: Symbol) -> Result<()> {
        let Some(ref token) = self.current_token else {
            bail!("Expected symbol '{:?}', found None", symbol);
        };

        if !token.is_symbol(&symbol) {
            bail!("Expected symbol '{:?}', found {:?}", symbol, token);
        }
        self.next();

        Ok(())
    }

    pub fn eat_keyword(&mut self, keyword: &str) -> Result<()> {
        if !self.is_keyword(keyword) {
            bail!(
                "Expected keyword '{}', found {:?}",
                keyword,
                self.current_token
            );
        }
        self.next();
        Ok(())
    }

    pub fn eat_int_constant(&mut self) -> Result<i32> {
        let Some(ref token) = self.current_token else {
            bail!("Expected int constant, found None");
        };

        let value = match token {
            Token::Number(n) => *n,
            _ => bail!("Expected int constant, found {:?}", token),
        };

        self.next();
        Ok(value)
    }

    pub fn eat_string_constant(&mut self) -> Result<String> {
        let Some(ref token) = self.current_token else {
            bail!("Expected string constant, found None");
        };

        let value = match token {
            Token::String(s) => s.clone(),
            _ => bail!("Expected string constant, found {:?}", token),
        };

        self.next();
        Ok(value)
    }

    pub fn is_symbol(&mut self, symbol: Symbol) -> bool {
        if let Some(ref token) = self.current_token {
            return token.is_symbol(&symbol);
        }
        false
    }

    pub fn is_ident(&self) -> bool {
        if let Some(ref token) = self.current_token {
            return matches!(token, Token::Ident(_));
        }
        false
    }

    pub fn is_keyword(&self, keyword: &str) -> bool {
        if let Some(ref token) = self.current_token {
            return token.is_keyword(keyword);
        }
        false
    }

    pub fn is_string_constant(&self) -> bool {
        if let Some(ref token) = self.current_token {
            return matches!(token, Token::String(_));
        }
        false
    }

    fn read_while<F>(&mut self, condition: F) -> String
    where
        F: Fn(char) -> bool,
    {
        let mut token = String::new();
        while let Some(&c) = self.input.peek() {
            if !condition(c) {
                break;
            }
            token.push(self.input.next().unwrap());
        }
        token
    }
}

fn is_symbol(c: char) -> bool {
    matches!(c, '=' | ',' | '*' | '(' | ')' | ';' | '.')
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(c) = self.input.next() {
            if c.is_whitespace() {
                continue;
            }

            let token = match c {
                c if c.is_numeric() => {
                    let mut token = c.to_string();
                    token.push_str(&self.read_while(|c| c.is_numeric()));
                    Token::Number(token.parse().unwrap())
                }
                '\'' => {
                    let token = self.read_while(|c| c != '\'');
                    self.input.next(); // skip closing '
                    Token::String(token)
                }
                c if is_symbol(c) => Token::Symbol(c.into()),
                _ => {
                    let mut token = c.to_string();
                    token.push_str(&self.read_while(|c| !c.is_whitespace() && !is_symbol(c)));

                    if KEYWORD.contains(&token.as_str()) {
                        Token::Keyword(token)
                    } else {
                        Token::Ident(token)
                    }
                }
            };

            self.current_token.clone_from(&self.peek_token);
            self.peek_token = Some(token.clone());
            return self.current_token.clone();
        }

        self.current_token.clone_from(&self.peek_token);
        self.peek_token = None;
        self.current_token.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::parse::lexer::{Lexer, Token};
    use paste::paste;

    macro_rules! test_lexer {
        ($name:ident, $input:expr, $wants:expr) => {
            paste! {
                #[test]
                fn [<should_can_lex _$name>]() {
                    let mut lexer = Lexer::new($input);
                    for want in $wants {
                        assert_eq!(lexer.next(), Some(want));
                    }
                    assert_eq!(lexer.next(), None);
                }
            }
        };
    }

    #[test]
    fn should_can_lex_numbers() {
        let input = "123 456";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::Number(123)));
        assert_eq!(lexer.next(), Some(Token::Number(456)));
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_single_string() {
        let input = "'hello world'";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::String("hello world".into())));
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_multiple_string() {
        let input = "'hello' 'world' 'foo' 'bar '";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::String("hello".into())));
        assert_eq!(lexer.next(), Some(Token::String("world".into())));
        assert_eq!(lexer.next(), Some(Token::String("foo".into())));
        assert_eq!(lexer.next(), Some(Token::String("bar ".into())));
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_string_and_number() {
        let input = "'hello' 123 'world' 456";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::String("hello".into())));
        assert_eq!(lexer.next(), Some(Token::Number(123)));
        assert_eq!(lexer.next(), Some(Token::String("world".into())));
        assert_eq!(lexer.next(), Some(Token::Number(456)));
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_ident() {
        let input = "hello world foo bar";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::Ident("hello".into())));
        assert_eq!(lexer.next(), Some(Token::Ident("world".into())));
        assert_eq!(lexer.next(), Some(Token::Ident("foo".into())));
        assert_eq!(lexer.next(), Some(Token::Ident("bar".into())));
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_keyword() {
        let input = "select from where and insert into values delete update set create table int varchar view as index on";
        let mut lexer = Lexer::new(input);
        let wants = vec![
            Token::Keyword("select".into()),
            Token::Keyword("from".into()),
            Token::Keyword("where".into()),
            Token::Keyword("and".into()),
            Token::Keyword("insert".into()),
            Token::Keyword("into".into()),
            Token::Keyword("values".into()),
            Token::Keyword("delete".into()),
            Token::Keyword("update".into()),
            Token::Keyword("set".into()),
            Token::Keyword("create".into()),
            Token::Keyword("table".into()),
            Token::Keyword("int".into()),
            Token::Keyword("varchar".into()),
            Token::Keyword("view".into()),
            Token::Keyword("as".into()),
            Token::Keyword("index".into()),
            Token::Keyword("on".into()),
        ];

        for want in wants {
            assert_eq!(lexer.next(), Some(want));
        }
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn should_can_lex_symbol() {
        let input = "= ,";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next(), Some(Token::Symbol('='.into())));
        assert_eq!(lexer.next(), Some(Token::Symbol(','.into())));
        assert_eq!(lexer.next(), None);
    }

    test_lexer!(
        select,
        "select * from users where id = 1",
        vec![
            Token::Keyword("select".into()),
            Token::Symbol('*'.into()),
            Token::Keyword("from".into()),
            Token::Ident("users".into()),
            Token::Keyword("where".into()),
            Token::Ident("id".into()),
            Token::Symbol('='.into()),
            Token::Number(1),
        ]
    );

    test_lexer!(
        insert,
        "insert into users (id, name) values (1, 'foo');",
        vec![
            Token::Keyword("insert".into()),
            Token::Keyword("into".into()),
            Token::Ident("users".into()),
            Token::Symbol('('.into()),
            Token::Ident("id".into()),
            Token::Symbol(','.into()),
            Token::Ident("name".into()),
            Token::Symbol(')'.into()),
            Token::Keyword("values".into()),
            Token::Symbol('('.into()),
            Token::Number(1),
            Token::Symbol(','.into()),
            Token::String("foo".into()),
            Token::Symbol(')'.into()),
            Token::Symbol(';'.into()),
        ]
    );

    test_lexer!(
        update,
        "update users set name = 'foo' where id = 1",
        vec![
            Token::Keyword("update".into()),
            Token::Ident("users".into()),
            Token::Keyword("set".into()),
            Token::Ident("name".into()),
            Token::Symbol('='.into()),
            Token::String("foo".into()),
            Token::Keyword("where".into()),
            Token::Ident("id".into()),
            Token::Symbol('='.into()),
            Token::Number(1),
        ]
    );

    test_lexer!(
        create_table,
        "create table users (id int, name varchar);",
        vec![
            Token::Keyword("create".into()),
            Token::Keyword("table".into()),
            Token::Ident("users".into()),
            Token::Symbol('('.into()),
            Token::Ident("id".into()),
            Token::Keyword("int".into()),
            Token::Symbol(','.into()),
            Token::Ident("name".into()),
            Token::Keyword("varchar".into()),
            Token::Symbol(')'.into()),
            Token::Symbol(';'.into()),
        ]
    );

    test_lexer!(
        create_index,
        "create index on users (id);",
        vec![
            Token::Keyword("create".into()),
            Token::Keyword("index".into()),
            Token::Keyword("on".into()),
            Token::Ident("users".into()),
            Token::Symbol('('.into()),
            Token::Ident("id".into()),
            Token::Symbol(')'.into()),
            Token::Symbol(';'.into()),
        ]
    );

    test_lexer!(
        delete,
        "delete from users where id = 1",
        vec![
            Token::Keyword("delete".into()),
            Token::Keyword("from".into()),
            Token::Ident("users".into()),
            Token::Keyword("where".into()),
            Token::Ident("id".into()),
            Token::Symbol('='.into()),
            Token::Number(1),
        ]
    );
}
