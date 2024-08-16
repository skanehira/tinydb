use std::sync::Arc;

use crate::{
    query::{
        constant::Constant,
        create_index_data::CreateIndexData,
        create_table_data::CreateTableData,
        create_view_data::CreateViewData,
        delete_data::DeleteData,
        expression::Expression,
        insert_data::InsertData,
        modify_data::ModifyData,
        predicate::Predicate,
        query_data::QueryData,
        statement::{CreateStatement, Statement},
        term::Term,
    },
    record::schema::Schema,
};
use anyhow::{anyhow, bail, Result};

use super::lexer::{Lexer, Symbol, Token};

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Parser {
        let mut lexer = Lexer::new(input);
        lexer.next();
        Parser { lexer }
    }

    pub fn constant(&mut self) -> Result<Constant> {
        if self.lexer.is_string_constant() {
            Ok(Constant::String(self.lexer.eat_string_constant()?))
        } else {
            Ok(Constant::Int(self.lexer.eat_int_constant()?))
        }
    }

    pub fn expression(&mut self) -> Result<Expression> {
        if self.lexer.is_ident() {
            Ok(Expression::FieldName(self.lexer.eat_ident()?))
        } else {
            Ok(Expression::Value(self.constant()?))
        }
    }

    pub fn term(&mut self) -> Result<Term> {
        let lhs = self.expression()?;
        self.lexer.eat_symbol(Symbol::Equal)?;
        let rhs = self.expression()?;

        Ok(Term::new(lhs, rhs))
    }

    pub fn predicate(&mut self) -> Result<Predicate> {
        let mut pred = Predicate::new(self.term()?);
        if self.lexer.is_keyword("and") {
            self.lexer.eat_keyword("and")?;
            pred.con_join_with(&self.predicate()?);
        }

        Ok(pred)
    }

    pub fn get_select_list(&mut self) -> Result<Vec<String>> {
        let mut fields = vec![self.lexer.eat_ident()?];
        while self.lexer.is_symbol(Symbol::Comma) {
            self.lexer.next();
            fields.push(self.lexer.eat_ident()?);
        }
        Ok(fields)
    }

    pub fn get_table_list(&mut self) -> Result<Vec<String>> {
        let mut tables = vec![self.lexer.eat_ident()?];
        while self.lexer.is_symbol(Symbol::Comma) {
            self.lexer.next();
            tables.push(self.lexer.eat_ident()?);
        }
        Ok(tables)
    }

    pub fn get_field_list(&mut self) -> Result<Vec<String>> {
        let mut fields = vec![self.lexer.eat_ident()?];

        while self.lexer.is_symbol(Symbol::Comma) {
            self.lexer.next();
            fields.push(self.lexer.eat_ident()?);
        }

        Ok(fields)
    }

    pub fn get_constant_list(&mut self) -> Result<Vec<Constant>> {
        let mut values = vec![self.constant()?];

        while self.lexer.is_symbol(Symbol::Comma) {
            self.lexer.next();
            values.push(self.constant()?);
        }

        Ok(values)
    }

    pub fn query(&mut self) -> Result<QueryData> {
        self.lexer.eat_keyword("select")?;
        let fields = self.get_select_list()?;
        self.lexer.eat_keyword("from")?;
        let tables = self.get_table_list()?;

        let pred = if self.lexer.is_keyword("where") {
            self.lexer.eat_keyword("where")?;
            self.predicate()?
        } else {
            Predicate::default()
        };

        Ok(QueryData::new(fields, tables, pred))
    }

    pub fn update_cmd(&mut self) -> Result<Statement> {
        let Some(ref token) = self.lexer.current_token else {
            bail!("Expected a token, found None");
        };

        let stmt = match token {
            Token::Keyword(k) => match k.as_str() {
                "insert" => self.insert()?,
                "create" => self.create()?,
                "update" => self.modify()?,
                "delete" => self.delete()?,
                _ => bail!("Unknown keyword: {}", k),
            },
            _ => bail!("Expected a keyword, found {:?}", token),
        };

        Ok(stmt)
    }

    pub fn delete(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("delete")?;
        self.lexer.eat_keyword("from")?;
        let table_name = self.lexer.eat_ident()?;
        let pred = if self.lexer.is_keyword("where") {
            self.lexer.eat_keyword("where")?;
            self.predicate()?
        } else {
            Predicate::default()
        };

        Ok(Statement::Delete(DeleteData { table_name, pred }))
    }

    pub fn modify(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("update")?;
        let table_name = self.lexer.eat_ident()?;
        self.lexer.eat_keyword("set")?;
        let field_name = self.lexer.eat_ident()?;
        self.lexer.eat_symbol(Symbol::Equal)?;
        let new_value = self.expression()?;
        let pred = if self.lexer.is_keyword("where") {
            self.lexer.eat_keyword("where")?;
            self.predicate()?
        } else {
            Predicate::default()
        };

        let modfy_data = ModifyData {
            table_name,
            field_name,
            new_value,
            pred,
        };

        Ok(Statement::Update(modfy_data))
    }

    pub fn insert(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("insert")?;
        self.lexer.eat_keyword("into")?;
        let table_name = self.lexer.eat_ident()?;
        self.lexer.eat_symbol(Symbol::LParen)?;
        let fields = self.get_field_list()?;
        self.lexer.eat_symbol(Symbol::RParen)?;
        self.lexer.eat_keyword("values")?;
        self.lexer.eat_symbol(Symbol::LParen)?;
        let values = self.get_constant_list()?;
        self.lexer.eat_symbol(Symbol::RParen)?;

        Ok(Statement::Insert(InsertData {
            table_name,
            fields,
            values,
        }))
    }

    pub fn create(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("create")?;
        let token = self
            .lexer
            .current_token
            .as_ref()
            .ok_or(anyhow!("Expected a token, found None"))?;

        let stmt = match token {
            Token::Keyword(k) => match k.as_str() {
                "table" => self.create_table()?,
                "view" => self.create_view()?,
                "index" => self.create_index()?,
                _ => bail!("Unknown keyword: {}", k),
            },
            _ => bail!("Expected a keyword, found {:?}", token),
        };
        Ok(stmt)
    }

    pub fn create_index(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("index")?;
        let index_name = self.lexer.eat_ident()?;
        self.lexer.eat_keyword("on")?;
        let table_name = self.lexer.eat_ident()?;
        self.lexer.eat_symbol(Symbol::LParen)?;
        let field_name = self.lexer.eat_ident()?;
        self.lexer.eat_symbol(Symbol::RParen)?;

        let stmt = CreateIndexData {
            index_name,
            table_name,
            field_name,
        };
        Ok(Statement::Create(CreateStatement::CreateIndex(stmt)))
    }

    pub fn create_view(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("view")?;
        let view_name = self.lexer.eat_ident()?;
        self.lexer.eat_keyword("as")?;
        let query = self.query()?;
        let stmt = CreateViewData { view_name, query };
        Ok(Statement::Create(CreateStatement::CreateView(stmt)))
    }

    pub fn create_table(&mut self) -> Result<Statement> {
        self.lexer.eat_keyword("table")?;
        let table_name = self.lexer.eat_ident()?;
        self.lexer.eat_symbol(Symbol::LParen)?;
        let schema = self.field_defs()?;
        self.lexer.eat_symbol(Symbol::RParen)?;
        Ok(Statement::Create(CreateStatement::CreateTable(
            CreateTableData { table_name, schema },
        )))
    }

    fn field_defs(&mut self) -> Result<Schema> {
        let mut schema = Schema::default();
        loop {
            let sch = self.field_def()?;
            schema.add_all(Arc::new(sch))?;
            let Some(ref token) = self.lexer.current_token else {
                break;
            };
            if !token.is_symbol(&Symbol::Comma) {
                break;
            }
            self.lexer.next();
        }
        Ok(schema)
    }

    fn field_def(&mut self) -> Result<Schema> {
        let field_name = self.lexer.eat_ident()?;
        self.field_type(field_name)
    }

    fn field_type(&mut self, field_name: String) -> Result<Schema> {
        let Some(ref token) = self.lexer.current_token else {
            bail!("Expected a token, found None");
        };

        let mut schema = Schema::default();
        if token.is_keyword("int") {
            self.lexer.next();
            schema.add_int_field(field_name);
        } else {
            self.lexer.eat_keyword("varchar")?;
            self.lexer.eat_symbol(Symbol::LParen)?;
            let len = self.lexer.eat_int_constant()?;
            self.lexer.eat_symbol(Symbol::RParen)?;
            schema.add_string_field(field_name, len);
        }

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parse::parser::Parser,
        query::{
            constant::Constant, create_index_data::CreateIndexData, create_table_data::CreateTableData, create_view_data::CreateViewData, delete_data::DeleteData, expression::Expression, insert_data::InsertData, modify_data::ModifyData, predicate::Predicate, query_data::QueryData, statement::{CreateStatement, Statement}, term::Term
        },
        record::schema::Schema,
    };

    #[test]
    fn can_parse_select() {
        let query = "select name, age from people where age = 30";
        let mut parser = Parser::new(query);
        let query_data = parser.query().unwrap();
        assert_eq!(
            query_data,
            QueryData {
                fields: vec!["name".into(), "age".into()],
                tables: vec!["people".into()],
                pred: Predicate::new(Term::new(
                    Expression::FieldName("age".into()),
                    Expression::Value(Constant::Int(30)),
                )),
            }
        )
    }

    #[test]
    fn can_parse_create_table() {
        let query = "create table people (name varchar(255), age int)";
        let mut parser = Parser::new(query);
        let stmt = parser.create().unwrap();

        let create_table_data = match stmt {
            Statement::Create(CreateStatement::CreateTable(data)) => data,
            _ => panic!("Expected CreateTable"),
        };

        let mut schema = Schema::default();
        schema.add_string_field("name", 255);
        schema.add_int_field("age");

        assert_eq!(
            create_table_data,
            CreateTableData {
                table_name: "people".into(),
                schema
            }
        )
    }

    #[test]
    fn can_parse_create_view() {
        let query = "create view people_view as select name, age from people where age = 30";
        let mut parser = Parser::new(query);
        let stmt = parser.create().unwrap();

        let create_view_data = match stmt {
            Statement::Create(super::CreateStatement::CreateView(data)) => data,
            _ => panic!("Expected CreateView"),
        };

        let query_data = QueryData {
            fields: vec!["name".into(), "age".into()],
            tables: vec!["people".into()],
            pred: Predicate::new(Term::new(
                Expression::FieldName("age".into()),
                Expression::Value(Constant::Int(30)),
            )),
        };

        assert_eq!(
            create_view_data,
            CreateViewData {
                view_name: "people_view".into(),
                query: query_data
            }
        )
    }

    #[test]
    fn can_parse_create_index() {
        let query = "create index people_name_index on people (name)";
        let mut parser = Parser::new(query);
        let stmt = parser.create().unwrap();

        let create_index_data = match stmt {
            Statement::Create(super::CreateStatement::CreateIndex(data)) => data,
            _ => panic!("Expected CreateIndex"),
        };

        assert_eq!(
            create_index_data,
            CreateIndexData {
                index_name: "people_name_index".into(),
                table_name: "people".into(),
                field_name: "name".into()
            }
        )
    }

    #[test]
    fn can_parse_insert() {
        let query = "insert into people (name, age) values ('Alice', 30)";
        let mut parser = Parser::new(query);
        let stmt = parser.update_cmd().unwrap();

        let insert_data = match stmt {
            Statement::Insert(data) => data,
            _ => panic!("Expected Insert"),
        };

        assert_eq!(
            insert_data,
            InsertData {
                table_name: "people".into(),
                fields: vec!["name".into(), "age".into()],
                values: vec![Constant::String("Alice".into()), Constant::Int(30)]
            }
        )
    }

    #[test]
    fn can_parse_update() {
        let query = "update people set age = 31 where name = 'Alice'";
        let mut parser = Parser::new(query);
        let stmt = parser.update_cmd().unwrap();

        let modify_data = match stmt {
            Statement::Update(data) => data,
            _ => panic!("Expected Update"),
        };

        assert_eq!(
            modify_data,
            ModifyData {
                table_name: "people".into(),
                field_name: "age".into(),
                new_value: Expression::Value(Constant::Int(31)),
                pred: Predicate::new(Term::new(
                    Expression::FieldName("name".into()),
                    Expression::Value(Constant::String("Alice".into())),
                )),
            }
        )
    }

    #[test]
    fn can_parse_delete() {
        let query = "delete from people where name = 'Alice'";
        let mut parser = Parser::new(query);
        let stmt = parser.update_cmd().unwrap();

        let delete_data = match stmt {
            Statement::Delete(data) => data,
            _ => panic!("Expected Delete"),
        };

        assert_eq!(
            delete_data,
            DeleteData {
                table_name: "people".into(),
                pred: Predicate::new(Term::new(
                    Expression::FieldName("name".into()),
                    Expression::Value(Constant::String("Alice".into())),
                )),
            }
        )
    }
}
