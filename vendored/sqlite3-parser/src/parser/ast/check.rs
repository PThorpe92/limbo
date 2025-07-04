//! Check for additional syntax error
use crate::ast::*;
use crate::custom_err;
use std::fmt::{Display, Formatter};

impl Cmd {
    /// Statement accessor
    pub fn stmt(&self) -> &Stmt {
        match self {
            Self::Explain(stmt) => stmt,
            Self::ExplainQueryPlan(stmt) => stmt,
            Self::Stmt(stmt) => stmt,
        }
    }
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Explain(_) => ColumnCount::Fixed(8),
            Self::ExplainQueryPlan(_) => ColumnCount::Fixed(4),
            Self::Stmt(stmt) => stmt.column_count(),
        }
    }
    /// Like `sqlite3_stmt_isexplain`
    pub fn is_explain(&self) -> bool {
        matches!(self, Self::Explain(_) | Self::ExplainQueryPlan(_))
    }
    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        self.stmt().readonly()
    }
    /// check for extra rules
    pub fn check(&self) -> Result<(), ParserError> {
        self.stmt().check()
    }
}

/// Column count
pub enum ColumnCount {
    /// With `SELECT *` / PRAGMA
    Dynamic,
    /// Constant count
    Fixed(usize),
    /// No column
    None,
}

impl ColumnCount {
    fn incr(&mut self) {
        if let Self::Fixed(n) = self {
            *n += 1;
        }
    }
}

impl Stmt {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Delete(delete) => {
                let Delete { returning, .. } = &**delete;
                match returning {
                    Some(returning) => column_count(returning),
                    None => ColumnCount::None,
                }
            }
            Self::Insert(insert) => {
                let Insert { returning, .. } = &**insert;
                match returning {
                    Some(returning) => column_count(returning),
                    None => ColumnCount::None,
                }
            }
            Self::Pragma(..) => ColumnCount::Dynamic,
            Self::Select(s) => s.column_count(),
            Self::Update(update) => {
                let Update { returning, .. } = &**update;
                match returning {
                    Some(returning) => column_count(returning),
                    None => ColumnCount::None,
                }
            }
            _ => ColumnCount::None,
        }
    }

    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        match self {
            Self::Attach { .. } => true,
            Self::Begin(..) => true,
            Self::Commit(..) => true,
            Self::Detach(..) => true,
            Self::Pragma(..) => true, // TODO check all
            Self::Reindex { .. } => true,
            Self::Release(..) => true,
            Self::Rollback { .. } => true,
            Self::Savepoint(..) => true,
            Self::Select(..) => true,
            _ => false,
        }
    }

    /// check for extra rules
    pub fn check(&self) -> Result<(), ParserError> {
        match self {
            Self::AlterTable(alter_table) => {
                let (_, body) = &**alter_table;
                if let AlterTableBody::AddColumn(cd) = body {
                    for c in cd {
                        if let ColumnConstraint::PrimaryKey { .. } = c {
                            return Err(custom_err!("Cannot add a PRIMARY KEY column"));
                        }
                        if let ColumnConstraint::Unique(..) = c {
                            return Err(custom_err!("Cannot add a UNIQUE column"));
                        }
                    }
                }
                Ok(())
            }
            Self::CreateTable {
                temporary,
                tbl_name,
                body,
                ..
            } => {
                if *temporary {
                    if let Some(ref db_name) = tbl_name.db_name {
                        if db_name != "TEMP" {
                            return Err(custom_err!("temporary table name must be unqualified"));
                        }
                    }
                }
                body.check(tbl_name)
            }
            Self::CreateView {
                view_name,
                columns: Some(columns),
                select,
                ..
            } => {
                // SQLite3 engine renames duplicates:
                for (i, c) in columns.iter().enumerate() {
                    for o in &columns[i + 1..] {
                        if c.col_name == o.col_name {
                            return Err(custom_err!("duplicate column name: {}", c.col_name,));
                        }
                    }
                }
                // SQLite3 engine raises this error later (not while parsing):
                match select.column_count() {
                    ColumnCount::Fixed(n) if n != columns.len() => Err(custom_err!(
                        "expected {} columns for {} but got {}",
                        columns.len(),
                        view_name,
                        n
                    )),
                    _ => Ok(()),
                }
            }
            Self::Delete(delete) => {
                let Delete {
                    order_by, limit, ..
                } = &**delete;
                if order_by.is_some() && limit.is_none() {
                    return Err(custom_err!("ORDER BY without LIMIT on DELETE"));
                }
                Ok(())
            }
            Self::Insert(insert) => {
                let Insert { columns, body, .. } = &**insert;
                if columns.is_none() {
                    return Ok(());
                }
                let columns = columns.as_ref().unwrap();
                match body {
                    InsertBody::Select(select, ..) => match select.body.select.column_count() {
                        ColumnCount::Fixed(n) if n != columns.len() => {
                            Err(custom_err!("{} values for {} columns", n, columns.len()))
                        }
                        _ => Ok(()),
                    },
                    InsertBody::DefaultValues => {
                        Err(custom_err!("0 values for {} columns", columns.len()))
                    }
                }
            }
            Self::Update(update) => {
                let Update {
                    order_by, limit, ..
                } = &**update;
                if order_by.is_some() && limit.is_none() {
                    return Err(custom_err!("ORDER BY without LIMIT on UPDATE"));
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl CreateTableBody {
    /// check for extra rules
    pub fn check(&self, tbl_name: &QualifiedName) -> Result<(), ParserError> {
        if let Self::ColumnsAndConstraints {
            columns,
            constraints: _,
            options,
        } = self
        {
            let mut generated_count = 0;
            for c in columns.values() {
                if c.col_name == "rowid" {
                    return Err(custom_err!("cannot use reserved word: ROWID"));
                }
                for cs in &c.constraints {
                    if let ColumnConstraint::Generated { .. } = cs.constraint {
                        generated_count += 1;
                    }
                }
            }
            if generated_count == columns.len() {
                return Err(custom_err!("must have at least one non-generated column"));
            }

            if options.contains(TableOptions::STRICT) {
                for c in columns.values() {
                    match &c.col_type {
                        Some(Type { name, .. }) => {
                            // The datatype must be one of following: INT INTEGER REAL TEXT BLOB ANY
                            if !(name.eq_ignore_ascii_case("INT")
                                || name.eq_ignore_ascii_case("INTEGER")
                                || name.eq_ignore_ascii_case("REAL")
                                || name.eq_ignore_ascii_case("TEXT")
                                || name.eq_ignore_ascii_case("BLOB")
                                || name.eq_ignore_ascii_case("ANY"))
                            {
                                return Err(custom_err!(
                                    "unknown datatype for {}.{}: \"{}\"",
                                    tbl_name,
                                    c.col_name,
                                    name
                                ));
                            }
                        }
                        _ => {
                            // Every column definition must specify a datatype for that column. The freedom to specify a column without a datatype is removed.
                            return Err(custom_err!(
                                "missing datatype for {}.{}",
                                tbl_name,
                                c.col_name
                            ));
                        }
                    }
                }
            }
            if options.contains(TableOptions::WITHOUT_ROWID) && !self.has_primary_key() {
                return Err(custom_err!("PRIMARY KEY missing on table {}", tbl_name,));
            }
        }
        Ok(())
    }

    /// explicit primary key constraint ?
    pub fn has_primary_key(&self) -> bool {
        if let Self::ColumnsAndConstraints {
            columns,
            constraints,
            ..
        } = self
        {
            for col in columns.values() {
                for c in col {
                    if let ColumnConstraint::PrimaryKey { .. } = c {
                        return true;
                    }
                }
            }
            if let Some(constraints) = constraints {
                for c in constraints {
                    if let TableConstraint::PrimaryKey { .. } = c.constraint {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl<'a> IntoIterator for &'a ColumnDefinition {
    type Item = &'a ColumnConstraint;
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, NamedColumnConstraint>,
        fn(&'a NamedColumnConstraint) -> &'a ColumnConstraint,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.constraints.iter().map(|nc| &nc.constraint)
    }
}

impl Select {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        self.body.select.column_count()
    }
}

impl OneSelect {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Select(select) => {
                let SelectInner { columns, .. } = &**select;
                column_count(columns)
            }
            Self::Values(values) => {
                assert!(!values.is_empty()); // TODO Validate
                ColumnCount::Fixed(values[0].len())
            }
        }
    }
    /// Check all VALUES have the same number of terms
    pub fn push(values: &mut Vec<Vec<Expr>>, v: Vec<Expr>) -> Result<(), ParserError> {
        if values[0].len() != v.len() {
            return Err(custom_err!("all VALUES must have the same number of terms"));
        }
        values.push(v);
        Ok(())
    }
}

impl Display for QualifiedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.to_fmt(f)
    }
}

impl ResultColumn {
    fn column_count(&self) -> ColumnCount {
        match self {
            Self::Expr(..) => ColumnCount::Fixed(1),
            _ => ColumnCount::Dynamic,
        }
    }
}
fn column_count(cols: &[ResultColumn]) -> ColumnCount {
    assert!(!cols.is_empty());
    let mut count = ColumnCount::Fixed(0);
    for col in cols {
        match col.column_count() {
            ColumnCount::Fixed(_) => count.incr(),
            _ => return ColumnCount::Dynamic,
        }
    }
    count
}
