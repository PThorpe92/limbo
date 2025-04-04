use limbo_sqlite3_parser::ast::{self, CreateTableBody, Expr, FunctionTail, Literal};
use std::{rc::Rc, sync::Arc};

use crate::{
    schema::{self, Column, Schema, Type},
    types::{OwnedValue, OwnedValueType},
    LimboError, OpenFlags, Result, Statement, StepResult, SymbolTable, IO,
};

pub trait RoundToPrecision {
    fn round_to_precision(self, precision: i32) -> f64;
}

impl RoundToPrecision for f64 {
    fn round_to_precision(self, precision: i32) -> f64 {
        let factor = 10f64.powi(precision);
        (self * factor).round() / factor
    }
}

// https://sqlite.org/lang_keywords.html
const QUOTE_PAIRS: &[(char, char)] = &[('"', '"'), ('[', ']'), ('`', '`')];

pub fn normalize_ident(identifier: &str) -> String {
    let quote_pair = QUOTE_PAIRS
        .iter()
        .find(|&(start, end)| identifier.starts_with(*start) && identifier.ends_with(*end));

    if let Some(&(_, _)) = quote_pair {
        &identifier[1..identifier.len() - 1]
    } else {
        identifier
    }
    .to_lowercase()
}

pub const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";

pub fn parse_schema_rows(
    rows: Option<Statement>,
    schema: &mut Schema,
    io: Arc<dyn IO>,
    syms: &SymbolTable,
    mv_tx_id: Option<u64>,
) -> Result<()> {
    if let Some(mut rows) = rows {
        rows.set_mv_tx_id(mv_tx_id);
        let mut automatic_indexes = Vec::new();
        loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let ty = row.get::<&str>(0)?;
                    if !["table", "index"].contains(&ty) {
                        continue;
                    }
                    match ty {
                        "table" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            let sql: &str = row.get::<&str>(4)?;
                            if root_page == 0 && sql.to_lowercase().contains("create virtual") {
                                let name: &str = row.get::<&str>(1)?;
                                let vtab = syms.vtabs.get(name).unwrap().clone();
                                schema.add_virtual_table(vtab);
                            } else {
                                let table = schema::BTreeTable::from_sql(sql, root_page as usize)?;
                                schema.add_btree_table(Rc::new(table));
                            }
                        }
                        "index" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            match row.get::<&str>(4) {
                                Ok(sql) => {
                                    let index = schema::Index::from_sql(sql, root_page as usize)?;
                                    schema.add_index(Arc::new(index));
                                }
                                _ => {
                                    // Automatic index on primary key, e.g.
                                    // table|foo|foo|2|CREATE TABLE foo (a text PRIMARY KEY, b)
                                    // index|sqlite_autoindex_foo_1|foo|3|
                                    let index_name = row.get::<&str>(1)?;
                                    let table_name = row.get::<&str>(2)?;
                                    let root_page = row.get::<i64>(3)?;
                                    automatic_indexes.push((
                                        index_name.to_string(),
                                        table_name.to_string(),
                                        root_page,
                                    ));
                                }
                            }
                        }
                        _ => continue,
                    }
                }
                StepResult::IO => {
                    // TODO: How do we ensure that the I/O we submitted to
                    // read the schema is actually complete?
                    io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => break,
            }
        }
        for (index_name, table_name, root_page) in automatic_indexes {
            // We need to process these after all tables are loaded into memory due to the schema.get_table() call
            let table = schema.get_btree_table(&table_name).unwrap();
            let index =
                schema::Index::automatic_from_primary_key(&table, &index_name, root_page as usize)?;
            schema.add_index(Arc::new(index));
        }
    }
    Ok(())
}

fn cmp_numeric_strings(num_str: &str, other: &str) -> bool {
    match (num_str.parse::<f64>(), other.parse::<f64>()) {
        (Ok(num), Ok(other)) => num == other,
        _ => num_str == other,
    }
}

pub fn check_ident_equivalency(ident1: &str, ident2: &str) -> bool {
    fn strip_quotes(identifier: &str) -> &str {
        for &(start, end) in QUOTE_PAIRS {
            if identifier.starts_with(start) && identifier.ends_with(end) {
                return &identifier[1..identifier.len() - 1];
            }
        }
        identifier
    }
    strip_quotes(ident1).eq_ignore_ascii_case(strip_quotes(ident2))
}

pub fn check_literal_equivalency(lhs: &Literal, rhs: &Literal) -> bool {
    match (lhs, rhs) {
        (Literal::Numeric(n1), Literal::Numeric(n2)) => cmp_numeric_strings(n1, n2),
        (Literal::String(s1), Literal::String(s2)) => check_ident_equivalency(s1, s2),
        (Literal::Blob(b1), Literal::Blob(b2)) => b1 == b2,
        (Literal::Keyword(k1), Literal::Keyword(k2)) => check_ident_equivalency(k1, k2),
        (Literal::Null, Literal::Null) => true,
        (Literal::CurrentDate, Literal::CurrentDate) => true,
        (Literal::CurrentTime, Literal::CurrentTime) => true,
        (Literal::CurrentTimestamp, Literal::CurrentTimestamp) => true,
        _ => false,
    }
}

/// This function is used to determine whether two expressions are logically
/// equivalent in the context of queries, even if their representations
/// differ. e.g.: `SUM(x)` and `sum(x)`, `x + y` and `y + x`
///
/// *Note*: doesn't attempt to evaluate/compute "constexpr" results
pub fn exprs_are_equivalent(expr1: &Expr, expr2: &Expr) -> bool {
    match (expr1, expr2) {
        (
            Expr::Between {
                lhs: lhs1,
                not: not1,
                start: start1,
                end: end1,
            },
            Expr::Between {
                lhs: lhs2,
                not: not2,
                start: start2,
                end: end2,
            },
        ) => {
            not1 == not2
                && exprs_are_equivalent(lhs1, lhs2)
                && exprs_are_equivalent(start1, start2)
                && exprs_are_equivalent(end1, end2)
        }
        (Expr::Binary(lhs1, op1, rhs1), Expr::Binary(lhs2, op2, rhs2)) => {
            op1 == op2
                && ((exprs_are_equivalent(lhs1, lhs2) && exprs_are_equivalent(rhs1, rhs2))
                    || (op1.is_commutative()
                        && exprs_are_equivalent(lhs1, rhs2)
                        && exprs_are_equivalent(rhs1, lhs2)))
        }
        (
            Expr::Case {
                base: base1,
                when_then_pairs: pairs1,
                else_expr: else1,
            },
            Expr::Case {
                base: base2,
                when_then_pairs: pairs2,
                else_expr: else2,
            },
        ) => {
            base1 == base2
                && pairs1.len() == pairs2.len()
                && pairs1.iter().zip(pairs2).all(|((w1, t1), (w2, t2))| {
                    exprs_are_equivalent(w1, w2) && exprs_are_equivalent(t1, t2)
                })
                && else1 == else2
        }
        (
            Expr::Cast {
                expr: expr1,
                type_name: type1,
            },
            Expr::Cast {
                expr: expr2,
                type_name: type2,
            },
        ) => {
            exprs_are_equivalent(expr1, expr2)
                && match (type1, type2) {
                    (Some(t1), Some(t2)) => t1.name.eq_ignore_ascii_case(&t2.name),
                    _ => false,
                }
        }
        (Expr::Collate(expr1, collation1), Expr::Collate(expr2, collation2)) => {
            exprs_are_equivalent(expr1, expr2) && collation1.eq_ignore_ascii_case(collation2)
        }
        (
            Expr::FunctionCall {
                name: name1,
                distinctness: distinct1,
                args: args1,
                order_by: order1,
                filter_over: filter1,
            },
            Expr::FunctionCall {
                name: name2,
                distinctness: distinct2,
                args: args2,
                order_by: order2,
                filter_over: filter2,
            },
        ) => {
            name1.0.eq_ignore_ascii_case(&name2.0)
                && distinct1 == distinct2
                && args1 == args2
                && order1 == order2
                && filter1 == filter2
        }
        (
            Expr::FunctionCallStar {
                name: name1,
                filter_over: filter1,
            },
            Expr::FunctionCallStar {
                name: name2,
                filter_over: filter2,
            },
        ) => {
            name1.0.eq_ignore_ascii_case(&name2.0)
                && match (filter1, filter2) {
                    (None, None) => true,
                    (
                        Some(FunctionTail {
                            filter_clause: fc1,
                            over_clause: oc1,
                        }),
                        Some(FunctionTail {
                            filter_clause: fc2,
                            over_clause: oc2,
                        }),
                    ) => match ((fc1, fc2), (oc1, oc2)) {
                        ((Some(fc1), Some(fc2)), (Some(oc1), Some(oc2))) => {
                            exprs_are_equivalent(fc1, fc2) && oc1 == oc2
                        }
                        ((Some(fc1), Some(fc2)), _) => exprs_are_equivalent(fc1, fc2),
                        _ => false,
                    },
                    _ => false,
                }
        }
        (Expr::NotNull(expr1), Expr::NotNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::IsNull(expr1), Expr::IsNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::Literal(lit1), Expr::Literal(lit2)) => check_literal_equivalency(lit1, lit2),
        (Expr::Id(id1), Expr::Id(id2)) => check_ident_equivalency(&id1.0, &id2.0),
        (Expr::Unary(op1, expr1), Expr::Unary(op2, expr2)) => {
            op1 == op2 && exprs_are_equivalent(expr1, expr2)
        }
        (Expr::Variable(var1), Expr::Variable(var2)) => var1 == var2,
        (Expr::Parenthesized(exprs1), Expr::Parenthesized(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1
                    .iter()
                    .zip(exprs2)
                    .all(|(e1, e2)| exprs_are_equivalent(e1, e2))
        }
        (Expr::Parenthesized(exprs1), exprs2) | (exprs2, Expr::Parenthesized(exprs1)) => {
            exprs1.len() == 1 && exprs_are_equivalent(&exprs1[0], exprs2)
        }
        (Expr::Qualified(tn1, cn1), Expr::Qualified(tn2, cn2)) => {
            check_ident_equivalency(&tn1.0, &tn2.0) && check_ident_equivalency(&cn1.0, &cn2.0)
        }
        (Expr::DoublyQualified(sn1, tn1, cn1), Expr::DoublyQualified(sn2, tn2, cn2)) => {
            check_ident_equivalency(&sn1.0, &sn2.0)
                && check_ident_equivalency(&tn1.0, &tn2.0)
                && check_ident_equivalency(&cn1.0, &cn2.0)
        }
        (
            Expr::InList {
                lhs: lhs1,
                not: not1,
                rhs: rhs1,
            },
            Expr::InList {
                lhs: lhs2,
                not: not2,
                rhs: rhs2,
            },
        ) => {
            *not1 == *not2
                && exprs_are_equivalent(lhs1, lhs2)
                && rhs1
                    .as_ref()
                    .zip(rhs2.as_ref())
                    .map(|(list1, list2)| {
                        list1.len() == list2.len()
                            && list1
                                .iter()
                                .zip(list2)
                                .all(|(e1, e2)| exprs_are_equivalent(e1, e2))
                    })
                    .unwrap_or(false)
        }
        // fall back to naive equality check
        _ => expr1 == expr2,
    }
}

pub fn columns_from_create_table_body(body: &ast::CreateTableBody) -> crate::Result<Vec<Column>> {
    let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Err(crate::LimboError::ParseError(
            "CREATE TABLE body must contain columns and constraints".to_string(),
        ));
    };

    Ok(columns
        .into_iter()
        .filter_map(|(name, column_def)| {
            // if column_def.col_type includes HIDDEN, omit it for now
            if let Some(data_type) = column_def.col_type.as_ref() {
                if data_type.name.as_str().contains("HIDDEN") {
                    return None;
                }
            }
            let column = Column {
                name: Some(name.0.clone()),
                ty: match column_def.col_type {
                    Some(ref data_type) => {
                        // https://www.sqlite.org/datatype3.html
                        let type_name = data_type.name.as_str().to_uppercase();
                        if type_name.contains("INT") {
                            Type::Integer
                        } else if type_name.contains("CHAR")
                            || type_name.contains("CLOB")
                            || type_name.contains("TEXT")
                        {
                            Type::Text
                        } else if type_name.contains("BLOB") || type_name.is_empty() {
                            Type::Blob
                        } else if type_name.contains("REAL")
                            || type_name.contains("FLOA")
                            || type_name.contains("DOUB")
                        {
                            Type::Real
                        } else {
                            Type::Numeric
                        }
                    }
                    None => Type::Null,
                },
                default: column_def
                    .constraints
                    .iter()
                    .find_map(|c| match &c.constraint {
                        limbo_sqlite3_parser::ast::ColumnConstraint::Default(val) => {
                            Some(val.clone())
                        }
                        _ => None,
                    }),
                notnull: column_def.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        limbo_sqlite3_parser::ast::ColumnConstraint::NotNull { .. }
                    )
                }),
                ty_str: column_def
                    .col_type
                    .clone()
                    .map(|t| t.name.to_string())
                    .unwrap_or_default(),
                primary_key: column_def.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        limbo_sqlite3_parser::ast::ColumnConstraint::PrimaryKey { .. }
                    )
                }),
                is_rowid_alias: false,
            };
            Some(column)
        })
        .collect::<Vec<_>>())
}

#[derive(Debug, Default, PartialEq)]
pub struct OpenOptions<'a> {
    /// The authority component of the URI. may be 'localhost' or empty
    pub authority: Option<&'a str>,
    /// The normalized path to the database file
    pub path: String,
    /// The vfs query parameter causes the database connection to be opened using the VFS called NAME
    pub vfs: Option<String>,
    /// read-only, read-write, read-write and created if it does not exist, or pure in-memory database that never interacts with disk
    pub mode: OpenMode,
    /// Attempt to set the permissions of the new database file to match the existing file "filename".
    pub modeof: Option<String>,
    /// Specifies Cache mode shared | private
    pub cache: CacheMode,
    /// immutable=1|0 specifies that the database is stored on read-only media
    pub immutable: bool,
}

#[derive(Clone, Default, Debug, Copy, PartialEq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
    Memory,
    #[default]
    ReadWriteCreate,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum CacheMode {
    #[default]
    Private,
    Shared,
}

impl From<&str> for CacheMode {
    fn from(s: &str) -> Self {
        match s {
            "private" => CacheMode::Private,
            "shared" => CacheMode::Shared,
            _ => CacheMode::Private,
        }
    }
}

impl OpenMode {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "ro" => Ok(OpenMode::ReadOnly),
            "rw" => Ok(OpenMode::ReadWrite),
            "memory" => Ok(OpenMode::Memory),
            "rwc" => Ok(OpenMode::ReadWriteCreate),
            _ => Err(LimboError::InvalidArgument(format!(
                "Invalid mode: '{}'. Expected one of 'ro', 'rw', 'memory', 'rwc'",
                s
            ))),
        }
    }
    pub fn get_flags(&self) -> OpenFlags {
        match self {
            OpenMode::ReadWriteCreate => OpenFlags::Create,
            _ => OpenFlags::None,
        }
    }
}

fn is_windows_path(path: &str) -> bool {
    path.len() >= 3
        && path.chars().nth(1) == Some(':')
        && (path.chars().nth(2) == Some('/') || path.chars().nth(2) == Some('\\'))
}

/// converts windows-style paths to forward slashes, per SQLite spec.
fn normalize_windows_path(path: &str) -> String {
    let mut normalized = path.replace("\\", "/");

    // remove duplicate slashes (`//` → `/`)
    while normalized.contains("//") {
        normalized = normalized.replace("//", "/");
    }

    // if absolute windows path (`C:/...`), ensure it starts with `/`
    if normalized.len() >= 3
        && !normalized.starts_with('/')
        && normalized.chars().nth(1) == Some(':')
        && normalized.chars().nth(2) == Some('/')
    {
        normalized.insert(0, '/');
    }
    normalized
}

/// Parses a SQLite URI, handling Windows and Unix paths separately.
pub fn parse_sqlite_uri(uri: &str) -> Result<OpenOptions> {
    if !uri.starts_with("file:") {
        return Ok(OpenOptions {
            path: uri.to_string(),
            ..Default::default()
        });
    }

    let mut opts = OpenOptions::default();
    let without_scheme = &uri[5..];

    let (without_fragment, _) = without_scheme
        .split_once('#')
        .unwrap_or((without_scheme, ""));

    let (without_query, query) = without_fragment
        .split_once('?')
        .unwrap_or((without_fragment, ""));
    parse_query_params(query, &mut opts)?;

    // handle authority + path separately
    if let Some(after_slashes) = without_query.strip_prefix("//") {
        let (authority, path) = after_slashes.split_once('/').unwrap_or((after_slashes, ""));

        // sqlite allows only `localhost` or empty authority.
        if !(authority.is_empty() || authority == "localhost") {
            return Err(LimboError::InvalidArgument(format!(
                "Invalid authority '{}'. Only '' or 'localhost' allowed.",
                authority
            )));
        }
        opts.authority = if authority.is_empty() {
            None
        } else {
            Some(authority)
        };

        if is_windows_path(path) {
            opts.path = normalize_windows_path(&decode_percent(path));
        } else if !path.is_empty() {
            opts.path = format!("/{}", decode_percent(path));
        } else {
            opts.path = String::new();
        }
    } else {
        // no authority, must be a normal absolute or relative path.
        opts.path = decode_percent(without_query);
    }

    Ok(opts)
}

// parses query parameters and updates OpenOptions
fn parse_query_params(query: &str, opts: &mut OpenOptions) -> Result<()> {
    for param in query.split('&') {
        if let Some((key, value)) = param.split_once('=') {
            let decoded_value = decode_percent(value);
            match key {
                "mode" => opts.mode = OpenMode::from_str(value)?,
                "modeof" => opts.modeof = Some(decoded_value),
                "cache" => opts.cache = decoded_value.as_str().into(),
                "immutable" => opts.immutable = decoded_value == "1",
                "vfs" => opts.vfs = Some(decoded_value),
                _ => {}
            }
        }
    }
    Ok(())
}

/// Decodes percent-encoded characters
/// this function was adapted from the 'urlencoding' crate. MIT
pub fn decode_percent(uri: &str) -> String {
    let from_hex_digit = |digit: u8| -> Option<u8> {
        match digit {
            b'0'..=b'9' => Some(digit - b'0'),
            b'A'..=b'F' => Some(digit - b'A' + 10),
            b'a'..=b'f' => Some(digit - b'a' + 10),
            _ => None,
        }
    };

    let offset = uri.chars().take_while(|&c| c != '%').count();

    if offset >= uri.len() {
        return uri.to_string();
    }

    let mut decoded: Vec<u8> = Vec::with_capacity(uri.len());
    let (ascii, mut data) = uri.as_bytes().split_at(offset);
    decoded.extend_from_slice(ascii);

    loop {
        let mut parts = data.splitn(2, |&c| c == b'%');
        let non_escaped_part = parts.next().unwrap();
        let rest = parts.next();
        if rest.is_none() && decoded.is_empty() {
            return String::from_utf8_lossy(data).to_string();
        }
        decoded.extend_from_slice(non_escaped_part);
        match rest {
            Some(rest) => match rest.get(0..2) {
                Some([first, second]) => match from_hex_digit(*first) {
                    Some(first_val) => match from_hex_digit(*second) {
                        Some(second_val) => {
                            decoded.push((first_val << 4) | second_val);
                            data = &rest[2..];
                        }
                        None => {
                            decoded.extend_from_slice(&[b'%', *first]);
                            data = &rest[1..];
                        }
                    },
                    None => {
                        decoded.push(b'%');
                        data = rest;
                    }
                },
                _ => {
                    decoded.push(b'%');
                    decoded.extend_from_slice(rest);
                    break;
                }
            },
            None => break,
        }
    }
    String::from_utf8_lossy(&decoded).to_string()
}

/// When casting a TEXT value to INTEGER, the longest possible prefix of the value that can be interpreted as an integer number
/// is extracted from the TEXT value and the remainder ignored. Any leading spaces in the TEXT value when converting from TEXT to INTEGER are ignored.
/// If there is no prefix that can be interpreted as an integer number, the result of the conversion is 0.
/// If the prefix integer is greater than +9223372036854775807 then the result of the cast is exactly +9223372036854775807.
/// Similarly, if the prefix integer is less than -9223372036854775808 then the result of the cast is exactly -9223372036854775808.
/// When casting to INTEGER, if the text looks like a floating point value with an exponent, the exponent will be ignored
/// because it is no part of the integer prefix. For example, "CAST('123e+5' AS INTEGER)" results in 123, not in 12300000.
/// The CAST operator understands decimal integers only — conversion of hexadecimal integers stops at the "x" in the "0x" prefix of the hexadecimal integer string and thus result of the CAST is always zero.
pub fn cast_text_to_integer(text: &str) -> OwnedValue {
    let text = text.trim();
    if text.is_empty() {
        return OwnedValue::Integer(0);
    }
    if let Ok(i) = text.parse::<i64>() {
        return OwnedValue::Integer(i);
    }
    let bytes = text.as_bytes();
    let mut end = 0;
    if bytes[0] == b'-' {
        end = 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    text[..end]
        .parse::<i64>()
        .map_or(OwnedValue::Integer(0), OwnedValue::Integer)
}

/// When casting a TEXT value to REAL, the longest possible prefix of the value that can be interpreted
/// as a real number is extracted from the TEXT value and the remainder ignored. Any leading spaces in
/// the TEXT value are ignored when converging from TEXT to REAL.
/// If there is no prefix that can be interpreted as a real number, the result of the conversion is 0.0.
pub fn cast_text_to_real(text: &str) -> OwnedValue {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return OwnedValue::Float(0.0);
    }
    let Ok((_, text)) = parse_numeric_str(trimmed) else {
        return OwnedValue::Float(0.0);
    };
    text.parse::<f64>()
        .map_or(OwnedValue::Float(0.0), OwnedValue::Float)
}

/// NUMERIC Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
/// If the input text looks like an integer (there is no decimal point nor exponent) and the value
/// is small enough to fit in a 64-bit signed integer, then the result will be INTEGER.
/// Input text that looks like floating point (there is a decimal point and/or an exponent)
/// and the text describes a value that can be losslessly converted back and forth between IEEE 754
/// 64-bit float and a 51-bit signed integer, then the result is INTEGER. (In the previous sentence,
/// a 51-bit integer is specified since that is one bit less than the length of the mantissa of an
/// IEEE 754 64-bit float and thus provides a 1-bit of margin for the text-to-float conversion operation.)
/// Any text input that describes a value outside the range of a 64-bit signed integer yields a REAL result.
/// Casting a REAL or INTEGER value to NUMERIC is a no-op, even if a real value could be losslessly converted to an integer.
pub fn checked_cast_text_to_numeric(text: &str) -> std::result::Result<OwnedValue, ()> {
    // sqlite will parse the first N digits of a string to numeric value, then determine
    // whether _that_ value is more likely a real or integer value. e.g.
    // '-100234-2344.23e14' evaluates to -100234 instead of -100234.0
    let (kind, text) = parse_numeric_str(text)?;
    match kind {
        OwnedValueType::Integer => {
            match text.parse::<i64>() {
                Ok(i) => Ok(OwnedValue::Integer(i)),
                Err(e) => {
                    if matches!(
                        e.kind(),
                        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow
                    ) {
                        // if overflow, we return the representation as a real:
                        // we have to match sqlite exactly here, so we match sqlite3AtoF
                        let value = text.parse::<f64>().unwrap_or_default();
                        let factor = 10f64.powi(15 - value.abs().log10().ceil() as i32);
                        Ok(OwnedValue::Float((value * factor).round() / factor))
                    } else {
                        Err(())
                    }
                }
            }
        }
        OwnedValueType::Float => Ok(text
            .parse::<f64>()
            .map_or(OwnedValue::Float(0.0), OwnedValue::Float)),
        _ => unreachable!(),
    }
}

fn parse_numeric_str(text: &str) -> Result<(OwnedValueType, &str), ()> {
    let text = text.trim();
    let bytes = text.as_bytes();

    if bytes.is_empty()
        || bytes[0] == b'e'
        || bytes[0] == b'E'
        || (bytes[0] == b'.' && (bytes[1] == b'e' || bytes[1] == b'E'))
    {
        return Err(());
    }

    let mut end = 0;
    let mut has_decimal = false;
    let mut has_exponent = false;
    if bytes[0] == b'-' {
        end = 1;
    }
    while end < bytes.len() {
        match bytes[end] {
            b'0'..=b'9' => end += 1,
            b'.' if !has_decimal && !has_exponent => {
                has_decimal = true;
                end += 1;
            }
            b'e' | b'E' if !has_exponent => {
                has_exponent = true;
                end += 1;
                // allow exponent sign
                if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
                    end += 1;
                }
            }
            _ => break,
        }
    }
    if end == 0 || (end == 1 && bytes[0] == b'-') {
        return Err(());
    }
    // edge case: if it ends with exponent, strip and cast valid digits as float
    let last = bytes[end - 1];
    if last.eq_ignore_ascii_case(&b'e') {
        return Ok((OwnedValueType::Float, &text[0..end - 1]));
    // edge case: ends with extponent / sign
    } else if has_exponent && (last == b'-' || last == b'+') {
        return Ok((OwnedValueType::Float, &text[0..end - 2]));
    }
    Ok((
        if !has_decimal && !has_exponent {
            OwnedValueType::Integer
        } else {
            OwnedValueType::Float
        },
        &text[..end],
    ))
}

pub fn cast_text_to_numeric(txt: &str) -> OwnedValue {
    checked_cast_text_to_numeric(txt).unwrap_or(OwnedValue::Integer(0))
}

// Check if float can be losslessly converted to 51-bit integer
pub fn cast_real_to_integer(float: f64) -> std::result::Result<i64, ()> {
    let i = float as i64;
    if float == i as f64 && i.abs() < (1i64 << 51) {
        return Ok(i);
    }
    Err(())
}

// for TVF's we need these at planning time so we cannot emit translate_expr
pub fn vtable_args(args: &[ast::Expr]) -> Vec<limbo_ext::Value> {
    let mut vtable_args = Vec::new();
    for arg in args {
        match arg {
            Expr::Literal(lit) => match lit {
                Literal::Numeric(i) => {
                    if i.contains('.') {
                        vtable_args.push(limbo_ext::Value::from_float(i.parse().unwrap()));
                    } else {
                        vtable_args.push(limbo_ext::Value::from_integer(i.parse().unwrap()));
                    }
                }
                Literal::String(s) => {
                    vtable_args.push(limbo_ext::Value::from_text(s.clone()));
                }
                Literal::Blob(b) => {
                    vtable_args.push(limbo_ext::Value::from_blob(b.as_bytes().into()));
                }
                _ => {
                    vtable_args.push(limbo_ext::Value::null());
                }
            },
            _ => vtable_args.push(limbo_ext::Value::null()),
        }
    }
    vtable_args
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use limbo_sqlite3_parser::ast::{self, Expr, Id, Literal, Operator::*, Type};

    #[test]
    fn test_normalize_ident() {
        assert_eq!(normalize_ident("foo"), "foo");
        assert_eq!(normalize_ident("`foo`"), "foo");
        assert_eq!(normalize_ident("[foo]"), "foo");
        assert_eq!(normalize_ident("\"foo\""), "foo");
    }

    #[test]
    fn test_basic_addition_exprs_are_equivalent() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_addition_expressions_equivalent_normalized() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("243".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_subtraction_expressions_not_equivalent() {
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
        );
        assert!(!exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_subtraction_expressions_normalized() {
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_expressions_equivalent_case_insensitive_functioncalls() {
        let func1 = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        let func2 = Expr::FunctionCall {
            name: Id("sum".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(exprs_are_equivalent(&func1, &func2));

        let func3 = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(!exprs_are_equivalent(&func1, &func3));
    }

    #[test]
    fn test_expressions_equivalent_identical_fn_with_distinct() {
        let sum = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        let sum_distinct = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(!exprs_are_equivalent(&sum, &sum_distinct));
    }

    #[test]
    fn test_expressions_equivalent_multiplication() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("38".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("42".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_both_parenthesized_equivalent() {
        let expr1 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("799.0".to_string()))),
        )]);
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_parenthesized_equivalent() {
        let expr7 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        )]);
        let expr8 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr7, &expr8));
    }

    #[test]
    fn test_like_expressions_equivalent() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_equivalent_like_escaped() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("#".to_string())))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_equivalent_between() {
        let expr1 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        let expr2 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));

        // differing BETWEEN bounds
        let expr3 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("20".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr3));
    }
    #[test]
    fn test_cast_exprs_equivalent() {
        let cast1 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
        };

        let cast2 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "integer".to_string(),
                size: None,
            }),
        };
        assert!(exprs_are_equivalent(&cast1, &cast2));
    }

    #[test]
    fn test_ident_equivalency() {
        assert!(check_ident_equivalency("\"foo\"", "foo"));
        assert!(check_ident_equivalency("[foo]", "foo"));
        assert!(check_ident_equivalency("`FOO`", "foo"));
        assert!(check_ident_equivalency("\"foo\"", "`FOO`"));
        assert!(!check_ident_equivalency("\"foo\"", "[bar]"));
        assert!(!check_ident_equivalency("foo", "\"bar\""));
    }

    #[test]
    fn test_simple_uri() {
        let uri = "file:/home/user/db.sqlite";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.authority, None);
    }

    #[test]
    fn test_uri_with_authority() {
        let uri = "file://localhost/home/user/db.sqlite";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.authority, Some("localhost"));
    }

    #[test]
    fn test_uri_with_invalid_authority() {
        let uri = "file://example.com/home/user/db.sqlite";
        let result = parse_sqlite_uri(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_uri_with_query_params() {
        let uri = "file:/home/user/db.sqlite?vfs=unix&mode=ro&immutable=1";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
        assert_eq!(opts.mode, OpenMode::ReadOnly);
        assert_eq!(opts.immutable, true);
    }

    #[test]
    fn test_uri_with_fragment() {
        let uri = "file:/home/user/db.sqlite#section1";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_percent_encoding() {
        let uri = "file:/home/user/db%20with%20spaces.sqlite?vfs=unix";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db with spaces.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
    }

    #[test]
    fn test_uri_without_scheme() {
        let uri = "/home/user/db.sqlite";
        let result = parse_sqlite_uri(uri);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path, "/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_empty_query() {
        let uri = "file:/home/user/db.sqlite?";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_with_partial_query() {
        let uri = "file:/home/user/db.sqlite?mode=rw";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.mode, OpenMode::ReadWrite);
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_windows_style_path() {
        let uri = "file:///C:/Users/test/db.sqlite";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/C:/Users/test/db.sqlite");
    }

    #[test]
    fn test_uri_with_only_query_params() {
        let uri = "file:?mode=memory&cache=shared";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "");
        assert_eq!(opts.mode, OpenMode::Memory);
        assert_eq!(opts.cache, CacheMode::Shared);
    }

    #[test]
    fn test_uri_with_only_fragment() {
        let uri = "file:#fragment";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "");
    }

    #[test]
    fn test_uri_with_invalid_scheme() {
        let uri = "http:/home/user/db.sqlite";
        let result = parse_sqlite_uri(uri);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path, "http:/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_multiple_query_params() {
        let uri = "file:/home/user/db.sqlite?vfs=unix&mode=rw&cache=private&immutable=0";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
        assert_eq!(opts.mode, OpenMode::ReadWrite);
        assert_eq!(opts.cache, CacheMode::Private);
        assert_eq!(opts.immutable, false);
    }

    #[test]
    fn test_uri_with_unknown_query_param() {
        let uri = "file:/home/user/db.sqlite?unknown=param";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_with_multiple_equal_signs() {
        let uri = "file:/home/user/db.sqlite?vfs=unix=custom";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix=custom".to_string()));
    }

    #[test]
    fn test_uri_with_trailing_slash() {
        let uri = "file:/home/user/db.sqlite/";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite/");
    }

    #[test]
    fn test_uri_with_encoded_characters_in_query() {
        let uri = "file:/home/user/db.sqlite?vfs=unix%20mode";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix mode".to_string()));
    }

    #[test]
    fn test_uri_windows_network_path() {
        let uri = "file://server/share/db.sqlite";
        let result = parse_sqlite_uri(uri);
        assert!(result.is_err()); // non-localhost authority should fail
    }

    #[test]
    fn test_uri_windows_drive_letter_with_slash() {
        let uri = "file:///C:/database.sqlite";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/C:/database.sqlite");
    }

    #[test]
    fn test_localhost_with_double_slash_and_no_path() {
        let uri = "file://localhost";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "");
        assert_eq!(opts.authority, Some("localhost"));
    }

    #[test]
    fn test_uri_windows_drive_letter_without_slash() {
        let uri = "file:///C:/database.sqlite";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/C:/database.sqlite");
    }

    #[test]
    fn test_improper_mode() {
        // any other mode but ro, rwc, rw, memory should fail per sqlite

        let uri = "file:data.db?mode=readonly";
        let res = parse_sqlite_uri(uri);
        assert!(res.is_err());
        // including empty
        let uri = "file:/home/user/db.sqlite?vfs=&mode=";
        let res = parse_sqlite_uri(uri);
        assert!(res.is_err());
    }

    // Some examples from https://www.sqlite.org/c3ref/open.html#urifilenameexamples
    #[test]
    fn test_simple_file_current_dir() {
        let uri = "file:data.db";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "data.db");
        assert_eq!(opts.authority, None);
        assert_eq!(opts.vfs, None);
        assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
    }

    #[test]
    fn test_simple_file_three_slash() {
        let uri = "file:///home/data/data.db";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/data/data.db");
        assert_eq!(opts.authority, None);
        assert_eq!(opts.vfs, None);
        assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
    }

    #[test]
    fn test_simple_file_two_slash_localhost() {
        let uri = "file://localhost/home/fred/data.db";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/home/fred/data.db");
        assert_eq!(opts.authority, Some("localhost"));
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_windows_double_invalid() {
        let uri = "file://C:/home/fred/data.db?mode=ro";
        let opts = parse_sqlite_uri(uri);
        assert!(opts.is_err());
    }

    #[test]
    fn test_simple_file_two_slash() {
        let uri = "file:///C:/Documents%20and%20Settings/fred/Desktop/data.db";
        let opts = parse_sqlite_uri(uri).unwrap();
        assert_eq!(opts.path, "/C:/Documents and Settings/fred/Desktop/data.db");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_decode_percent_basic() {
        assert_eq!(decode_percent("hello%20world"), "hello world");
        assert_eq!(decode_percent("file%3Adata.db"), "file:data.db");
        assert_eq!(decode_percent("path%2Fto%2Ffile"), "path/to/file");
    }

    #[test]
    fn test_decode_percent_edge_cases() {
        assert_eq!(decode_percent(""), "");
        assert_eq!(decode_percent("plain_text"), "plain_text");
        assert_eq!(
            decode_percent("%2Fhome%2Fuser%2Fdb.sqlite"),
            "/home/user/db.sqlite"
        );
        // multiple percent-encoded characters in sequence
        assert_eq!(decode_percent("%41%42%43"), "ABC");
        assert_eq!(decode_percent("%61%62%63"), "abc");
    }

    #[test]
    fn test_decode_percent_invalid_sequences() {
        // invalid percent encoding (single % without two hex digits)
        assert_eq!(decode_percent("hello%"), "hello%");
        // only one hex digit after %
        assert_eq!(decode_percent("file%2"), "file%2");
        // invalid hex digits (not 0-9, A-F, a-f)
        assert_eq!(decode_percent("file%2X.db"), "file%2X.db");

        // Incomplete sequence at the end, leave untouched
        assert_eq!(decode_percent("path%2Fto%2"), "path/to%2");
    }

    #[test]
    fn test_decode_percent_mixed_valid_invalid() {
        assert_eq!(decode_percent("hello%20world%"), "hello world%");
        assert_eq!(decode_percent("%2Fpath%2Xto%2Ffile"), "/path%2Xto/file");
        assert_eq!(decode_percent("file%3Adata.db%2"), "file:data.db%2");
    }

    #[test]
    fn test_decode_percent_special_characters() {
        assert_eq!(
            decode_percent("%21%40%23%24%25%5E%26%2A%28%29"),
            "!@#$%^&*()"
        );
        assert_eq!(decode_percent("%5B%5D%7B%7D%7C%5C%3A"), "[]{}|\\:");
    }

    #[test]
    fn test_decode_percent_unmodified_valid_text() {
        // ensure already valid text remains unchanged
        assert_eq!(
            decode_percent("C:/Users/Example/Database.sqlite"),
            "C:/Users/Example/Database.sqlite"
        );
        assert_eq!(
            decode_percent("/home/user/db.sqlite"),
            "/home/user/db.sqlite"
        );
    }

    #[test]
    fn test_text_to_integer() {
        assert_eq!(cast_text_to_integer("1"), OwnedValue::Integer(1),);
        assert_eq!(cast_text_to_integer("-1"), OwnedValue::Integer(-1),);
        assert_eq!(
            cast_text_to_integer("1823400-00000"),
            OwnedValue::Integer(1823400),
        );
        assert_eq!(
            cast_text_to_integer("-10000000"),
            OwnedValue::Integer(-10000000),
        );
        assert_eq!(cast_text_to_integer("123xxx"), OwnedValue::Integer(123),);
        assert_eq!(
            cast_text_to_integer("9223372036854775807"),
            OwnedValue::Integer(i64::MAX),
        );
        assert_eq!(
            cast_text_to_integer("9223372036854775808"),
            OwnedValue::Integer(0),
        );
        assert_eq!(
            cast_text_to_integer("-9223372036854775808"),
            OwnedValue::Integer(i64::MIN),
        );
        assert_eq!(
            cast_text_to_integer("-9223372036854775809"),
            OwnedValue::Integer(0),
        );
        assert_eq!(cast_text_to_integer("-"), OwnedValue::Integer(0),);
    }

    #[test]
    fn test_text_to_real() {
        assert_eq!(cast_text_to_real("1"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_real("-1"), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_real("1.0"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_real("-1.0"), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_real("1e10"), OwnedValue::Float(1e10));
        assert_eq!(cast_text_to_real("-1e10"), OwnedValue::Float(-1e10));
        assert_eq!(cast_text_to_real("1e-10"), OwnedValue::Float(1e-10));
        assert_eq!(cast_text_to_real("-1e-10"), OwnedValue::Float(-1e-10));
        assert_eq!(cast_text_to_real("1.123e10"), OwnedValue::Float(1.123e10));
        assert_eq!(cast_text_to_real("-1.123e10"), OwnedValue::Float(-1.123e10));
        assert_eq!(cast_text_to_real("1.123e-10"), OwnedValue::Float(1.123e-10));
        assert_eq!(cast_text_to_real("-1.123-e-10"), OwnedValue::Float(-1.123));
        assert_eq!(cast_text_to_real("1-282584294928"), OwnedValue::Float(1.0));
        assert_eq!(
            cast_text_to_real("1.7976931348623157e309"),
            OwnedValue::Float(f64::INFINITY),
        );
        assert_eq!(
            cast_text_to_real("-1.7976931348623157e308"),
            OwnedValue::Float(f64::MIN),
        );
        assert_eq!(
            cast_text_to_real("-1.7976931348623157e309"),
            OwnedValue::Float(f64::NEG_INFINITY),
        );
        assert_eq!(cast_text_to_real("1E"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_real("1EE"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_real("-1E"), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_real("1."), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_real("-1."), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_real("1.23E"), OwnedValue::Float(1.23));
        assert_eq!(cast_text_to_real(".1.23E-"), OwnedValue::Float(0.1));
        assert_eq!(cast_text_to_real("0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_real("-0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_real("-0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_real("-0.0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_real("0.0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_real("-"), OwnedValue::Float(0.0));
    }

    #[test]
    fn test_text_to_numeric() {
        assert_eq!(cast_text_to_numeric("1"), OwnedValue::Integer(1));
        assert_eq!(cast_text_to_numeric("-1"), OwnedValue::Integer(-1));
        assert_eq!(
            cast_text_to_numeric("1823400-00000"),
            OwnedValue::Integer(1823400)
        );
        assert_eq!(
            cast_text_to_numeric("-10000000"),
            OwnedValue::Integer(-10000000)
        );
        assert_eq!(cast_text_to_numeric("123xxx"), OwnedValue::Integer(123));
        assert_eq!(
            cast_text_to_numeric("9223372036854775807"),
            OwnedValue::Integer(i64::MAX)
        );
        assert_eq!(
            cast_text_to_numeric("9223372036854775808"),
            OwnedValue::Float(9.22337203685478e18)
        ); // Exceeds i64, becomes float
        assert_eq!(
            cast_text_to_numeric("-9223372036854775808"),
            OwnedValue::Integer(i64::MIN)
        );
        assert_eq!(
            cast_text_to_numeric("-9223372036854775809"),
            OwnedValue::Float(-9.22337203685478e18)
        ); // Exceeds i64, becomes float

        assert_eq!(cast_text_to_numeric("1.0"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1.0"), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1e10"), OwnedValue::Float(1e10));
        assert_eq!(cast_text_to_numeric("-1e10"), OwnedValue::Float(-1e10));
        assert_eq!(cast_text_to_numeric("1e-10"), OwnedValue::Float(1e-10));
        assert_eq!(cast_text_to_numeric("-1e-10"), OwnedValue::Float(-1e-10));
        assert_eq!(
            cast_text_to_numeric("1.123e10"),
            OwnedValue::Float(1.123e10)
        );
        assert_eq!(
            cast_text_to_numeric("-1.123e10"),
            OwnedValue::Float(-1.123e10)
        );
        assert_eq!(
            cast_text_to_numeric("1.123e-10"),
            OwnedValue::Float(1.123e-10)
        );
        assert_eq!(
            cast_text_to_numeric("-1.123-e-10"),
            OwnedValue::Float(-1.123)
        );
        assert_eq!(
            cast_text_to_numeric("1-282584294928"),
            OwnedValue::Integer(1)
        );
        assert_eq!(cast_text_to_numeric("xxx"), OwnedValue::Integer(0));
        assert_eq!(
            cast_text_to_numeric("1.7976931348623157e309"),
            OwnedValue::Float(f64::INFINITY)
        );
        assert_eq!(
            cast_text_to_numeric("-1.7976931348623157e308"),
            OwnedValue::Float(f64::MIN)
        );
        assert_eq!(
            cast_text_to_numeric("-1.7976931348623157e309"),
            OwnedValue::Float(f64::NEG_INFINITY)
        );

        assert_eq!(cast_text_to_numeric("1E"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_numeric("1EE"), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1E"), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1."), OwnedValue::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1."), OwnedValue::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1.23E"), OwnedValue::Float(1.23));
        assert_eq!(cast_text_to_numeric("1.23E-"), OwnedValue::Float(1.23));

        assert_eq!(cast_text_to_numeric("0"), OwnedValue::Integer(0));
        assert_eq!(cast_text_to_numeric("-0"), OwnedValue::Integer(0));
        assert_eq!(cast_text_to_numeric("-0.0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_numeric("0.0"), OwnedValue::Float(0.0));
        assert_eq!(cast_text_to_numeric("-"), OwnedValue::Integer(0));
        assert_eq!(cast_text_to_numeric("-e"), OwnedValue::Integer(0));
        assert_eq!(cast_text_to_numeric("-E"), OwnedValue::Integer(0));
    }

    #[test]
    fn test_parse_numeric_str_valid_integer() {
        assert_eq!(
            parse_numeric_str("123"),
            Ok((OwnedValueType::Integer, "123"))
        );
        assert_eq!(
            parse_numeric_str("-456"),
            Ok((OwnedValueType::Integer, "-456"))
        );
        assert_eq!(
            parse_numeric_str("000789"),
            Ok((OwnedValueType::Integer, "000789"))
        );
    }

    #[test]
    fn test_parse_numeric_str_valid_float() {
        assert_eq!(
            parse_numeric_str("123.456"),
            Ok((OwnedValueType::Float, "123.456"))
        );
        assert_eq!(
            parse_numeric_str("-0.789"),
            Ok((OwnedValueType::Float, "-0.789"))
        );
        assert_eq!(
            parse_numeric_str("1e10"),
            Ok((OwnedValueType::Float, "1e10"))
        );
        assert_eq!(
            parse_numeric_str("-1.23e-4"),
            Ok((OwnedValueType::Float, "-1.23e-4"))
        );
        assert_eq!(
            parse_numeric_str("1.23E+4"),
            Ok((OwnedValueType::Float, "1.23E+4"))
        );
        assert_eq!(
            parse_numeric_str("1.2.3"),
            Ok((OwnedValueType::Float, "1.2"))
        )
    }

    #[test]
    fn test_parse_numeric_str_edge_cases() {
        assert_eq!(parse_numeric_str("1e"), Ok((OwnedValueType::Float, "1")));
        assert_eq!(parse_numeric_str("1e-"), Ok((OwnedValueType::Float, "1")));
        assert_eq!(parse_numeric_str("1e+"), Ok((OwnedValueType::Float, "1")));
        assert_eq!(parse_numeric_str("-1e"), Ok((OwnedValueType::Float, "-1")));
        assert_eq!(parse_numeric_str("-1e-"), Ok((OwnedValueType::Float, "-1")));
    }

    #[test]
    fn test_parse_numeric_str_invalid() {
        assert_eq!(parse_numeric_str(""), Err(()));
        assert_eq!(parse_numeric_str("abc"), Err(()));
        assert_eq!(parse_numeric_str("-"), Err(()));
        assert_eq!(parse_numeric_str("e10"), Err(()));
        assert_eq!(parse_numeric_str(".e10"), Err(()));
    }

    #[test]
    fn test_parse_numeric_str_with_whitespace() {
        assert_eq!(
            parse_numeric_str("   123"),
            Ok((OwnedValueType::Integer, "123"))
        );
        assert_eq!(
            parse_numeric_str("  -456.78  "),
            Ok((OwnedValueType::Float, "-456.78"))
        );
        assert_eq!(
            parse_numeric_str("  1.23e4  "),
            Ok((OwnedValueType::Float, "1.23e4"))
        );
    }

    #[test]
    fn test_parse_numeric_str_leading_zeros() {
        assert_eq!(
            parse_numeric_str("000123"),
            Ok((OwnedValueType::Integer, "000123"))
        );
        assert_eq!(
            parse_numeric_str("000.456"),
            Ok((OwnedValueType::Float, "000.456"))
        );
        assert_eq!(
            parse_numeric_str("0001e3"),
            Ok((OwnedValueType::Float, "0001e3"))
        );
    }

    #[test]
    fn test_parse_numeric_str_trailing_characters() {
        assert_eq!(
            parse_numeric_str("123abc"),
            Ok((OwnedValueType::Integer, "123"))
        );
        assert_eq!(
            parse_numeric_str("456.78xyz"),
            Ok((OwnedValueType::Float, "456.78"))
        );
        assert_eq!(
            parse_numeric_str("1.23e4extra"),
            Ok((OwnedValueType::Float, "1.23e4"))
        );
    }
}
