use crate::{types::OwnedValue, Connection, Statement, StepResult};
use limbo_ext::{Conn as ExtConn, ResultCode, Stmt, Value};
use std::{
    boxed::Box,
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    ptr,
    rc::Rc,
};

pub unsafe extern "C" fn connect(ctx: *mut c_void) -> *mut ExtConn {
    let conn = unsafe { &*(ctx as *const Rc<Connection>) };
    let ext_conn = ExtConn::new(ctx, prepare_stmt, close);
    Box::into_raw(Box::new(ext_conn)) as *mut ExtConn
}

pub unsafe extern "C" fn close(ctx: *mut c_void) {
    let conn = unsafe { &mut *(ctx as *mut Connection) };
    let _ = conn.close();
}

pub unsafe extern "C" fn prepare_stmt(
    ctx: *mut c_void,
    sql: *const c_char,
) -> *const Stmt<'static> {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ptr::null_mut(),
    };
    if ctx.is_null() {
        return ptr::null_mut();
    }
    let extcon = unsafe { &*(ctx as *const ExtConn) };
    let conn = unsafe { &*(extcon._ctx as *const Rc<Connection>) };
    let stmt = conn.prepare(&sql_str);
    Box::into_raw(Box::new(Stmt::new(
        extcon,
        Box::into_raw(Box::new(stmt)) as *mut c_void,
        stmt_bind_args_fn,
        stmt_step,
        stmt_get_row,
        stmt_get_column_names,
        stmt_get_col_count,
        stmt_close,
    ))) as *const Stmt<'static>
}

pub unsafe extern "C" fn stmt_bind_args_fn(ctx: *mut c_void, idx: i32, arg: Value) -> ResultCode {
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let stmt = Stmt::from_ptr(ctx);
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let Ok(owned_val) = OwnedValue::from_ffi(arg) else {
        return ResultCode::Error;
    };
    let Some(idx) = NonZeroUsize::new(idx as usize) else {
        return ResultCode::Error;
    };
    stmt_ctx.bind_at(idx, owned_val.to_value());
    ResultCode::OK
}

pub unsafe extern "C" fn stmt_step(stmt: *mut c_void) -> ResultCode {
    if stmt.is_null() {
        return ResultCode::Error;
    }
    let stmt = Stmt::from_ptr(stmt);
    let conn: &ExtConn = &*(stmt._conn);
    let conn: &Rc<Connection> = &Rc::from_raw(conn._ctx as *const Connection);
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    loop {
        match stmt_ctx.step() {
            Ok(StepResult::Row) => return ResultCode::Row,
            Ok(StepResult::Done) => return ResultCode::EOF,
            Ok(StepResult::IO) => {
                let _ = conn.pager.io.run_once();
                continue;
            }
            Ok(StepResult::Interrupt) => return ResultCode::Interrupt,
            Ok(StepResult::Busy) => return ResultCode::Busy,
            Err(err) => {
                let c_str = CString::new(err.to_string()).unwrap();
                stmt._err = c_str.into_raw();
                return ResultCode::Error;
            }
        }
    }
}

pub unsafe extern "C" fn stmt_get_row(ctx: *mut c_void) -> *const Value {
    if ctx.is_null() {
        return std::ptr::null() as *const Value;
    }
    let stmt = Stmt::from_ptr(ctx);
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    if let Some(row) = stmt_ctx.row() {
        let values = row.get_values();
        let mut owned_values = Vec::with_capacity(values.len());
        for value in values {
            owned_values.push(OwnedValue::to_ffi(value));
        }
        owned_values.as_ptr()
    } else {
        vec![Value::null()].as_ptr()
    }
}
pub unsafe extern "C" fn stmt_get_column_names(
    ctx: *mut c_void,
    count: *mut i32,
) -> *mut *mut c_char {
    if ctx.is_null() {
        *count = 0;
        return ptr::null_mut();
    }
    let stmt = Stmt::from_ptr(ctx);
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let num_cols = stmt_ctx.num_columns();
    if num_cols == 0 {
        *count = 0;
        return ptr::null_mut();
    }
    let mut c_names = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let name = stmt_ctx.get_column_name(i);
        let c_str = CString::new(name.as_bytes()).unwrap();
        c_names.push(c_str.into_raw());
    }
    *count = c_names.len() as i32;
    c_names.as_mut_ptr()
}

pub unsafe extern "C" fn stmt_close(ctx: *mut c_void) {
    let stmt = Stmt::from_ptr(ctx);
    let mut stmt: Box<Statement> = Box::from_raw(stmt._ctx as *mut Statement);
    stmt.reset()
}

pub unsafe extern "C" fn stmt_get_col_count(ctx: *mut c_void) -> i32 {
    let stmt = Stmt::from_ptr(ctx as *mut c_void);
    let stmt: &Statement = unsafe { &*(stmt._ctx as *const Statement) };
    stmt.num_columns() as i32
}
