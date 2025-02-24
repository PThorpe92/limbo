use crate::{ExtensionApi, ResultCode, Value};
use std::ffi::{c_char, c_void, CStr, CString};

pub type ConnectFn = unsafe extern "C" fn(ctx: *mut c_void) -> *const Connection;
pub type PrepareStmtFn =
    unsafe extern "C" fn(api: *const c_void, sql: *const c_char) -> *mut c_void;
pub type GetResultColumnCountFn = unsafe extern "C" fn(ctx: *mut c_void) -> i32;
pub type GetColumnNamesFn =
    unsafe extern "C" fn(ctx: *mut c_void, count: *mut i32) -> *mut *mut c_char;
pub type BindArgsFn = unsafe extern "C" fn(ctx: *mut c_void, idx: i32, arg: Value) -> ResultCode;
pub type StmtStepFn = unsafe extern "C" fn(ctx: *mut c_void) -> ResultCode;
pub type StmtGetRowValuesFn = unsafe extern "C" fn(ctx: *mut c_void) -> *const Value;
pub type CloseConnectionFn = unsafe extern "C" fn(ctx: *mut c_void);
pub type CloseStmtFn = unsafe extern "C" fn(ctx: *mut c_void);

#[repr(C)]
pub struct Connection {
    /// pointer to the Rc<Connection> from core
    pub _ctx: *mut c_void,
    pub _prepare_stmt: PrepareStmtFn,
    pub _close: CloseConnectionFn,
}

impl Connection {
    pub fn new(ctx: *mut c_void, prepare_stmt: PrepareStmtFn, close: CloseConnectionFn) -> Self {
        Connection {
            _ctx: ctx,
            _prepare_stmt: prepare_stmt,
            _close: close,
        }
    }
    pub fn close(&self) {
        unsafe { (self._close)(self._ctx) };
    }

    pub fn to_ptr(&self) -> *const Connection {
        self
    }

    /// #Safety
    /// returns static reference to self by dereferencing a raw pointer
    pub unsafe fn from_ptr(ptr: *const Connection) -> &'static Self {
        unsafe { &*ptr }
    }

    pub unsafe fn mut_from_ptr(ptr: *mut Connection) -> &'static mut Self {
        unsafe { &mut *ptr }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn prepare_stmt(&self, sql: &str) -> &mut Stmt {
        let sql = CString::new(sql).unwrap();
        let stmt = unsafe { (self._prepare_stmt)(self._ctx as *const ExtensionApi, sql.as_ptr()) };
        Stmt::from_ptr(stmt)
    }
}

pub struct Statement {
    __ctx: *const Stmt,
}

#[repr(C)]
pub struct Stmt {
    _marker: std::marker::PhantomData<Connection>,
    /// pointer to the underlying core Connection
    pub _conn: *const Connection,
    /// pointer to core 'Statement'
    pub _ctx: *mut c_void,
    /// pointer to the error message
    pub _err: *const c_char,
    pub _bind_args_fn: BindArgsFn,
    pub _step: StmtStepFn,
    pub _get_row_values: StmtGetRowValuesFn,
    pub _get_column_names: GetColumnNamesFn,
    pub _close: CloseStmtFn,
    pub _get_column_count: GetResultColumnCountFn,
}

impl Stmt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conn: *const Connection,
        ctx: *mut c_void,
        bind_fn: BindArgsFn,
        step: StmtStepFn,
        get_row: StmtGetRowValuesFn,
        get_column_names: GetColumnNamesFn,
        close: CloseStmtFn,
        get_column_count: GetResultColumnCountFn,
    ) -> Self {
        Stmt {
            _marker: std::marker::PhantomData,
            _conn: conn,
            _ctx: ctx,
            _err: std::ptr::null(),
            _bind_args_fn: bind_fn,
            _step: step,
            _get_row_values: get_row,
            _get_column_names: get_column_names,
            _close: close,
            _get_column_count: get_column_count,
        }
    }

    pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
        unsafe { &mut *(ptr as *mut Stmt) }
    }
    pub fn close(&self) {
        unsafe { (self._close)(self._ctx) };
    }

    pub fn to_ptr(&self) -> *const Stmt {
        self
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_mut_ptr(&mut self) -> *mut Stmt {
        self
    }

    pub fn get_error(&self) -> Option<&str> {
        if self._err.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(self._err).to_str().ok() }
    }

    pub fn bind_args(&self, idx: i32, arg: Value) {
        unsafe { (self._bind_args_fn)(self._ctx, idx, arg) };
    }

    pub fn step(&self) -> ResultCode {
        unsafe { (self._step)(self._ctx) }
    }

    pub fn result_col_count(&self) -> i32 {
        unsafe { (self._get_column_count)(self._ctx) }
    }

    pub fn get_row(&self) -> &[Value] {
        let values = unsafe { (self._get_row_values)(self._ctx) };
        if values.is_null() {
            return &[];
        }
        let col_count = self.result_col_count();
        let result = unsafe { std::slice::from_raw_parts(values, col_count as usize) };
        result
    }

    pub fn get_column_names(&self) -> Vec<String> {
        let count: *mut i32 = std::ptr::null_mut();
        let col_names = unsafe { (self._get_column_names)(self._ctx, count) };
        if !count.is_null() && unsafe { (*count) > 0 } {
            let count = unsafe { *count as usize };
            let mut names = Vec::new();
            let slice = unsafe { std::slice::from_raw_parts(col_names, count) };
            slice.iter().for_each(|x| {
                let name = unsafe { CString::from_raw(*x) };
                names.push(name.to_str().unwrap().to_string());
            });
            names
        } else {
            Vec::new()
        }
    }
}
