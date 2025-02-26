use crate::{ResultCode, Value};
use std::ffi::{c_char, c_void, CStr, CString};

pub type ConnectFn = unsafe extern "C" fn(ctx: *mut c_void) -> *const Conn;
pub type PrepareStmtFn =
    unsafe extern "C" fn(api: *mut c_void, sql: *const c_char) -> *const Stmt<'static>;
pub type GetResultColumnCountFn = unsafe extern "C" fn(ctx: *mut c_void) -> i32;
pub type GetColumnNamesFn =
    unsafe extern "C" fn(ctx: *mut c_void, count: *mut i32) -> *mut *mut c_char;
pub type BindArgsFn = unsafe extern "C" fn(ctx: *mut c_void, idx: i32, arg: Value) -> ResultCode;
pub type StmtStepFn = unsafe extern "C" fn(ctx: *mut c_void) -> ResultCode;
pub type StmtGetRowValuesFn = unsafe extern "C" fn(ctx: *mut c_void) -> *const Value;
pub type CloseConnectionFn = unsafe extern "C" fn(ctx: *mut c_void);
pub type CloseStmtFn = unsafe extern "C" fn(ctx: *mut c_void);

/// core database connection
pub struct Conn {
    // public fields for core only
    pub _ctx: *mut c_void,
    pub _prepare_stmt: PrepareStmtFn,
    pub _close: CloseConnectionFn,
}

impl Conn {
    pub fn new(ctx: *mut c_void, prepare_stmt: PrepareStmtFn, close: CloseConnectionFn) -> Self {
        Conn {
            _ctx: ctx,
            _prepare_stmt: prepare_stmt,
            _close: close,
        }
    }

    pub fn close(&self) {
        unsafe { (self._close)(self._ctx) };
    }

    pub fn prepare_stmt<'conn>(&'conn self, sql: &str) -> Statement<'conn> {
        let sql = CString::new(sql).unwrap();
        let stmt =
            unsafe { (self._prepare_stmt)(self as *const Conn as *mut c_void, sql.as_ptr()) };
        Statement { __ctx: stmt }
    }
}

/// prepared statement for querying a core database connection
/// public API with wrapper methods for extensions
#[derive(Debug)]
pub struct Statement<'conn> {
    __ctx: *const Stmt<'conn>,
}

/// core database connection:
/// public API with wrapper methods for extensions
#[derive(Debug)]
pub struct Connection {
    __ctx: *const Conn,
}

impl Connection {
    pub fn new(ctx: *const Conn) -> Self {
        Connection { __ctx: ctx }
    }
    pub fn prepare<'conn>(&'conn self, sql: &str) -> Statement<'conn> {
        unsafe { (*self.__ctx).prepare_stmt(sql) }
    }

    pub fn close(self) {
        unsafe { ((*self.__ctx)._close)(self.__ctx as *mut c_void) };
    }
}

impl<'conn> Statement<'_> {
    pub fn bind(&self, idx: i32, arg: Value) {
        unsafe { (*self.__ctx).bind_args(idx, arg) }
    }

    pub fn step(&self) -> ResultCode {
        unsafe { (*self.__ctx).step() }
    }

    pub fn get_row(&self) -> &[Value] {
        unsafe { (*self.__ctx).get_row() }
    }

    pub fn result_col_count(&self) -> i32 {
        unsafe { (*self.__ctx).result_col_count() }
    }

    pub fn get_column_names(&self) -> Vec<String> {
        unsafe { (*self.__ctx).get_column_names() }
    }
}

#[repr(C)]
pub struct Stmt<'conn> {
    _marker: std::marker::PhantomData<&'conn Conn>,
    pub _conn: *const Conn,
    pub _ctx: *mut c_void,
    pub _err: *const c_char,
    pub _bind_args_fn: BindArgsFn,
    pub _step: StmtStepFn,
    pub _get_row_values: StmtGetRowValuesFn,
    pub _get_column_names: GetColumnNamesFn,
    pub _close: CloseStmtFn,
    pub _get_column_count: GetResultColumnCountFn,
}

impl<'conn> Stmt<'conn> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conn: &'conn Conn,
        ctx: *mut c_void,
        bind: BindArgsFn,
        step: StmtStepFn,
        rows: StmtGetRowValuesFn,
        names: GetColumnNamesFn,
        count: GetResultColumnCountFn,
        close: CloseStmtFn,
    ) -> Self {
        Stmt {
            _marker: std::marker::PhantomData,
            _conn: conn as *const Conn,
            _ctx: ctx,
            _err: std::ptr::null(),
            _bind_args_fn: bind,
            _step: step,
            _get_row_values: rows,
            _get_column_names: names,
            _close: close,
            _get_column_count: count,
        }
    }

    pub fn close(&self) {
        unsafe { (self._close)(self._ctx) };
    }

    pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
        unsafe { &mut *(ptr as *mut Stmt) }
    }

    pub fn to_ptr(&self) -> *const Stmt {
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
