use crate::{ResultCode, Value};
use std::ffi::{c_char, c_void};

pub type RegisterModuleFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode;
pub type VTabFnCreateSchema = unsafe extern "C" fn(args: *const Value, argc: i32) -> *mut c_char;
pub type VTabFnOpen = unsafe extern "C" fn(*const c_void) -> *const c_void;
pub type VTabFnFilter =
    unsafe extern "C" fn(cursor: *const c_void, argc: i32, argv: *const Value) -> ResultCode;
pub type VTabFnColumn = unsafe extern "C" fn(cursor: *const c_void, idx: u32) -> Value;
pub type VTabFnNext = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;
pub type VTabFnEof = unsafe extern "C" fn(cursor: *const c_void) -> bool;
pub type VTabRowIDFn = unsafe extern "C" fn(cursor: *const c_void) -> i64;
pub type VTabFnUpdate = unsafe extern "C" fn(
    vtab: *const c_void,
    argc: i32,
    argv: *const Value,
    p_out_rowid: *mut i64,
) -> ResultCode;

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub ctx: *const c_void,
    pub name: *const c_char,
    pub create_schema: VTabFnCreateSchema,
    pub open: VTabFnOpen,
    pub filter: VTabFnFilter,
    pub column: VTabFnColumn,
    pub next: VTabFnNext,
    pub eof: VTabFnEof,
    pub update: VTabFnUpdate,
    pub rowid: VTabRowIDFn,
}

impl VTabModuleImpl {
    pub fn init_schema(&self, args: Vec<Value>) -> crate::Result<String> {
        let schema = unsafe { (self.create_schema)(args.as_ptr(), args.len() as i32) };
        if schema.is_null() {
            return Err(ResultCode::InvalidArgs);
        }
        for arg in args {
            unsafe { arg.free() };
        }
        let schema = unsafe { std::ffi::CString::from_raw(schema) };
        Ok(schema.to_string_lossy().to_string())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VTabKind {
    VirtualTable,
    TableValuedFunction,
}

pub trait VTabModule: 'static {
    type VCursor: VTabCursor<Error = Self::Error>;
    const VTAB_KIND: VTabKind;
    const NAME: &'static str;
    type Error: std::fmt::Display;

    fn create_schema(args: &[Value]) -> String;
    fn open(&self) -> std::result::Result<Self::VCursor, Self::Error>;
    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode;
    fn column(cursor: &Self::VCursor, idx: u32) -> std::result::Result<Value, Self::Error>;
    fn next(cursor: &mut Self::VCursor) -> ResultCode;
    fn eof(cursor: &Self::VCursor) -> bool;
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    fn insert(&mut self, _args: &[Value]) -> std::result::Result<i64, Self::Error> {
        Ok(0)
    }
    fn delete(&mut self, _rowid: i64) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> std::result::Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}
