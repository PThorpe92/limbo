mod types;
pub use limbo_macros::{register_extension, scalar, AggregateDerive, VTabModuleDerive, VfsDerive};
use std::ffi::{c_char, c_void};
use std::fmt::Display;
pub use types::{ResultCode, Value, ValueType};

pub type ExtResult<T> = std::result::Result<T, ResultCode>;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,
    pub register_scalar_function: RegisterScalarFn,
    pub register_aggregate_function: RegisterAggFn,
    pub register_module: RegisterModuleFn,
    pub declare_vtab: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        sql: *const c_char,
    ) -> ResultCode,
}

impl ExtensionApi {
    pub fn declare_virtual_table(&self, name: &str, sql: &str) -> ResultCode {
        let Ok(name) = std::ffi::CString::new(name) else {
            return ResultCode::Error;
        };
        let Ok(sql) = std::ffi::CString::new(sql) else {
            return ResultCode::Error;
        };
        unsafe { (self.declare_vtab)(self.ctx, name.as_ptr(), sql.as_ptr()) }
    }
}

pub trait VfsExtension: Default {
    const NAME: &'static str;
    type File: VfsFile;
    fn open_file(&self, path: &str, flags: i32, direct: bool) -> ExtResult<Self::File>;
    fn close(&self, file: Self::File) -> ExtResult<()>;
    fn run_once(&self) -> ExtResult<()>;
    fn generate_random_number(&self) -> i64;
    fn get_current_time(&self) -> String;
}

pub trait VfsFile: Sized {
    fn lock(&mut self, exclusive: bool) -> ExtResult<()>;
    fn unlock(&self) -> ExtResult<()>;
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> ExtResult<i32>;
    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> ExtResult<i32>;
    fn sync(&self) -> ExtResult<()>;
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct VfsImpl {
    pub vfs: *const c_void,
    pub open: VfsOpen,
    pub close: VfsClose,
    pub read: VfsRead,
    pub write: VfsWrite,
    pub sync: VfsSync,
    pub lock: VfsLock,
    pub unlock: VfsUnlock,
    pub size: VfsSize,
    pub run_once: VfsRunOnce,
    pub current_time: VfsGetCurrentTime,
    pub gen_random_number: VfsGenerateRandomNumber,
}

pub type VfsOpen = unsafe extern "C" fn(
    ctx: *const c_void,
    path: *const c_char,
    flags: i32,
    direct: bool,
) -> *const c_void;

pub type VfsClose = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsRead =
    unsafe extern "C" fn(file: *const c_void, buf: *mut u8, count: usize, offset: i64) -> i32;

pub type VfsWrite =
    unsafe extern "C" fn(file: *const c_void, buf: *const u8, count: usize, offset: i64) -> i32;

pub type VfsSync = unsafe extern "C" fn(file: *const c_void) -> i32;

pub type VfsLock = unsafe extern "C" fn(file: *const c_void, exclusive: bool) -> ResultCode;

pub type VfsUnlock = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsSize = unsafe extern "C" fn(file: *const c_void) -> i64;

pub type VfsRunOnce = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsGetCurrentTime = unsafe extern "C" fn() -> *const c_char;

pub type VfsGenerateRandomNumber = unsafe extern "C" fn() -> i64;

#[repr(C)]
pub struct VfsFileImpl {
    pub file: *const c_void,
    pub vfs: *const VfsImpl,
}

impl VfsFileImpl {
    pub fn new(file: *const c_void, vfs: *const VfsImpl) -> ExtResult<Self> {
        if file.is_null() || vfs.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Self { file, vfs })
    }
}

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;

pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;

pub type RegisterScalarFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char, func: ScalarFunction) -> ResultCode;

pub type RegisterAggFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
) -> ResultCode;

pub type RegisterModuleFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode;

pub type InitAggFunction = unsafe extern "C" fn() -> *mut AggCtx;
pub type StepFunction = unsafe extern "C" fn(ctx: *mut AggCtx, argc: i32, argv: *const Value);
pub type FinalizeFunction = unsafe extern "C" fn(ctx: *mut AggCtx) -> Value;

#[repr(C)]
pub struct AggCtx {
    pub state: *mut c_void,
}

pub trait AggFunc {
    type State: Default;
    type Error: Display;
    const NAME: &'static str;
    const ARGS: i32;

    fn step(state: &mut Self::State, args: &[Value]);
    fn finalize(state: Self::State) -> Result<Value, Self::Error>;
}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub ctx: *const c_void,
    pub name: *const c_char,
    pub create_schema: VtabFnCreateSchema,
    pub open: VtabFnOpen,
    pub filter: VtabFnFilter,
    pub column: VtabFnColumn,
    pub next: VtabFnNext,
    pub eof: VtabFnEof,
    pub update: VtabFnUpdate,
    pub rowid: VtabRowIDFn,
}

impl VTabModuleImpl {
    pub fn init_schema(&self, args: Vec<Value>) -> ExtResult<String> {
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

pub type VtabFnCreateSchema = unsafe extern "C" fn(args: *const Value, argc: i32) -> *mut c_char;

pub type VtabFnOpen = unsafe extern "C" fn(*const c_void) -> *const c_void;

pub type VtabFnFilter =
    unsafe extern "C" fn(cursor: *const c_void, argc: i32, argv: *const Value) -> ResultCode;

pub type VtabFnColumn = unsafe extern "C" fn(cursor: *const c_void, idx: u32) -> Value;

pub type VtabFnNext = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;

pub type VtabFnEof = unsafe extern "C" fn(cursor: *const c_void) -> bool;

pub type VtabRowIDFn = unsafe extern "C" fn(cursor: *const c_void) -> i64;

pub type VtabFnUpdate = unsafe extern "C" fn(
    vtab: *const c_void,
    argc: i32,
    argv: *const Value,
    p_out_rowid: *mut i64,
) -> ResultCode;

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
    fn open(&self) -> Result<Self::VCursor, Self::Error>;
    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode;
    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error>;
    fn next(cursor: &mut Self::VCursor) -> ResultCode;
    fn eof(cursor: &Self::VCursor) -> bool;
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn insert(&mut self, _args: &[Value]) -> Result<i64, Self::Error> {
        Ok(0)
    }
    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}
