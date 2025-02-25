use crate::{Result, ResultCode};
use std::ffi::{c_char, c_void};

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

#[cfg(not(target_family = "wasm"))]
pub trait VfsExtension: Default {
    const NAME: &'static str;
    type File: VfsFile;
    fn open_file(&self, path: &str, flags: i32, direct: bool) -> Result<Self::File>;
    fn run_once(&self) -> Result<()> {
        Ok(())
    }
    fn close(&self, _file: Self::File) -> Result<()> {
        Ok(())
    }
    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }
    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

#[cfg(not(target_family = "wasm"))]
pub trait VfsFile: Sized {
    fn lock(&mut self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock(&self) -> Result<()> {
        Ok(())
    }
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> Result<i32>;
    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32>;
    fn sync(&self) -> Result<()>;
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct VfsImpl {
    pub name: *const c_char,
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

pub type RegisterVfsFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char, vfs: *const VfsImpl) -> ResultCode;

pub type VfsOpen = unsafe extern "C" fn(
    ctx: *const c_void,
    path: *const c_char,
    flags: i32,
    direct: bool,
) -> *const c_void;

#[repr(C)]
pub struct VfsFileImpl {
    pub file: *const c_void,
    pub vfs: *const VfsImpl,
}

impl VfsFileImpl {
    pub fn new(file: *const c_void, vfs: *const VfsImpl) -> Result<Self> {
        if file.is_null() || vfs.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Self { file, vfs })
    }
}

impl Drop for VfsFileImpl {
    fn drop(&mut self) {
        if self.vfs.is_null() {
            return;
        }
        let vfs = unsafe { &*self.vfs };
        unsafe {
            (vfs.close)(self.file);
        }
    }
}
