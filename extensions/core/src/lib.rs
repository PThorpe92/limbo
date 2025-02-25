mod functions;
mod types;
mod vfs;
mod vtabs;
use functions::{RegisterAggFn, RegisterModuleFn, RegisterScalarFn};
pub use limbo_macros::{register_extension, scalar, AggregateDerive, VTabModuleDerive, VfsDerive};
use std::ffi::c_void;
pub use types::{ResultCode, Value, ValueType};
use vfs::{RegisterVfsFn, VfsImpl};

pub type Result<T> = std::result::Result<T, ResultCode>;
pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,
    pub register_scalar_function: RegisterScalarFn,
    pub register_aggregate_function: RegisterAggFn,
    pub register_module: RegisterModuleFn,
    pub register_vfs: RegisterVfsFn,
    pub builtin_vfs: *mut *const VfsImpl,
    pub builtin_vfs_count: i32,
}

impl ExtensionApi {
    /// Since we want the option to build in extensions at compile time as well,
    /// we add a slice of VfsImpls to the extension API, and this is called with any
    /// libraries that we load staticly that will add their VFS implementations to the list.
    pub fn add_builtin_vfs(&mut self, vfs: *const VfsImpl) -> ResultCode {
        if vfs.is_null() || self.builtin_vfs.is_null() {
            return ResultCode::Error;
        }
        let mut new = unsafe {
            let slice =
                std::slice::from_raw_parts_mut(self.builtin_vfs, self.builtin_vfs_count as usize);
            Vec::from(slice)
        };
        new.push(vfs);
        self.builtin_vfs = Box::into_raw(new.into_boxed_slice()) as *mut *const VfsImpl;
        self.builtin_vfs_count += 1;
        ResultCode::OK
    }
}
