use std::ops::Deref;
use std::sync::Arc;

use crate::storage::buffer_pool::BufferRef;
use crate::{BufferPool, Result};
use cfg_block::cfg_block;

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Completion) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: BufferRef, c: Completion) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn size(&self) -> Result<u64>;
}

#[derive(Copy, Clone)]
pub enum OpenFlags {
    None,
    Create,
}

impl OpenFlags {
    pub fn to_flags(&self) -> i32 {
        match self {
            Self::None => 0,
            Self::Create => 1,
        }
    }
}

pub struct IOManager {
    io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,
}

impl Deref for IOManager {
    type Target = dyn IO;

    fn deref(&self) -> &Self::Target {
        self.io.as_ref()
    }
}

impl IOManager {
    pub fn new(io: Arc<dyn IO>, page_size: usize, default_pages: usize) -> Arc<Self> {
        Arc::new(IOManager {
            io: io.clone(),
            buffer_pool: BufferPool::new(default_pages, page_size, io),
        })
    }

    pub fn run_once(&self) -> Result<()> {
        self.io.run_once()
    }

    pub fn expand_by(&self, pages: usize) {
        self.buffer_pool.expand(self.io.clone(), pages);
    }

    pub fn allocate_page(&self) -> BufferRef {
        self.buffer_pool.get(&self.io, None)
    }

    pub fn buf_with_size(&self, size: usize) -> BufferRef {
        self.buffer_pool.get(&self.io, Some(size))
    }

    pub fn prepare_read_page<F>(&self, size: Option<usize>, complete_fn: F) -> Completion
    where
        F: Fn(BufferRef) + Send + Sync + 'static,
    {
        let buffer = self.buffer_pool.get(&self.io, size);
        Completion::Read(ReadCompletion(buffer, Box::new(complete_fn)))
    }

    pub fn prepare_write_page<F>(&self, complete_fn: F) -> Completion
    where
        F: Fn(i32) + Send + Sync + 'static,
    {
        Completion::Write(WriteCompletion(Box::new(complete_fn)))
    }

    pub fn prepare_sync<F>(&self, complete_fn: F) -> Completion
    where
        F: Fn(i32) + Send + Sync + 'static,
    {
        Completion::Sync(SyncCompletion(Box::new(complete_fn)))
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn run_once(&self) -> Result<()>;

    fn generate_random_number(&self) -> i64;

    fn get_memory_io(&self) -> Arc<MemoryIO>;

    fn register_buffers(&self, buffers: &[(u16, *const u8, usize)]) -> Result<()> {
        Ok(())
    }
}

pub type Complete = Box<dyn Fn(BufferRef) + Send + Sync>;
pub type WriteComplete = Box<dyn FnMut(i32) + Send + Sync>;
pub type SyncComplete = Box<dyn Fn(i32) + Send + Sync>;

pub enum Completion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
}

impl Completion {
    pub fn complete(self, result: i32) {
        match self {
            Completion::Read(r) => r.complete(result),
            Completion::Write(w) => w.complete(result),
            Completion::Sync(s) => s.complete(result),
        }
    }
    pub fn as_read(&self) -> &ReadCompletion {
        match self {
            Completion::Read(r) => r,
            _ => panic!("Not a read completion"),
        }
    }
}

// read completion needs the ID of the buffer and the callback
pub struct ReadCompletion(BufferRef, Complete);

impl ReadCompletion {
    pub fn new(buf: BufferRef, complete: Complete) -> Self {
        Self(buf, complete)
    }
    pub fn complete(self, _res: i32) {
        self.1(self.0)
    }
    pub fn buf_ptr(&self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
    pub fn buf_slice(&self) -> &mut [u8] {
        self.0.as_mut_slice()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
pub struct WriteCompletion(WriteComplete);

pub struct SyncCompletion(SyncComplete);

impl WriteCompletion {
    pub fn new(complete: WriteComplete) -> Self {
        Self(complete)
    }
    pub fn complete(mut self, result: i32) {
        (self.0)(result)
    }
}

impl SyncCompletion {
    pub fn new(complete: SyncComplete) -> Self {
        Self(complete)
    }
    pub fn complete(mut self, result: i32) {
        (self.0)(result)
    }
}

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring"))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as SyscallIO;
        pub use unix::UnixIO as PlatformIO;
    }

    #[cfg(any(all(target_os = "linux",not(feature = "io_uring")), target_os = "macos"))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(target_os = "windows")] {
        mod windows;
        pub use windows::WindowsIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

mod memory;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
pub mod clock;
mod common;
pub use clock::Clock;
