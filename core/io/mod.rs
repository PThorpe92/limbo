use crate::storage::buffer_pool::{ArenaBuffer, GLOBAL_BUFFER_POOL};
use crate::Result;
use bitflags::bitflags;
use cfg_block::cfg_block;
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::{fmt::Debug, pin::Pin};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Completion) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: Arc<Buffer>, c: Completion) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn size(&self) -> Result<u64>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

bitflags! {
    impl OpenFlags: i32 {
        const None = 0b00000000;
        const Create = 0b0000001;
        const ReadOnly = 0b0000010;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn run_once(&self) -> Result<()>;

    fn generate_random_number(&self) -> i64;

    fn get_memory_io(&self) -> Arc<MemoryIO>;

    fn register_arena(&self, _iov: (*mut u8, usize)) -> std::io::Result<()> {
        Err(std::io::Error::from_raw_os_error(22))
    }
}

pub type Complete = dyn Fn(Arc<Buffer>, i32);
pub type WriteComplete = dyn Fn(i32);
pub type SyncComplete = dyn Fn(i32);

pub enum Completion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<Complete>,
}

impl Completion {
    pub fn complete(&self, result: i32) {
        match self {
            Self::Read(r) => r.complete(result),
            Self::Write(w) => w.complete(result),
            Self::Sync(s) => s.complete(result), // fix
        }
    }
    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(i32) + 'static,
    {
        Self::Write(WriteCompletion::new(Box::new(complete)))
    }
    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Arc<Buffer>, i32) + 'static,
    {
        Self::Read(ReadCompletion::new(buf, Box::new(complete)))
    }
    pub fn new_sync<F>(complete: F) -> Self
    where
        F: Fn(i32) + 'static,
    {
        Self::Sync(SyncCompletion::new(Box::new(complete)))
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        match self {
            Self::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<Complete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Arc<Buffer> {
        &self.buf
    }

    pub fn buf_mut(&self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    pub fn complete(&self, res: i32) {
        (self.complete)(self.buf.clone(), res);
    }
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn complete(&self, bytes_written: i32) {
        (self.complete)(bytes_written);
    }
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self { complete }
    }

    pub fn complete(&self, res: i32) {
        (self.complete)(res);
    }
}

pub type BufferData = Pin<Box<[u8]>>;

pub enum Buffer {
    Heap {
        buf: ManuallyDrop<BufferData>,
        len: usize,
        ephemeral: bool,
    },
    Pooled(ArenaBuffer),
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        match self {
            Self::Heap {
                buf,
                len,
                ephemeral,
            } => Self::Heap {
                buf: buf.clone(),
                len: *len,
                ephemeral: *ephemeral,
            },
            Self::Pooled(buf) => Self::Pooled(buf.deep_clone()),
        }
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "{:?}", p),
            Self::Heap { buf, len, .. } => write!(f, "{:?}: {len}", buf),
        }
    }
}
impl Drop for Buffer {
    fn drop(&mut self) {
        if let Self::Heap { buf, ephemeral, .. } = self {
            unsafe {
                let b: ManuallyDrop<Pin<Box<[u8]>>> = std::ptr::read(buf);
                if !*ephemeral {
                    if let Some(bp) = GLOBAL_BUFFER_POOL.get() {
                        bp.return_heap_buffer(b);
                    }
                }
            }
        }
    }
}

impl Buffer {
    pub fn is_fixed(&self) -> bool {
        match self {
            Self::Heap { .. } => false,
            Self::Pooled(buf) => buf.is_fixed(),
        }
    }

    pub fn new_pooled(buf: ArenaBuffer) -> Arc<Self> {
        tracing::trace!("new_pooled({:?})", buf);
        Self::Pooled(buf).into()
    }

    pub fn new_temporary(size: usize) -> Arc<Self> {
        tracing::trace!("new_temporary(size={size})");
        Self::Heap {
            buf: ManuallyDrop::new(Pin::new(vec![0; size].into_boxed_slice())),
            len: size,
            ephemeral: true,
        }
        .into()
    }

    pub fn new_heap(size: usize, pg_size: usize) -> Arc<Self> {
        tracing::trace!("new_heap(size={size})");
        Self::Heap {
            buf: ManuallyDrop::new(Pin::new(vec![0; pg_size].into_boxed_slice())),
            len: size,
            ephemeral: false,
        }
        .into()
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap { len, .. } => *len,
            Self::Pooled(buf) => buf.logical_len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap { buf, len, .. } => buf[..*len].as_ref(),
            Self::Pooled(buf) => buf,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_mut_slice(&self) -> &mut [u8] {
        // SAFETY: We are the only owner of the buffer, so we can safely return a mutable slice
        // SAFETY: The buffer is guaranteed to be valid for the lifetime of the slice
        unsafe {
            match self {
                Self::Heap { buf, len, .. } => {
                    let ptr = buf.as_ptr() as *mut u8;
                    std::slice::from_raw_parts_mut(ptr, *len)
                }
                Self::Pooled(buf) => {
                    let ptr = buf.as_ptr() as *mut u8;
                    std::slice::from_raw_parts_mut(ptr, buf.len())
                }
            }
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Heap { buf, .. } => buf.as_ptr(),
            Self::Pooled(buf) => buf.as_ptr(),
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Self::Heap { buf, .. } => buf.as_mut_ptr(),
            Self::Pooled(buf) => buf.as_mut_ptr(),
        }
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
