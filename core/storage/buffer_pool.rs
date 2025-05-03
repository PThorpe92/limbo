use crossbeam::queue::SegQueue;

use crate::io::BufferData;
use crate::{Buffer, GLOBAL_BUFFER_POOL};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use super::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;

const DEFAULT_ARENA_SIZE: usize = 4 * 1024 * 1024; // 2MB arena
const CACHED_PAGE_COUNT: usize = 256; // number of pages to cache in thread local storage

thread_local! {
    static TLC_PAGES: RefCell<VecDeque<FreeEntry>> = const { RefCell::new(VecDeque::new())};
    static TLC_OVERFLOW: RefCell<VecDeque<ManuallyDrop<BufferData>>> = const { RefCell::new(VecDeque::new())};
    static TLC_OVERFLOW_WAL: RefCell<VecDeque<ManuallyDrop<BufferData>>> = const { RefCell::new(VecDeque::new())};
}
/// A page of memory from an arena used for IO operations.
/// each has size arena.page_size but has it's own logical size
pub struct ArenaBuffer {
    fixed: bool,
    cloned: bool,
    /// the entry in the freelist of the buffer pool
    entry: FreeEntry,
    /// the logical size of the requested buffer
    len: usize,
}

impl std::fmt::Debug for ArenaBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArenaBuffer(fixed={},len={})", self.fixed, self.len)
    }
}

impl Clone for ArenaBuffer {
    fn clone(&self) -> Self {
        // clones will always be done on the Arc, if there is a
        // clone on the dereferenced buffer it will be a deep clone
        self.deep_clone()
    }
}

// Buffer pool is responsible for making sure two buffers
// are not allocated from the same arena slot or overlapping.
// Only one owner can exist.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    fn from_entry(fixed: bool, entry: FreeEntry, len: usize) -> Self {
        Self {
            fixed,
            entry,
            len,
            cloned: false,
        }
    }

    pub fn is_fixed(&self) -> bool {
        !self.cloned && self.fixed
    }

    pub fn deep_clone(&self) -> Self {
        tracing::trace!("deep cloning buffer");
        let buf = Self {
            cloned: true,
            fixed: false,
            entry: FreeEntry(
                NonNull::new(unsafe {
                    std::alloc::alloc(
                        std::alloc::Layout::from_size_align(self.len, std::mem::align_of::<u8>())
                            .unwrap(),
                    )
                })
                .unwrap(),
            ),
            len: self.len,
        };
        unsafe {
            std::ptr::copy_nonoverlapping(self.as_ptr(), buf.entry.0.as_ptr(), self.len);
        }
        buf
    }
    pub fn logical_len(&self) -> usize {
        self.len
    }
}

impl Deref for ArenaBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.entry.0.as_ptr(), self.len) }
    }
}

impl DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.entry.0.as_ptr(), self.len) }
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        if self.cloned {
            // deep clone, free the cloned buffer
            tracing::trace!("freeing cloned pool buffer");
            unsafe {
                std::alloc::dealloc(
                    self.entry.0.as_ptr(),
                    std::alloc::Layout::from_size_align(self.len, std::mem::align_of::<u8>())
                        .unwrap(),
                )
            };
        } else {
            tracing::trace!("freeing pooled buffer");
            BufferPool::get_global().free(self.entry);
        }
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        unsafe { arena::dealloc(self.base.as_ptr(), DEFAULT_ARENA_SIZE) };
    }
}

#[cfg(unix)]
mod arena {
    use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};
    /// On Linux we first try a 2 MiB hugetlb mapping and fall back
    /// to a normal mapping if that fails or if huge pages are
    /// unavailable.
    pub unsafe fn alloc(len: usize) -> *mut u8 {
        #[cfg(target_os = "linux")]
        {
            // try explicit 2 MiB hugetlb page
            if let Ok(ptr) = mmap_anonymous(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE | MapFlags::HUGETLB | MapFlags::HUGE_2MB,
            ) {
                // check for MAP_FAILED
                if ptr != !0 as *mut std::ffi::c_void {
                    return ptr.cast();
                }
            }
        }

        // Darwin and fallback: normal anonymous mapping
        let ptr = mmap_anonymous(
            std::ptr::null_mut(),
            len,
            ProtFlags::READ | ProtFlags::WRITE,
            MapFlags::PRIVATE,
        )
        .expect("mmap failed");

        #[cfg(target_os = "linux")]
        {
            // Advise kernel to use transparent hugepages for this mapping since hugetlb is not available.
            // This is advise only so errors aren’t fatal, we can ignore ENOSYS / EINVAL / ENOMEM.
            let _ = rustix::mm::madvise(ptr, len, rustix::mm::Advice::LinuxHugepage);
        }
        ptr.cast()
    }

    pub unsafe fn dealloc(ptr: *mut u8, len: usize) {
        munmap(ptr.cast(), len).expect("munmap failed");
    }
}

#[cfg(windows)]
mod arena {
    use windows_sys::Win32::System::Memory::{
        VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE,
    };

    pub unsafe fn alloc(len: usize) -> *mut u8 {
        let ptr = VirtualAlloc(
            std::ptr::null_mut(),
            len,
            MEM_RESERVE | MEM_COMMIT,
            PAGE_READWRITE,
        );
        assert!(!ptr.is_null(), "VirtualAlloc failed");
        ptr.cast()
    }
    pub unsafe fn dealloc(ptr: *mut u8, _len: usize) {
        let ok = VirtualFree(ptr.cast(), 0, MEM_RELEASE);
        assert!(ok != 0, "VirtualFree failed");
    }
}

#[derive(Debug)]
/// BufferPool manages a set of arenas which are devided into pages
/// and allocated to the pager/IO layer.
pub struct BufferPool {
    /// the page_size of the database, and the default size of requested buffers
    default_page_size: usize,
    /// global freelist (TLC is preferred)
    freelist: SegQueue<FreeEntry>,
    /// base pointer to the arena
    base: NonNull<u8>,
    /// maximum size of a requested page (default_page_size + WAL_HEADER_SIZE)
    page_size: usize,
    registered: bool,
}
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(default_page: usize, io: Arc<dyn crate::io::IO>) -> Arc<Self> {
        tracing::trace!("creating buffer pool with default page size: {default_page}");
        // the 'page_size' is saved in the buffer pool, to keep that as the default size that is
        // returned from 'get_page', however arenas are created with
        // page_size + WAL_FRAME_HEADER_SIZE to accommodate requests from WAL writes.
        let stride = default_page + WAL_FRAME_HEADER_SIZE;
        let ptr = NonNull::new(unsafe { arena::alloc(DEFAULT_ARENA_SIZE) }).unwrap();
        let registered = io
            .register_arena((ptr.as_ptr(), DEFAULT_ARENA_SIZE))
            .is_ok();
        let page_count = DEFAULT_ARENA_SIZE / stride;
        let new = Self {
            default_page_size: default_page,
            freelist: SegQueue::new(),
            base: ptr,
            page_size: stride,
            registered,
        };
        (0..page_count).for_each(|i| {
            let base = NonNull::new(unsafe { ptr.as_ptr().add(i * stride) }).unwrap();
            new.freelist.push(FreeEntry::new(base))
        });
        Arc::new(new)
    }

    pub fn new_global(page_size: usize, io: Arc<dyn crate::io::IO>) -> Arc<Self> {
        GLOBAL_BUFFER_POOL
            .get_or_init(|| Self::new(page_size, io))
            .clone()
    }

    pub fn get_global() -> Arc<Self> {
        GLOBAL_BUFFER_POOL
            .get()
            .expect("BufferPool not initialized")
            .clone()
    }

    pub fn return_heap_buffer(self: &Arc<Self>, mut buf: ManuallyDrop<BufferData>) {
        tracing::trace!("returning heap buffer");
        match buf.len() {
            n if n == self.default_page_size => {
                TLC_OVERFLOW.with(|tc| {
                    tc.borrow_mut().push_back(buf);
                });
            }
            n if n == self.page_size => {
                TLC_OVERFLOW_WAL.with(|tc| {
                    tc.borrow_mut().push_back(buf);
                });
            }
            n => {
                tracing::debug!("Dropping heap buffer of size {n}");
                unsafe { ManuallyDrop::drop(&mut buf) };
            } // rare case, just drop the buffer
        }
    }

    pub fn get_page(self: &Arc<Self>, len: Option<usize>) -> Arc<Buffer> {
        let len = len.unwrap_or(self.default_page_size);
        assert!(len <= self.page_size);
        // fast path, thread local cache
        TLC_PAGES.with(|tc| {
            if let Some(entry) = tc.borrow_mut().pop_front() {
                return Buffer::new_pooled(ArenaBuffer::from_entry(self.registered, entry, len));
            }
            // if cache is empty, grab from the global freelist
            match self
                .freelist
                .pop()
                .map(|buf| ArenaBuffer::from_entry(self.registered, buf, len))
            {
                Some(b) => Buffer::new_pooled(b),
                None => {
                    // if freelist is empty, try thread local cache of overflowed buffers
                    if len == self.default_page_size {
                        return TLC_OVERFLOW.with(|tc| {
                            if let Some(entry) = tc.borrow_mut().pop_front() {
                                return Arc::new(Buffer::Heap { buf: entry });
                            }
                            Buffer::new_heap(len)
                        });
                    } else if len == self.page_size {
                        return TLC_OVERFLOW_WAL.with(|tc| {
                            if let Some(entry) = tc.borrow_mut().pop_front() {
                                return Arc::new(Buffer::Heap { buf: entry });
                            }
                            Buffer::new_heap(len)
                        });
                    }
                    Buffer::new_heap(len)
                }
            }
        })
    }

    #[inline(always)]
    fn free(&self, entry: FreeEntry) {
        if let Some(spilled) = TLC_PAGES.with(|tc| {
            let mut tc = tc.borrow_mut();
            if tc.len() < CACHED_PAGE_COUNT {
                tc.push_back(entry);
                None // stored locally
            } else {
                Some(entry) // cache full
            }
        }) {
            self.freelist.push(spilled);
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FreeEntry(NonNull<u8>);

impl FreeEntry {
    pub fn new(base: NonNull<u8>) -> Self {
        Self(base)
    }
}
