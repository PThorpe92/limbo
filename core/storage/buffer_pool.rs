use crate::{Buffer, IO};
use core::sync::atomic::{AtomicU32, Ordering};
use parking_lot::Mutex;
use std::cell::{Cell, RefCell};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

use super::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;

pub const MAX_ARENA_PAGES: u32 = 256; // 512MB total max buffer pool size
pub const DEFAULT_ARENA_SIZE: usize = 2 * 1024 * 1024; // 2MB arenas

#[derive(Debug, Clone)]
/// A page of memory from an arena used for IO operations.
/// each has size arena.page_size but has it's own logical size as
/// requested by pool.get_page which can be <= arena.page_size
pub struct ArenaBuffer {
    /// start of the buffer
    ptr: NonNull<u8>,
    /// logical size
    len: usize,
    /// packed identifier: arena, slot
    id: u32,
    // arc to ensure that the arena is not deallocated while buffers are in use
    _parent: Arc<Arena>,
}

// Buffer pool is responsible for making sure two buffers
// are not allocated from the same arena slot or overlapping.
// Only one owner can exist.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    pub fn new(ptr: NonNull<u8>, len: usize, id: u32, arena: &Arc<Arena>) -> Self {
        Self {
            ptr,
            len,
            id,
            _parent: Arc::clone(arena),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The arena ID, only useful for representing the index into
    /// the kernel's array of fixed iovecs registered with io_uring.
    pub fn io_id(&self) -> u32 {
        split_id(self.id).0
    }

    /// Mark the buffer as free in the arena.
    pub fn mark_free(&self) {
        self._parent.mark_free(self.id);
    }
}

impl Deref for ArenaBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        self.mark_free();
    }
}

/// Each arena represents a 2MB anonymous mmap'd memory region that is split into
/// logical pages that are given out to the BufferPool.
#[derive(Debug)]
pub struct Arena {
    /// id of the arena, also used as the index to io_uring registered buffers
    id: u32,
    /// pointer to the start of the arena
    base: NonNull<u8>,
    /// packed (page_size, page_count)
    pg_info: u64,
    /// whether this arena is reistered with io_uring
    is_registered: Cell<bool>,
    /// LIFO stack of free slots in the arena
    freelist: FreeStack,
    /// total number of pages not in the freelist
    in_use: AtomicU32,
    /// weak reference to the owning buffer pool
    pool: Weak<BufferPool>,
}

impl Arena {
    fn new(parent: Arc<BufferPool>, id: u32, page_size: usize) -> Arc<Self> {
        assert!(page_size > 0 && page_size <= DEFAULT_ARENA_SIZE);
        let base = unsafe { arena::alloc(DEFAULT_ARENA_SIZE) };
        let base = NonNull::new(base).unwrap();
        let page_count = (DEFAULT_ARENA_SIZE / page_size) as u32;
        let freelist = FreeStack::new(base.as_ptr(), page_size, page_count);
        tracing::trace!(
            "new arena: page_size: {}, page_count: {}",
            page_size,
            page_count
        );
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(Self {
            id,
            base,
            freelist,
            pg_info: ((page_size as u64) << 32) | (page_count as u64),
            is_registered: Cell::new(false),
            in_use: AtomicU32::new(0),
            pool: Arc::downgrade(&parent),
        })
    }

    pub fn try_alloc(self: &Arc<Self>, len: usize) -> Option<ArenaBuffer> {
        if len > self.page_size() {
            return None;
        }
        if let Some(slot) = self.freelist.pop() {
            self.in_use.fetch_add(1, Ordering::Relaxed);
            let addr = unsafe { self.base.as_ptr().add(slot as usize * self.page_size()) };
            return Some(ArenaBuffer::new(
                NonNull::new(addr).unwrap(),
                len,
                make_id(self.id, slot),
                self,
            ));
        }
        None
    }

    pub fn mark_free(&self, id: u32) {
        let split = split_id(id);
        assert!(self.id == split.0 && self.page_count() > split.1);
        tracing::trace!("arena({}: mark_free({}))", self.id, split.1);
        self.freelist.push(split.1);
        if self.in_use.fetch_sub(1, Ordering::Relaxed) == 1 && !self.is_registered.get() {
            // this arena was allocated in an overflow, so we can drop the arena to shrink the pool
            let pool = self.pool.upgrade().expect("BufferPool dropped");
            pool.try_reclaim(self.id);
        }
    }
    #[inline]
    fn page_size(&self) -> usize {
        (self.pg_info >> 32) as usize
    }

    #[inline]
    fn page_count(&self) -> u32 {
        (self.pg_info & 0xFFFF_FFFF) as u32
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        assert_eq!(self.in_use.load(Ordering::Relaxed), 0);
        unsafe { arena::dealloc(self.base.as_ptr(), DEFAULT_ARENA_SIZE) };
    }
}

const SLOT_BITS: u32 = 16; // 65,535 slots / arena
const ARENA_BITS: u32 = 8; // 256 arenas per process
const MAX_ARENAS: u32 = 1 << ARENA_BITS;

#[inline]
fn make_id(a: u32, s: u32) -> u32 {
    (a << SLOT_BITS) | s
}
#[inline]
fn split_id(id: u32) -> (u32, u32) {
    (id >> SLOT_BITS, id & ((1 << SLOT_BITS) - 1))
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

/// BufferPool manages a set of arenas which are devided into pages
/// and allocated to the pager/IO layer. Can return an ephemeral Heap
/// buffer if no arena is available for the requested size.
pub struct BufferPool {
    io: Arc<dyn IO>,
    // whatever the default page size is, we will use that plus wal frame header size
    // for consistency to prevent arenas with multiple page sizes
    default_page_size: usize,
    next_arena: AtomicU32,
    rlimit_hit: Cell<bool>,
    arenas: RefCell<Vec<Arc<Arena>>>, // mixed sizes
    expand_guard: Mutex<()>,
}

impl BufferPool {
    pub fn new(io: Arc<dyn IO>, default_page: usize) -> Arc<Self> {
        tracing::trace!("creating buffer pool with default page size: {default_page}");
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(Self {
            io,
            default_page_size: default_page,
            next_arena: AtomicU32::new(0),
            rlimit_hit: Cell::new(false),
            arenas: RefCell::new(Vec::new()),
            expand_guard: Mutex::new(()),
        })
    }

    pub fn get_page(self: &Arc<Self>, len: Option<usize>) -> Arc<Buffer> {
        let len = len.unwrap_or(self.default_page_size);
        assert!(len <= self.default_page_size + WAL_FRAME_HEADER_SIZE);
        match self.get(len) {
            Ok(b) => Buffer::new_pooled(b),
            Err(_) => Buffer::new_heap(len),
        }
    }

    fn get(self: &Arc<Self>, len: usize) -> Result<ArenaBuffer, ()> {
        // first pass over existing arenas
        if let Some(buf) = self.try_existing(len) {
            return Ok(buf);
        }

        // try to grow the pool
        if let Ok(buf) = self.try_grow(len) {
            return Ok(buf);
        }

        // second pass (pages may have been freed)
        if let Some(buf) = self.try_existing(len) {
            return Ok(buf);
        }
        Err(())
    }

    #[inline]
    fn try_existing(&self, len: usize) -> Option<ArenaBuffer> {
        for a in self.arenas.borrow().iter() {
            if let Some(b) = a.try_alloc(len) {
                return Some(b);
            }
        }
        None
    }

    fn try_grow(self: &Arc<Self>, len: usize) -> Result<ArenaBuffer, ()> {
        // if somebody else is already growing the pool we fall back to a heap buffer
        if self.expand_guard.is_locked() {
            return Err(());
        }

        let _guard = self.expand_guard.lock();
        let id = self.next_arena.fetch_add(1, Ordering::Relaxed);
        if id >= MAX_ARENAS {
            return Err(());
        }
        let arena = Arena::new(
            self.clone(),
            id,
            self.default_page_size + WAL_FRAME_HEADER_SIZE,
        );
        if !self.rlimit_hit.get() {
            if let Err(e) = self
                .io
                .register_buffer(id, (arena.base.as_ptr(), DEFAULT_ARENA_SIZE))
            {
                tracing::warn!("failed to register buffer: {id}\n{e}");
                // most likely we hit ENOMEM: meaning the rlimit_memlock has been reached for this
                // process. we can still allocate arenas but we cannot use fixed opcodes for these
                // buffers, io_uring logs which id's are fixed. we set the flag unconditionally here
                // because even if the error wasn't ENOMEM, we don't want to make the call again
                self.rlimit_hit.set(true);
            } else {
                arena.is_registered.set(true);
            }
        }
        // hand out one page from the new arena
        let buf = arena.try_alloc(len).ok_or(())?;
        self.arenas.borrow_mut().push(arena);
        Ok(buf)
    }

    fn try_reclaim(&self, id: u32) {
        // we should never be expanding while reclaiming
        assert!(!self.expand_guard.is_locked());
        let mut arenas = self.arenas.borrow_mut();
        // dont shrink below 2 arenas. these first two arenas are the buffers
        // that will be registered with io_uring if it's in use
        if arenas.len() <= 2 {
            return;
        }
        if let Some(pos) = arenas.iter().position(|a| a.id == id) {
            let arena = &arenas[pos];
            assert!(!arena.is_registered.get());
            if arena.in_use.load(Ordering::Acquire) == 0 {
                tracing::debug!("reclaiming overflow arena({})", id);
                // drop the arc, unmaps memory in Arena::drop.
                arenas.remove(pos);
            }
        };
    }
}

/// u32 MAX is used as the stack-end marker
const EMPTY: u32 = u32::MAX;

/// Every free slot stores the index of the next free slot in its first 4 bytes
#[inline(always)]
unsafe fn write_next(base: *mut u8, page_size: usize, slot: u32, next: u32) {
    (base.add(slot as usize * page_size) as *mut u32).write(next);
}

#[inline(always)]
unsafe fn read_next(base: *mut u8, page_size: usize, slot: u32) -> u32 {
    (base.add(slot as usize * page_size) as *const u32).read()
}

/// Lock-free LIFO stack of free page indices
#[derive(Debug)]
pub struct FreeStack {
    // index of the first free slot or EMPTY
    head: AtomicU32,
    base: *mut u8,
    page_size: usize,
}

unsafe impl Send for FreeStack {}
unsafe impl Sync for FreeStack {}

impl FreeStack {
    pub fn new(base: *mut u8, page_size: usize, slots: u32) -> Self {
        // build a linked list
        for i in 0..slots - 1 {
            unsafe { write_next(base, page_size, i, i + 1) };
        }
        unsafe { write_next(base, page_size, slots - 1, EMPTY) };

        Self {
            head: AtomicU32::new(0),
            base,
            page_size,
        }
    }

    /// Pop a slot from the freelist (one CAS on the fast path)
    #[inline]
    pub fn pop(&self) -> Option<u32> {
        let mut cur = self.head.load(Ordering::Acquire);
        while cur != EMPTY {
            let nxt = unsafe { read_next(self.base, self.page_size, cur) };
            match self
                .head
                .compare_exchange_weak(cur, nxt, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(cur),
                Err(v) => cur = v, // lost race, retry
            }
        }
        None
    }

    /// Add a slot back to the freelist (one CAS)
    #[inline]
    pub fn push(&self, slot: u32) {
        let mut cur = self.head.load(Ordering::Acquire);
        loop {
            unsafe { write_next(self.base, self.page_size, slot, cur) };
            match self
                .head
                .compare_exchange_weak(cur, slot, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    // debugging helper only: expensive linear scan
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let mut n = 0;
        let mut cur = self.head.load(Ordering::Acquire);
        while cur != EMPTY {
            n += 1;
            cur = unsafe { read_next(self.base, self.page_size, cur) };
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn freelist_push_pop_sequential() {
        let base = unsafe { arena::alloc(4096 * 4) };
        let freelist = FreeStack::new(base, 4096, 4);

        // pop 4 times → should get 0, 1, 2, 3
        assert_eq!(freelist.pop(), Some(0));
        assert_eq!(freelist.pop(), Some(1));
        assert_eq!(freelist.pop(), Some(2));
        assert_eq!(freelist.pop(), Some(3));

        // empty
        assert_eq!(freelist.pop(), None);

        // push back 2 slots
        freelist.push(42);
        freelist.push(17);

        // should pop 17 first (LIFO)
        assert_eq!(freelist.pop(), Some(17));
        assert_eq!(freelist.pop(), Some(42));
        assert_eq!(freelist.pop(), None);

        unsafe { arena::dealloc(base, 4096 * 4) };
    }

    #[test]
    fn freelist_exhaust_and_reuse() {
        let base = unsafe { arena::alloc(4096 * 16) };
        let freelist = FreeStack::new(base, 4096, 16);

        // pop all 16
        let mut popped = vec![];
        for _ in 0..16 {
            popped.push(freelist.pop().unwrap());
        }

        // exhausted
        assert_eq!(freelist.pop(), None);

        // push them all back in reversed order
        for &slot in popped.iter().rev() {
            freelist.push(slot);
        }

        // pop again, should get back reversed order
        for &expected in &popped {
            assert_eq!(freelist.pop(), Some(expected));
        }

        assert_eq!(freelist.pop(), None);

        unsafe { arena::dealloc(base, 4096 * 16) };
    }

    #[test]
    fn freelist_len_counts() {
        let base = unsafe { arena::alloc(4096 * 8) };
        let freelist = FreeStack::new(base, 4096, 8);

        assert_eq!(freelist.len(), 8);
        for i in 0..4 {
            assert_eq!(freelist.pop(), Some(i));
        }
        assert_eq!(freelist.len(), 4);

        freelist.push(100);
        assert_eq!(freelist.len(), 5);

        unsafe { arena::dealloc(base, 4096 * 8) };
    }

    #[test]
    fn freelist_handles_empty() {
        let base = unsafe { arena::alloc(4096) };
        let freelist = FreeStack::new(base, 4096, 1);

        assert_eq!(freelist.pop(), Some(0));
        assert_eq!(freelist.pop(), None);
        freelist.push(42);
        assert_eq!(freelist.pop(), Some(42));
        assert_eq!(freelist.pop(), None);

        unsafe { arena::dealloc(base, 4096) };
    }

    #[test]
    fn freelist_multithreaded_stress() {
        const NUM_THREADS: usize = 8;
        const SLOTS: u32 = 512;

        let base = unsafe { arena::alloc(4096 * SLOTS as usize) };
        let freelist = Arc::new(FreeStack::new(base, 4096, SLOTS));

        let mut handles = vec![];

        // each thread will pop until no slots are left
        for _ in 0..NUM_THREADS {
            let freelist = Arc::clone(&freelist);
            handles.push(thread::spawn(move || {
                let mut popped = Vec::new();
                while let Some(slot) = freelist.pop() {
                    popped.push(slot);
                }
                popped
            }));
        }

        // collect all popped slots
        let mut all_slots = Vec::new();
        for handle in handles {
            let slots = handle.join().expect("thread panicked");
            all_slots.extend(slots);
        }

        // after popping in all threads, there should be exactly `SLOTS` slots
        assert_eq!(all_slots.len() as u32, SLOTS);

        // all slots should be unique
        all_slots.sort_unstable();
        for (i, slot) in all_slots.iter().enumerate() {
            if i > 0 {
                assert_ne!(slot, &all_slots[i - 1]);
            }
            assert!(*slot < SLOTS);
        }

        // push them all back
        for slot in all_slots {
            freelist.push(slot);
        }

        // after pushing back, freelist should be full again
        assert_eq!(freelist.len(), SLOTS as usize);
        unsafe { arena::dealloc(base, 4096 * SLOTS as usize) };
    }
}
