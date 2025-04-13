use parking_lot::RwLock;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use crate::IO;

pub struct BufferPool {
    page_size: usize,
    buffers: RwLock<Vec<Arc<Buffer>>>,
    in_use: RwLock<Vec<AtomicBool>>,
    next_id: AtomicUsize,
}

unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

pub type BufferData = Pin<Box<[u8]>>;

#[derive(Debug)]
struct Buffer {
    pub data: BufferData,
    pub pool: Weak<BufferPool>,
    id: usize,
}
pub type BufferRef = Arc<BufferRefInner>;

/// BufferRef is a logical reference to a buffer in the pool.
/// All IO operations are done with this object.
#[derive(Debug)]
pub struct BufferRefInner {
    len: usize,
    buf: Arc<Buffer>,
}

impl BufferRefInner {
    fn new(len: usize, buf: Arc<Buffer>) -> Self {
        Self { len, buf }
    }

    /// Creates a new BufferRef with a copy of the original data
    pub fn new_cloned(&self) -> Self {
        Self {
            buf: Arc::new(Buffer {
                data: self.buf.data.clone(),
                pool: Weak::new(),
                id: usize::MAX,
            }),
            len: self.len,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn id(&self) -> u16 {
        self.buf.id as u16
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf.data[..self.len]
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buf.data.as_ptr() as *mut u8, self.len) }
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.buf.data.as_ptr() as _
    }
}

impl Drop for BufferRefInner {
    fn drop(&mut self) {
        if let Some(pool) = self.buf.pool.upgrade() {
            tracing::trace!("dropping buffer with id: {}", self.buf.id);
            pool.put(self.buf.id);
        } else {
            assert_eq!(
                self.buf.id,
                usize::MAX,
                "only copy buffers with this ID will not have a weak ref to the pool"
            );
        }
    }
}

impl BufferPool {
    pub fn new(initial: usize, page_size: usize, io: Arc<dyn IO>) -> Arc<Self> {
        let pool = Arc::new(Self {
            page_size,
            buffers: RwLock::new(Vec::with_capacity(initial)),
            in_use: RwLock::new(Vec::with_capacity(initial)),
            next_id: AtomicUsize::new(0),
        });

        pool.expand(io, initial);
        pool
    }

    pub fn get(self: &Arc<Self>, io: &Arc<dyn IO>, len: Option<usize>) -> BufferRef {
        let in_use = self.in_use.read();
        for (id, used) in in_use.iter().enumerate() {
            if used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                tracing::trace!("getting buffer with id: {id}");
                return Arc::new(BufferRefInner::new(
                    len.unwrap_or(self.page_size),
                    self.buffers.read().get(id).unwrap().clone(),
                ));
            }
        }
        self.expand(io.clone(), in_use.len() * 2);
        self.get(io, len)
    }

    pub fn put(&self, id: usize) {
        tracing::trace!("putting buffer: {id}");
        self.in_use.read()[id].store(false, Ordering::Release);
    }

    pub fn expand(self: &Arc<Self>, io: Arc<dyn IO>, count: usize) {
        let mut buffers = self.buffers.write();
        let mut in_use = self.in_use.write();
        tracing::trace!("expanding buffer pool by {count}");
        let mut iovecs = Vec::with_capacity(count);
        for _ in 0..count {
            let id = self.next_id.fetch_add(1, Ordering::AcqRel);
            let buf = Buffer {
                data: Pin::new(vec![0; self.page_size].into_boxed_slice()),
                id,
                pool: Arc::downgrade(self),
            };
            iovecs.push((id as u16, buf.data.as_ptr(), self.page_size));
            buffers.push(Arc::new(buf));
            in_use.push(AtomicBool::new(false));
        }
        let _ = io.register_buffers(&iovecs);
    }
}
