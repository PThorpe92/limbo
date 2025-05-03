use super::{common, Completion, File, OpenFlags, IO};
use crate::{
    fast_lock::SpinLock,
    io::clock::{Clock, Instant},
    LimboError, MemoryIO, Result,
};
use rustix::fs::{self, FlockOperation, OFlags};
use std::{
    fmt,
    io::ErrorKind,
    mem::MaybeUninit,
    os::{fd::AsFd, unix::io::AsRawFd},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tracing::{debug, trace};

const ENTRIES: u32 = 1024;
const SQPOLL_IDLE: u32 = 1000;
const ACTIVE_FILES: usize = 2;

#[derive(Debug, Error)]
enum UringIOError {
    IOUringCQError(i32),
}

impl fmt::Display for UringIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UringIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {}",
                code
            ),
        }
    }
}

pub struct UringIO {
    inner: Arc<SpinLock<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pub pending: [MaybeUninit<(u64, Completion)>; ENTRIES as usize + 1],
    key: u64,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    files: AtomicU32,
    registered_arena: AtomicBool,
}

impl UringIO {
    pub fn new() -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_sqpoll(SQPOLL_IDLE)
            .setup_single_issuer()
            .setup_coop_taskrun()
            .build(ENTRIES)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(ENTRIES)?,
        };
        let sub = ring.submitter();
        sub.register_buffers_sparse(1)?; // one buffer for the bufferpool arena
        sub.register_files_sparse(ACTIVE_FILES as u32)?; // db and wal files
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: [const { MaybeUninit::uninit() }; ENTRIES as usize + 1],
                key: 0,
            },
            files: 0u32.into(),
            registered_arena: false.into(),
        };
        debug!("Using IO backend 'io-uring'");
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Self {
            inner: Arc::new(SpinLock::new(inner)),
        })
    }

    /// Register file descriptor with io_uring to allow for Fixed opcodes.
    fn register_file(&self, fd: i32) -> Result<u32> {
        let inner = &mut *self.inner.lock();
        let mut fd_idx = inner
            .files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if inner.files.load(Ordering::Relaxed) > ACTIVE_FILES as u32 {
            // TODO: when we support multiple databases, this will likely need to change.
            // each Database will register at most 2 files (db + WAL)
            // so this means we need to replace the oldest file and start over
            // io_uring will allow overwriting the fd index so we can just set back to 0.
            // If < 2 files are being used, we are always just using the next available slot.
            fd_idx = 0;
        }
        inner
            .ring
            .ring
            .submitter()
            .register_files_update(fd_idx, &[fd])?;
        trace!("io_uring(registered file: {fd})");
        Ok(fd_idx)
    }
}

fn idx_from_key(key: u64) -> usize {
    (key & (ENTRIES - 1) as u64) as usize
}

const GEN_SHIFT: u64 = 32;
impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Completion) {
        trace!("submit_entry({:?})", entry);
        let usr_data = entry.get_user_data();
        let idx = idx_from_key(usr_data);
        self.pending[idx].write((usr_data >> GEN_SHIFT, c));
        if self.ring.submission().is_full() {
            self.wait_for_completion().unwrap();
        }
        unsafe {
            self.ring
                .submission()
                .push(entry)
                .expect("submission queue is full");
        }
        self.pending_ops += 1;
        assert!(self.pending_ops <= ENTRIES as usize);
    }

    #[inline(always)]
    fn wait_for_completion(&mut self) -> Result<()> {
        self.ring.submit_and_wait(1)?;
        Ok(())
    }

    #[inline(always)]
    fn get_completion(&mut self) -> Option<io_uring::cqueue::Entry> {
        // NOTE: This works because CompletionQueue's next function pops the head of the queue. This is not normal behaviour of iterators
        let entry = self.ring.completion().next();
        if entry.is_some() {
            trace!("get_completion({:?})", entry);
            // consumed an entry from completion queue, update pending_ops
            self.pending_ops -= 1;
        }
        entry
    }

    #[inline(always)]
    fn empty(&self) -> bool {
        self.pending_ops == 0
    }

    #[inline(always)]
    fn get_key(&mut self) -> u64 {
        let slot = (self.key & ((ENTRIES - 1) as u64)) + 1; // round‑robin slot
        let gen = (self.key >> GEN_SHIFT) + 1;
        self.key = (gen << GEN_SHIFT) | slot;
        self.key
    }
}

impl IO for UringIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_fd();
        if direct {
            match fs::fcntl_setfl(fd, OFlags::DIRECT) {
                Ok(_) => {}
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            }
        }
        let id = self.register_file(file.as_raw_fd())?;
        let uring_file = Arc::new(UringFile {
            io: self.inner.clone(),
            file,
            id,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(uring_file)
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut inner = self.inner.lock();
        let ring = &mut inner.ring;
        if ring.empty() {
            return Ok(());
        }

        ring.wait_for_completion()?;
        while let Some(cqe) = ring.get_completion() {
            let result = cqe.result();
            if result < 0 {
                return Err(LimboError::UringIOError(format!(
                    "{} cqe: {:?}",
                    UringIOError::IOUringCQError(result),
                    cqe
                )));
            }
            let user_data = cqe.user_data();
            let idx = idx_from_key(user_data);
            // ensure the completion is for the current request and we didnt wrap
            let (gen, comp) = unsafe { ring.pending[idx].assume_init_read() };
            if gen == (user_data >> GEN_SHIFT) {
                comp.complete(result);
                ring.pending[idx] = MaybeUninit::uninit();
            } else {
                return Err(LimboError::UringIOError(format!(
                    "Invalid completion: {cqe:?}, expected {gen} but got {}",
                    user_data >> GEN_SHIFT
                )));
            }
        }
        Ok(())
    }

    fn register_arena(&self, iovec: (*mut u8, usize)) -> std::io::Result<()> {
        let inner = self.inner.lock();
        unsafe {
            inner.ring.ring.submitter().register_buffers_update(
                0,
                &[libc::iovec {
                    iov_base: iovec.0 as _,
                    iov_len: iovec.1,
                }],
                None,
            )
        }?;

        inner.registered_arena.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }
}

impl Clock for UringIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct UringFile {
    io: Arc<SpinLock<InnerUringIO>>,
    file: std::fs::File,
    id: u32,
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}

impl File for UringFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {}", io_error),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let io = &mut *self.io.lock();
        let read_e = {
            let len = r.buf.len();
            let ptr = r.buf.as_ptr() as *mut u8;
            assert!(!ptr.is_null());
            if r.buf.is_fixed() && io.registered_arena.load(Ordering::Relaxed) {
                trace!("pread_fixed(pos = {}, length = {})", pos, r.buf.len());
                io_uring::opcode::ReadFixed::new(
                    io_uring::types::Fixed(self.id),
                    ptr,
                    len as u32,
                    0,
                )
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
            } else {
                trace!("pread(pos = {}, length = {})", pos, r.buf.len());
                io_uring::opcode::Read::new(fd, ptr, len as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(io.ring.get_key())
            }
        };
        io.ring.submit_entry(&read_e, c);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<crate::Buffer>, c: Completion) -> Result<()> {
        assert!(!buffer.as_ptr().is_null());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let io = &mut *self.io.lock();
        let write = {
            if io.registered_arena.load(Ordering::Relaxed) && buffer.is_fixed() {
                trace!("pwrite_fixed(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::WriteFixed::new(
                    io_uring::types::Fixed(self.id),
                    buffer.as_ptr(),
                    buffer.len() as u32,
                    0,
                )
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
            } else {
                trace!("pwrite(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::Write::new(fd, buffer.as_ptr(), buffer.len() as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(io.ring.get_key())
            }
        };
        io.ring.submit_entry(&write, c);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let io = &mut *self.io.lock();
        trace!("sync()");
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(io.ring.get_key());
        io.ring.submit_entry(&sync, c);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::common;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
    }
}
