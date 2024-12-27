use super::{common, BatchWriteCompletion, Completion, File, OpenFlags, IO};
use crate::{LimboError, Result};
use libc::{c_short, fcntl, flock, iovec, F_SETLK};
use log::{debug, trace};
use nix::fcntl::{FcntlArg, OFlag};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use thiserror::Error;

const MAX_ENTRIES: u32 = 128;
const IOV_MAX: usize = 1024;
const MAX_IOVECS: usize = 128;

#[derive(Debug, Error)]
enum LinuxIOError {
    IOUringCQError(i32),
}

// Implement the Display trait to customize error messages
impl fmt::Display for LinuxIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinuxIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {}",
                code
            ),
        }
    }
}

pub struct LinuxIO {
    inner: Rc<RefCell<InnerLinuxIO>>,
}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pub pending: HashMap<u64, Rc<Completion>>,
    key: u64,
}

struct InnerLinuxIO {
    ring: WrappedIOUring,
    iovecs: [iovec; MAX_IOVECS],
    next_iovec: usize,
    // stored in PendingOps only until submission, normal unbatched ops still utilize
    // the static iovec array, they just aren't built until submision is possible
    pub unqueued: VecDeque<Rc<PendingOp>>,
    batches: Batcher,
}

pub struct PendingOp {
    opcode: OpCode,
    completion: Rc<Completion>,
}

pub enum OpCode {
    Read {
        fd: i32,
        offset: u64,
        buf: Rc<RefCell<crate::Buffer>>,
    },
    Write {
        fd: i32,
        offset: u64,
        buf: Rc<RefCell<crate::Buffer>>,
    },
    Sync {
        fd: i32,
    },
    BatchWrite {
        fd: i32,
        offset: u64,
        // expected bytes written, callback
        completions: Rc<Vec<(usize, Rc<Completion>)>>,
        // these have to live till cqe is returned
        iovecs: Vec<libc::iovec>,
    },
}

impl PendingOp {
    pub fn new(opcode: OpCode, completion: Rc<Completion>) -> Self {
        PendingOp { opcode, completion }
    }
}

pub struct Batch {
    fd: i32,
    offset: u64,
    iovecs: Vec<libc::iovec>,
    completions: Vec<(usize, Rc<Completion>)>,
    total_length: usize,
    next_offset: u64,
}

struct Batcher {
    queued: HashMap<i32, Batch>, // outstanding batches that can have ops added to them
    waiting_ops: VecDeque<Rc<PendingOp>>, // finalized batches that are ready to be submitted
    in_flight: HashMap<u64, Rc<PendingOp>>, // batches that have been submitted but not yet completed
}

impl Batcher {
    fn new() -> Self {
        Self {
            queued: HashMap::new(),
            waiting_ops: VecDeque::new(),
            in_flight: HashMap::new(), // TODO: could be changed to just Vec<iovec>
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.queued.is_empty() && self.waiting_ops.is_empty() && self.in_flight.is_empty()
    }

    #[inline(always)]
    fn should_batch(&self, fd: i32, offset: u64) -> bool {
        if let Some(batch) = self.queued.get(&fd) {
            return offset == batch.next_offset;
        }
        true
    }

    #[inline(always)]
    fn flush(&mut self) -> Vec<Rc<PendingOp>> {
        self.queued
            .drain()
            .map(|(_, batch)| batch.finalize().into())
            .collect::<Vec<_>>()
    }

    // queue a write operation for batching. if the relevant batch is full,
    // return the finalized batch to be submitted
    fn queue_write(
        &mut self,
        fd: i32,
        offset: u64,
        buf_ptr: *const u8,
        len: usize,
        c: Rc<Completion>,
    ) {
        if let Some(batch) = self.queued.get_mut(&fd) {
            // make sure it's sequential
            if offset == batch.next_offset {
                // append to existing batch if there's room
                if batch.iovecs.len() < IOV_MAX {
                    batch.push_write(offset, buf_ptr, len, c);
                } else {
                    log::info!("Batch is full for fd: {:?}", fd);
                    // it's full, so finalize the batch and add it to 'waiting'
                    let full = self.queued.remove(&fd).unwrap();
                    let finalized_op = Rc::new(full.finalize());
                    self.waiting_ops.push_back(finalized_op.clone());

                    // start a new batch for the new offset
                    let random_id = rand::random::<i32>();
                    let mut new_batch = Batch::new(fd, random_id, offset);
                    new_batch.push_write(offset, buf_ptr, len, c);
                    self.queued.insert(fd, new_batch);
                }
            }
        } else {
            log::info!("Creating new batch for fd: {:?}", fd);
            // no existing batch => create a new one
            let random_id = rand::random::<i32>();
            let mut new_batch = Batch::new(fd, random_id, offset);
            new_batch.push_write(offset, buf_ptr, len, c);
            self.queued.insert(fd, new_batch);
        }
    }
}

impl Batch {
    fn new(fd: i32, offset: u64) -> Self {
        Self {
            fd,
            offset,
            iovecs: Vec::new(),
            completions: Vec::new(),
            total_length: 0,
            next_offset: offset,
        }
    }

    fn push_write(&mut self, offset: u64, buf_ptr: *const u8, len: usize, c: Rc<Completion>) {
        debug_assert!(offset == self.next_offset, "Non-sequential offset in batch");
        debug_assert!(self.iovecs.len() < IOV_MAX, "Too many iovecs in batch");
        self.iovecs.push(libc::iovec {
            iov_base: buf_ptr as *mut _,
            iov_len: len,
        });
        self.completions.push((len, c));
        self.total_length += len;
        // Move next_offset forward
        self.next_offset = offset + len as u64;
    }

    /// convert Batch -> OpCode::BatchOp for submission
    /// TODO: perf, handle partial writes
    fn finalize(self) -> PendingOp {
        let opcode = OpCode::BatchWrite {
            fd: self.fd,
            offset: self.offset,
            iovecs: self.iovecs.into_iter().collect::<Vec<_>>(),
            completions: Rc::new(self.completions),
        };
        match opcode {
            OpCode::BatchWrite {
                ref completions, ..
            } => {
                let completions = completions.clone();
                PendingOp::new(
                    opcode,
                    Rc::new(Completion::BatchWrite(BatchWriteCompletion::new(
                        completions,
                    ))),
                )
            }
            _ => unreachable!(),
        }
    }
}

impl LinuxIO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(MAX_ENTRIES)?;
        let inner = InnerLinuxIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: HashMap::new(),
                key: 0,
            },
            iovecs: [iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS],
            next_iovec: 0,
            unqueued: VecDeque::new(),
            batches: Batcher::new(),
        };
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

#[derive(Debug, PartialEq)]
enum SqPushResult {
    Overflowed,
    Completed,
}

impl InnerLinuxIO {
    pub fn get_iovec(&mut self, buf: *const u8, len: usize) -> &iovec {
        let iovec = &mut self.iovecs[self.next_iovec];
        iovec.iov_base = buf as *mut std::ffi::c_void;
        iovec.iov_len = len;
        self.next_iovec = (self.next_iovec + 1) % MAX_IOVECS;
        iovec
    }

    #[inline(always)]
    fn should_flush_batches(&mut self) -> bool {
        !self.ring.ring.submission().is_full() && !self.batches.is_empty()
    }

    fn submit_pending(&mut self, op: OpCode, c: Rc<Completion>) -> Result<()> {
        if !self.ring.ring.submission().is_full() {
            // most ops will follow the easy path here of immediate submission
            let entry = self.build_sqe(&op)?;
            self.ring.submit_entry(&entry, c.clone());
        } else {
            // if sq is full, and this is a sequential write: batch it
            if let OpCode::Write {
                fd,
                offset,
                ref buf,
            } = &op
            {
                if self.batches.should_batch(*fd, *offset) {
                    let buff = buf.borrow();
                    self.batches
                        .queue_write(*fd, *offset, buff.as_ptr(), buff.len(), c.clone());
                    return Ok(());
                }
            }
            // if it cannot be batched, queue it up with normal priority
            let op = PendingOp {
                opcode: op,
                completion: c.clone(),
            };
            self.unqueued.push_back(Rc::new(op));
        }
        Ok(())
    }

    fn flush_batches(&mut self) -> Result<SqPushResult> {
        let ops = self.batches.flush();
        self.batches.waiting_ops.extend(ops);
        // submit all finalized batches
        while let Some(op) = self.batches.waiting_ops.pop_front() {
            if self.ring.ring.submission().is_full() {
                self.batches.waiting_ops.push_front(op);
                return Ok(SqPushResult::Overflowed);
            } else {
                let entry = self.build_sqe(&op.opcode)?;
                self.ring.submit_entry(&entry, op.completion.clone());
                self.batches
                    .in_flight
                    .insert(entry.get_user_data(), op.clone());
            }
        }
        if self.ring.pending_ops == 0 {
            self.batches.in_flight.clear();
        }
        Ok(SqPushResult::Completed)
    }

    // move operations from the unsubmitted queue to the ring, if there's room
    fn submit_unqueued(&mut self) -> Result<SqPushResult> {
        while let Some(op) = self.unqueued.pop_front() {
            if self.ring.ring.submission().is_full() {
                self.unqueued.push_front(op);
                return Ok(SqPushResult::Overflowed);
            }
            let entry = self.build_sqe(&op.opcode)?;
            self.ring.submit_entry(&entry, op.completion.clone());
            if let OpCode::BatchWrite { .. } = op.opcode {
                // batched ops get priority to clear up queue, so push front
                self.batches
                    .in_flight
                    .insert(entry.get_user_data(), op.clone());
            }
        }
        Ok(SqPushResult::Completed)
    }

    fn cycle_once(&mut self) -> Result<SqPushResult> {
        // can hang indefinitely if this is called without checking can_wait_for_completions
        if self.can_wait_for_completions() {
            self.ring.wait_for_completion()?;
            self.cycle_completions()?;
        }
        if self.should_flush_batches() {
            self.flush_batches()?;
        }
        self.submit_unqueued()
    }

    fn cycle_completions(&mut self) -> Result<()> {
        while let Some(cqe) = self.ring.get_completion() {
            let result = cqe.result();
            if result < 0 {
                return Err(LimboError::LinuxIOError(format!(
                    "{} cqe: {:?}",
                    LinuxIOError::IOUringCQError(result),
                    cqe
                )));
            }
            {
                let c = self.ring.pending.get(&cqe.user_data()).unwrap().clone();
                c.complete(result);
            }
            self.ring.pending.remove(&cqe.user_data());
            self.batches.in_flight.remove(&cqe.user_data());
        }
        if self.ring.pending_ops == 0 {
            // If no kernel ops left, in_flight is no longer relevant
            self.batches.in_flight.clear();
        }
        Ok(())
    }

    // this is only called immediately before submission, when we know that there is room,
    // to prevent overrunning iovec array with queued operations
    fn build_sqe(&mut self, op: &OpCode) -> Result<io_uring::squeue::Entry> {
        let key = self.ring.get_key();
        match &op {
            OpCode::Write { fd, offset, buf } => {
                let buf = buf.borrow();
                let len = buf.len();
                Ok(io_uring::opcode::Writev::new(
                    io_uring::types::Fd(*fd),
                    self.get_iovec(buf.as_ptr(), len),
                    1,
                )
                .offset(*offset)
                .build()
                .user_data(key))
            }
            OpCode::Read { fd, offset, buf } => {
                let buf = buf.borrow();
                let ptr = buf.as_ptr();
                Ok(io_uring::opcode::Readv::new(
                    io_uring::types::Fd(*fd),
                    self.get_iovec(ptr, buf.len()),
                    1,
                )
                .offset(*offset)
                .build()
                .user_data(key))
            }
            OpCode::Sync { fd } => Ok(io_uring::opcode::Fsync::new(io_uring::types::Fd(*fd))
                .build()
                .user_data(key)),
            OpCode::BatchWrite {
                fd, offset, iovecs, ..
            } => Ok(io_uring::opcode::Writev::new(
                io_uring::types::Fd(*fd),
                iovecs.as_ptr(),
                iovecs.len() as u32,
            )
            .offset(*offset)
            .build()
            .user_data(key)),
        }
    }

    #[inline(always)]
    fn can_wait_for_completions(&self) -> bool {
        !self.ring.empty()
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Rc<Completion>) {
        log::trace!("submit_entry({:?})", entry);
        self.pending.insert(entry.get_user_data(), c);
        unsafe {
            self.ring
                .submission()
                .push(entry)
                .expect("submission queue is full");
        }
        self.pending_ops += 1;
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        self.ring.submit_and_wait(1)?;
        Ok(())
    }

    fn get_completion(&mut self) -> Option<io_uring::cqueue::Entry> {
        // NOTE: This works because CompletionQueue's next function pops the head of the queue. This is not normal behaviour of iterators
        let entry = self.ring.completion().next();
        if entry.is_some() {
            log::trace!("get_completion({:?})", entry);
            // consumed an entry from completion queue, update pending_ops
            self.pending_ops -= 1;
        }
        entry
    }

    fn empty(&self) -> bool {
        self.pending_ops == 0 && self.pending.is_empty()
    }

    fn get_key(&mut self) -> u64 {
        self.key += 1;
        self.key
    }
}

impl IO for LinuxIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_raw_fd();
        if direct {
            match nix::fcntl::fcntl(fd, FcntlArg::F_SETFL(OFlag::O_DIRECT)) {
                Ok(_) => {},
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            };
        }
        let linux_file = Rc::new(LinuxFile {
            io: self.inner.clone(),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            linux_file.lock_file(true)?;
        }
        Ok(linux_file)
    }

    fn run_once(&self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let _ = inner.cycle_once()?;
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

    // TODO: since this is essentially run_once in a loop with a timeout,
    // probably no need to add a new trait method for this.
    fn wait_for_completion(&self, timeout: i32) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let start = std::time::Instant::now();
        loop {
            log::trace!("pending_ops: {}, batches_queued: {}, batches_in_flight: {}, batches_waiting: {}, unqueued: {}", inner.ring.pending_ops, inner.batches.queued.len(), inner.batches.in_flight.len(), inner.batches.waiting_ops.len(), inner.unqueued.len());
            if start.elapsed().as_millis() as i32 >= timeout {
                log::error!("Timeout waiting for completion {}", timeout);
                return Err(LimboError::LinuxIOError(
                    "timeout waiting for completion".into(),
                ));
            }
            if inner.cycle_once()? == SqPushResult::Completed {
                break;
            }
        }
        Ok(())
    }
}

pub struct LinuxFile {
    io: Rc<RefCell<InnerLinuxIO>>,
    file: std::fs::File,
}

impl File for LinuxFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let flock = flock {
            l_type: if exclusive {
                libc::F_WRLCK as c_short
            } else {
                libc::F_RDLCK as c_short
            },
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0, // Lock entire file
            l_pid: 0,
        };

        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        let lock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if lock_result == -1 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(LimboError::LockingError(
                    "File is locked by another process".into(),
                ));
            } else {
                return Err(LimboError::IOError(err));
            }
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let flock = flock {
            l_type: libc::F_UNLCK as c_short,
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };

        let unlock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if unlock_result == -1 {
            return Err(LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let r = match c.as_ref() {
            Completion::Read(r) => r,
            _ => unreachable!(),
        };
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        let op = OpCode::Read {
            fd: fd.0,
            offset: pos as u64,
            buf: r.buf.clone(),
        };
        io.submit_pending(op, c.clone())?;
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<Completion>,
    ) -> Result<()> {
        let mut io = self.io.borrow_mut();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let op = OpCode::Write {
            fd: fd.0,
            offset: pos as u64,
            buf: buffer.clone(),
        };
        io.submit_pending(op, c.clone())?;
        Ok(())
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        trace!("sync()");
        let op = OpCode::Sync { fd: fd.0 };
        io.submit_pending(op, c.clone())?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata().unwrap().len())
    }
}

impl Drop for LinuxFile {
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
        common::tests::test_multiple_processes_cannot_open_file(LinuxIO::new);
    }
}
