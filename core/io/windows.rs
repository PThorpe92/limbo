use crate::{Completion, File, LimboError, OpenFlags, Result, IO};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, trace};
use super::MemoryIO;
pub struct WindowsIO {
    memory_io: Arc<MemoryIO>,
}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

unsafe impl Send for WindowsIO {}
unsafe impl Sync for WindowsIO {}

impl IO for WindowsIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;
        Ok(Arc::new(WindowsFile {
            file: RefCell::new(file),
            memory_io: Arc::new(MemoryIO::new()),
        }))
    }

    fn run_once(&self) -> Result<()> {
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
  
    fn get_memory_io(&self) -> Option<Arc<MemoryIO>> {
        Some(self.memory_io.clone())
    }
}

pub struct WindowsFile {
    file: RefCell<std::fs::File>,
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

impl File for WindowsFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        unimplemented!()
    }

    fn unlock_file(&self) -> Result<()> {
        unimplemented!()
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
        }
        c.complete(0);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<crate::Buffer>>, c: Completion) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.borrow();
        let buf = buf.as_slice();
        file.write_all(buf)?;
        c.complete(buffer.borrow().len() as i32);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let file = self.file.borrow_mut();
        file.sync_all().map_err(LimboError::IOError)?;
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata().unwrap().len())
    }
}
