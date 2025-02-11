use limbo_ext::{register_extension, Result, ResultCode, VfsDerive, VfsExtension};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

register_extension! {
    vfs: { TestFS },
}

struct TestFile {
    file: File,
}

#[derive(VfsDerive, Default)]
struct TestFS;

impl VfsExtension for TestFS {
    const NAME: &'static str = "testfs";
    type File = TestFile;

    fn open(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(TestFile { file })
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn close(&self, file: Self::File) -> Result<()> {
        drop(file);
        Ok(())
    }

    fn read(
        &self,
        file: &mut Self::File,
        buf: &mut [u8],
        count: usize,
        offset: i64,
    ) -> Result<i32> {
        if file.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        file.file
            .read(&mut buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn write(&self, file: &mut Self::File, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
        if file.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        file.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self, file: &Self::File) -> Result<()> {
        file.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn lock(&self, _file: &Self::File, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock(&self, _file: &Self::File) -> Result<()> {
        Ok(())
    }

    fn size(&self, file: &Self::File) -> i64 {
        file.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
