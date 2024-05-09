use std::io::BufReader;
use std::{
    fs::File,
    io::{self, Read, Seek, Write},
};

use bytesize::ByteSize;
use interprocess::local_socket::{prelude::*, GenericFilePath};
use md5::Digest;
use tracing::{debug, info, trace};
use tracing_unwrap::ResultExt;

use crate::childproc_common::child_init;
use crate::compression::{decompress, CompressionFormat};
use crate::device;
use crate::ipc_common::write_msg;

use crate::writer_process::utils::{CountRead, CountWrite};
use crate::writer_process::xplat::open_blockdev;

use super::ipc::*;

/// This is intended to be run in a forked child process, possibly with
/// escalated permissions.
#[tokio::main]
pub async fn main() {
    let (sock, args) = child_init::<WriterProcessConfig>();

    info!("Opening socket {sock}");
    let mut stream =
        LocalSocketStream::connect(sock.to_fs_name::<GenericFilePath>().unwrap_or_log())
            .unwrap_or_log();

    let mut tx = move |msg: StatusMessage| {
        write_msg(&mut stream, &msg).expect("Failed to write message");
        stream.flush().expect("Failed to flush stream");
    };

    let final_msg = match run(&mut tx, &args) {
        Ok(_) => StatusMessage::Success,
        Err(e) => StatusMessage::Error(e),
    };

    info!(?final_msg, "Completed");
    tx(final_msg);
}

fn run(mut tx: impl FnMut(StatusMessage), args: &WriterProcessConfig) -> Result<(), ErrorType> {
    debug!("Opening file {}", args.src.to_string_lossy());
    let mut file = File::open(&args.src).unwrap_or_log();
    let size = file.seek(io::SeekFrom::End(0))?;
    file.seek(io::SeekFrom::Start(0))?;

    debug!(size, "Got input file size");

    debug!("Opening {} for writing", args.dest.to_string_lossy());

    let mut disk = match args.target_type {
        device::Type::File => File::create(&args.dest)?,
        device::Type::Disk | device::Type::Partition => {
            open_blockdev(&args.dest, args.compression)?
        }
    };

    tx(StatusMessage::InitSuccess(InitialInfo {
        input_file_bytes: size,
    }));
    let buf_size = ByteSize::kib(512).as_u64() as usize;

    WriteOp {
        file: &mut file,
        disk: &mut disk,
        cf: args.compression,
        buf_size,
        disk_block_size: 512,
        checkpoint_blocks: 32,
    }
    .write(&mut tx)?;

    tx(StatusMessage::FinishedWriting {
        verifying: args.verify,
    });

    if !args.verify {
        return Ok(());
    }

    file.seek(io::SeekFrom::Start(0))?;
    disk.seek(io::SeekFrom::Start(0))?;

    VerifyOp {
        file: &mut file,
        disk: &mut disk,
        cf: args.compression,
        buf_size,
        disk_block_size: 512,
        checkpoint_blocks: 32,
    }
    .verify(tx)?;

    Ok(())
}

/// Wraps a bunch of parameters for a big complicated operation where we:
/// - decompress the input file
/// - write to a disk
/// - write stats down a pipe
struct WriteOp<F: Read, D: Write> {
    file: F,
    disk: D,
    cf: CompressionFormat,
    buf_size: usize,
    disk_block_size: usize,
    checkpoint_blocks: usize,
}

impl<S: Read, D: Write> WriteOp<S, D> {
    fn write(&mut self, mut tx: impl FnMut(StatusMessage)) -> Result<(), ErrorType> {
        let mut file = decompress(self.cf, BufReader::new(CountRead::new(&mut self.file))).unwrap();
        let mut disk = CountWrite::new(&mut self.disk);
        let mut buf = vec![0u8; self.buf_size];

        macro_rules! checkpoint {
            () => {
                disk.flush()?;
                tx(StatusMessage::TotalBytes {
                    src: file.get_mut().get_ref().count(),
                    dest: disk.count(),
                });
            };
        }

        loop {
            for _ in 0..self.checkpoint_blocks {
                let read_bytes = file.read(&mut buf)?;
                if read_bytes == 0 {
                    checkpoint!();
                    return Ok(());
                }

                disk.write(&buf[..])?;
            }
            checkpoint!();
        }
    }
}

/// Wraps a bunch of parameters for a big complicated operation where we:
/// - decompress the input file
/// - read from a disk
/// - verify both sides are correct
/// - write stats down a pipe
struct VerifyOp<F: Read, D: Read> {
    file: F,
    disk: D,
    cf: CompressionFormat,
    buf_size: usize,
    disk_block_size: usize,
    checkpoint_blocks: usize,
}

impl<F: Read, D: Read> VerifyOp<F, D> {
    fn verify(&mut self, mut tx: impl FnMut(StatusMessage)) -> Result<(), ErrorType> {
        let mut file = decompress(self.cf, BufReader::new(CountRead::new(&mut self.file))).unwrap();
        let mut disk = CountRead::new(&mut self.disk);

        let mut file_buf = vec![0u8; self.buf_size];
        let mut disk_buf = vec![0u8; self.buf_size];

        macro_rules! checkpoint {
            () => {
                tx(StatusMessage::TotalBytes {
                    src: file.get_mut().get_ref().count(),
                    dest: disk.count(),
                });
            };
        }

        loop {
            for _ in 0..self.checkpoint_blocks {
                let read_bytes = file.read(&mut file_buf)?;
                if read_bytes == 0 {
                    checkpoint!();
                    return Ok(());
                }

                disk.read(&mut disk_buf)?;

                if &file_buf[..read_bytes] != &disk_buf[..read_bytes] {
                    return Err(ErrorType::VerificationFailed);
                }
            }
            checkpoint!();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use rand::{thread_rng, RngCore};

    use super::*;

    struct MockDisk<'a> {
        data: Cursor<&'a mut [u8]>,
        writes: Vec<Vec<u8>>,
    }
    impl<'a> MockDisk<'a> {
        fn new(data: &'a mut [u8]) -> Self {
            Self {
                data: Cursor::new(data),
                writes: vec![],
            }
        }
    }
    impl<'a> Write for MockDisk<'a> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes.push(buf.to_owned());
            self.data.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.data.flush()
        }
    }

    struct MockRead<'a> {
        all_data: Cursor<&'a [u8]>,
        read_sizes: Vec<usize>,
    }

    impl<'a> MockRead<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self {
                all_data: Cursor::new(data),
                read_sizes: vec![],
            }
        }
    }
    impl<'a> Read for MockRead<'a> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read_sizes.push(buf.len());
            self.all_data.read(buf)
        }
    }

    fn make_random(n: usize) -> Vec<u8> {
        let mut rng = thread_rng();
        let mut dest = vec![0; n];
        rng.fill_bytes(&mut dest);
        dest
    }

    #[test]
    fn write_sink_works() {
        let mut events = vec![];

        let file_data = make_random(1024);
        let file = MockRead::new(file_data);
        let mut disk = MockDisk::new();
        make_random(2048);

        WriteOp {
            file: Cursor::new(&file),
            disk: Cursor::new(&mut disk),
            cf: CompressionFormat::Identity,
            buf_size: 16,
            disk_block_size: 8,
            checkpoint_blocks: 16,
        }
        .write(|e| events.push(e))
        .unwrap();

        assert_eq!(&disk[..1024], file);
        assert_eq!(
            events,
            [
                StatusMessage::TotalBytes {
                    src: 256,
                    dest: 256
                },
                StatusMessage::TotalBytes {
                    src: 512,
                    dest: 512
                },
                StatusMessage::TotalBytes {
                    src: 768,
                    dest: 768
                },
                StatusMessage::TotalBytes {
                    src: 1024,
                    dest: 1024
                },
            ]
        );
    }

    /*
    #[test]
    fn verify_sink_multiple_blocks_incorrect() {
        let src = make_random(1000);
        let mut file = src.clone();
        file[593] = 5;

        let mut sink = VerifySink {
            file: Cursor::new(file),
        };

        sink.on_block(&src[..250], &mut make_random(250)).unwrap();
        sink.on_block(&src[250..500], &mut make_random(250))
            .unwrap();
        let r2 = sink
            .on_block(&src[500..750], &mut make_random(250))
            .unwrap_err();

        assert_eq!(r2, ErrorType::VerificationFailed);
    }

    #[test]
    fn verify_sink_multiple_blocks_correct() {
        let src = make_random(1000);
        let file = src.clone();

        let mut sink = VerifySink {
            file: Cursor::new(file),
        };

        sink.on_block(&src[..500], &mut make_random(500)).unwrap();
        sink.on_block(&src[500..], &mut make_random(500)).unwrap();
    }
    */
}
