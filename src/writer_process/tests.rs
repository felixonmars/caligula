use self::helpers::*;
use super::*;
use rstest::*;

#[test]
fn write_op_works() {
    let test = WriteTest {
        buf_size: 16,
        file_size: 1024,
        disk_size: 2048,
        disk_block_size: 8,
        checkpoint_period: 16,
    };
    let result = test.execute();

    for w in &result.requested_writes {
        assert_eq!(w.len(), test.buf_size);
    }
    assert_eq!(&result.disk[..1024], &result.file);
    assert_eq!(
        &result.events,
        &[
            StatusMessage::TotalBytes {
                src: 1024,
                dest: 256
            },
            StatusMessage::TotalBytes {
                src: 1024,
                dest: 512
            },
            StatusMessage::TotalBytes {
                src: 1024,
                dest: 768
            },
            StatusMessage::TotalBytes {
                src: 1024,
                dest: 1024
            },
            StatusMessage::TotalBytes {
                src: 1024,
                dest: 1024
            },
        ]
    );
}

#[rstest]
fn write_misaligned_file_works(
    #[values(0, 1, 33, 382, 438, 993)]
    file_size: usize,
    #[values(16, 32, 48, 64, 128)]
    buf_size: usize,
) {
    let test = WriteTest {
        buf_size,
        file_size,
        disk_size: 1024,
        disk_block_size: 16,
        checkpoint_period: 16,
    };
    let result = test.execute();

    for w in &result.requested_writes {
        assert_eq!(w.len(), test.buf_size);
    }
    assert_eq!(&result.disk[..test.file_size], &result.file);
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
/// Helpers for these tests. These go in their own little module to enforce
/// visibility.
mod helpers {
    use std::io::*;

    use rand::RngCore;

    use super::{ipc::StatusMessage, CompressionFormat, WriteOp};

    /// Wraps an in-memory buffer and logs every single chunk of data written to it.
    struct MockWrite<'a> {
        cursor: Cursor<&'a mut [u8]>,
        requested_writes: Vec<Vec<u8>>,
        enforced_block_size: usize,
    }

    impl<'a> MockWrite<'a> {
        pub fn new(data: &'a mut [u8], enforced_block_size: usize) -> Self {
            Self {
                cursor: Cursor::new(data),
                requested_writes: vec![],
                enforced_block_size,
            }
        }
    }

    impl<'a> Write for MockWrite<'a> {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            assert!(
                buf.len() % self.enforced_block_size == 0,
                "Received a write (size {}) that was not aligned to block (size {})!",
                buf.len(),
                self.enforced_block_size,
            );
            self.requested_writes.push(buf.to_owned());
            self.cursor.write(buf)
        }

        fn flush(&mut self) -> Result<()> {
            self.cursor.flush()
        }
    }

    /// Logs every single size read from it.
    struct MockRead<'a> {
        cursor: Cursor<&'a [u8]>,
        requested_reads: Vec<usize>,
        enforced_block_size: Option<usize>,
    }

    impl<'a> MockRead<'a> {
        pub fn new(data: &'a [u8], enforced_block_size: Option<usize>) -> Self {
            Self {
                cursor: Cursor::new(&data),
                requested_reads: vec![],
                enforced_block_size,
            }
        }
    }

    impl<'a> Read for MockRead<'a> {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            if let Some(bs) = &self.enforced_block_size {
                assert!(
                    buf.len() % bs == 0,
                    "Received a read (size {}) that was not aligned to blocks (size {})!",
                    buf.len(),
                    bs,
                );
            }
            self.requested_reads.push(buf.len());
            self.cursor.read(buf)
        }
    }

    pub struct WriteTest {
        pub buf_size: usize,
        pub file_size: usize,
        pub disk_size: usize,
        pub disk_block_size: usize,
        pub checkpoint_period: usize,
    }

    pub struct WriteTestResult {
        pub requested_reads: Vec<usize>,
        pub requested_writes: Vec<Vec<u8>>,
        pub file: Vec<u8>,
        pub disk: Vec<u8>,
        pub events: Vec<StatusMessage>,
    }

    impl WriteTest {
        pub fn execute(&self) -> WriteTestResult {
            let mut events = vec![];

            let file_data = make_random(self.file_size);
            let mut file = MockRead::new(&file_data, None);
            let mut disk_data = make_random(self.disk_size);
            let mut disk = MockWrite::new(&mut disk_data, self.disk_block_size);

            WriteOp {
                file: &mut file,
                disk: &mut disk,
                cf: CompressionFormat::Identity,
                buf_size: self.buf_size,
                disk_block_size: 8,
                checkpoint_period: 16,
            }
            .write(|e| events.push(e))
            .unwrap();

            WriteTestResult {
                requested_reads: file.requested_reads,
                requested_writes: disk.requested_writes,
                file: file_data,
                disk: disk_data,
                events,
            }
        }
    }

    fn make_random(n: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut dest = vec![0; n];
        rng.fill_bytes(&mut dest);
        dest
    }
}
