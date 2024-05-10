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

    // Every write must be the correct size
    for w in &result.requested_writes {
        assert_eq!(w.len(), test.buf_size);
    }

    // The result of the write must be correct
    assert_eq!(&result.disk[..result.file.len()], &result.file);

    // Correct events must be emitted
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
    #[values(0, 1, 33, 382, 438, 993)] file_size: usize,
    #[values(16, 32, 48, 64, 128)] buf_size: usize,
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

#[rstest]
fn write_file_larger_than_disk(#[values(1001, 1032, 2000, 6000, 7000)] file_size: usize) {
    let test = WriteTest {
        file_size,
        buf_size: 500,
        disk_size: 1000,
        disk_block_size: 10,
        checkpoint_period: 16,
    };
    let result = test.execute();

    assert_eq!(&result.disk, &result.file[..test.disk_size]);
}

#[rstest]
fn verify_happy_case_works() {
    let file = make_random(4096);
    let disk = file.clone();

    let test = VerifyTest {
        buf_size: 128,
        file,
        disk,
        disk_block_size: 128,
        checkpoint_period: 32,
    };
    let result = test.execute();

    assert_eq!(result.return_val, Ok(()));
}

#[rstest]
fn verify_sad_case_works() {
    let file = make_random(4096);
    let mut disk = file.clone();
    disk[10] = !disk[10];

    let test = VerifyTest {
        buf_size: 128,
        file,
        disk,
        disk_block_size: 128,
        checkpoint_period: 32,
    };
    let result = test.execute();

    assert_eq!(result.return_val, Err(ErrorType::VerificationFailed));
}

#[rstest]
fn verify_misaligned_case_happy_path_works(#[values(101, 103, 4348, 8337)] file_size: usize) {
    let file = make_random(file_size);
    let mut disk = make_random(16384);
    disk[..file_size].copy_from_slice(&file);

    let test = VerifyTest {
        buf_size: 128,
        file,
        disk,
        disk_block_size: 128,
        checkpoint_period: 32,
    };
    let result = test.execute();

    assert_eq!(result.return_val, Ok(()));
}

#[rstest]
#[case(4231, 0)]
#[case(4231, 1)]
#[case(4231, 834)]
#[case(4231, 4310)]
#[case(4231, 4313)]
#[case(4231, 4320)]
fn verify_misaligned_case_sad_path_works(#[case] file_size: usize, #[case] flip_offset: usize) {
    let file = make_random(file_size);
    let mut disk = make_random(16384);
    disk[..file_size].copy_from_slice(&file);
    disk[flip_offset] = !disk[flip_offset];

    let test = VerifyTest {
        buf_size: 128,
        file,
        disk,
        disk_block_size: 128,
        checkpoint_period: 32,
    };
    let result = test.execute();

    assert_eq!(result.return_val, Err(ErrorType::VerificationFailed));
}

/// Helpers for these tests. These go in their own little module to enforce
/// visibility.
mod helpers {
    use std::io::{self, Cursor, Read, Write};

    use rand::RngCore;

    use super::{
        ipc::{ErrorType, StatusMessage},
        CompressionFormat, VerifyOp, WriteOp,
    };

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
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            assert!(
                buf.len() % self.enforced_block_size == 0,
                "Received a write (size {}) that was not aligned to block (size {})!",
                buf.len(),
                self.enforced_block_size,
            );
            self.requested_writes.push(buf.to_owned());
            self.cursor.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
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
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
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
            .execute(|e| events.push(e))
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

    pub struct VerifyTest {
        pub buf_size: usize,
        pub file: Vec<u8>,
        pub disk: Vec<u8>,
        pub disk_block_size: usize,
        pub checkpoint_period: usize,
    }

    pub struct VerifyTestResult {
        pub requested_file_reads: Vec<usize>,
        pub requested_disk_reads: Vec<usize>,
        pub events: Vec<StatusMessage>,
        pub return_val: Result<(), ErrorType>,
    }

    impl VerifyTest {
        pub fn execute(&self) -> VerifyTestResult {
            let mut events = vec![];

            let mut file = MockRead::new(&self.file, None);
            let mut disk = MockRead::new(&self.disk, Some(self.disk_block_size));

            let verification_result = VerifyOp {
                file: &mut file,
                disk: &mut disk,
                cf: CompressionFormat::Identity,
                buf_size: self.buf_size,
                disk_block_size: self.disk_block_size,
                checkpoint_period: self.checkpoint_period,
            }
            .execute(|e| events.push(e));

            VerifyTestResult {
                requested_file_reads: file.requested_reads,
                requested_disk_reads: disk.requested_reads,
                events,
                return_val: verification_result,
            }
        }
    }

    pub fn make_random(n: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut dest = vec![0; n];
        rng.fill_bytes(&mut dest);
        dest
    }
}
