use std::io::BufReader;
use std::panic::set_hook;
use std::{
    env,
    fs::File,
    io::{self, Read, Seek, Write},
    path::PathBuf,
};

use bytesize::ByteSize;
use interprocess::local_socket::LocalSocketStream;
use tracing::{debug, error, info, trace};
use tracing_unwrap::ResultExt;

use crate::compression::decompress;
use crate::device;
use crate::logging::init_logging_child;

use crate::burn::xplat::open_blockdev;

use super::{ipc::*, BURN_ENV};

pub fn is_in_burn_mode() -> bool {
    env::var(BURN_ENV) == Ok("1".to_string())
}

/// This is intended to be run in a forked child process, possibly with
/// escalated permissions.
pub fn main() {
    let cli_args: Vec<String> = env::args().collect();
    let args = serde_json::from_str::<BurnConfig>(&cli_args[1]).unwrap_or_log();
    init_logging_child(&args.logfile);

    set_hook(Box::new(|p| {
        error!("{p}");
    }));

    debug!("We are in child process mode with args {:#?}", args);

    let sock = cli_args[2].as_str();
    info!("Opening socket {sock}");
    let reporter = StatusReporter::open(sock);

    let mut ctx = Ctx { args, reporter };
    let final_msg = match ctx.run() {
        Ok(_) => StatusMessage::Success,
        Err(e) => StatusMessage::Error(e),
    };
    ctx.send_msg(final_msg);
}

struct Ctx {
    reporter: StatusReporter,
    args: BurnConfig,
}

impl Ctx {
    fn run(&mut self) -> Result<(), ErrorType> {
        debug!("Opening file {}", self.args.src.to_string_lossy());
        let mut src = File::open(&self.args.src).unwrap_or_log();
        let size = src.seek(io::SeekFrom::End(0))?;
        src.seek(io::SeekFrom::Start(0))?;

        debug!(size, "Got input file size");

        self.burn(&mut src, size)?;
        self.send_msg(StatusMessage::FinishedWriting {
            verifying: self.args.verify,
        });

        if !self.args.verify {
            return Ok(());
        }

        src.seek(io::SeekFrom::Start(0))?;
        self.verify(&mut src)?;

        Ok(())
    }

    fn burn(&mut self, src: &mut File, input_file_bytes: u64) -> Result<(), ErrorType> {
        debug!("Opening {} for writing", self.args.dest.to_string_lossy());

        let mut dest = match self.args.target_type {
            device::Type::File => File::create(&self.args.dest)?,
            device::Type::Disk | device::Type::Partition => open_blockdev(&self.args.dest)?,
        };
        self.send_msg(StatusMessage::InitSuccess(InitialInfo { input_file_bytes }));

        for_each_block(self, src, |block, _| {
            trace!(block_len = block.len(), "Writing block");

            let written = dest.write(block).expect("Failed to write block to disk");
            if written != block.len() {
                return Err(ErrorType::EndOfOutput);
            }
            Ok(())
        })
    }

    fn verify(&mut self, src: &mut File) -> Result<(), ErrorType> {
        debug!(
            "Opening {} for verification",
            self.args.dest.to_string_lossy()
        );

        let mut dest = File::open(&self.args.dest)?;
        for_each_block(self, src, |block, dst| {
            trace!(block_len = block.len(), "Verifying block");

            let read = dest.read(dst).expect("Failed to read block from disk");
            if read != block.len() {
                return Err(ErrorType::EndOfOutput);
            }
            if block != dst {
                return Err(ErrorType::VerificationFailed);
            }
            Ok(())
        })
    }

    pub fn send_msg(&mut self, result: StatusMessage) {
        self.reporter.send_msg(result);
    }
}

#[inline]
fn for_each_block(
    ctx: &mut Ctx,
    src: &mut File,
    mut action: impl FnMut(&[u8], &mut [u8]) -> Result<(), ErrorType>,
) -> Result<(), ErrorType> {
    let block_size = ByteSize::kb(128).as_u64() as usize;
    let mut full_block = vec![0u8; block_size];
    let mut closure_block = vec![0u8; block_size]; // A block for the user to mutate

    let mut decompress = decompress(ctx.args.compression, BufReader::new(src)).unwrap();

    let checkpoint_blocks: usize = 32;
    let mut offset: u64 = 0;

    'outer: loop {
        for _ in 0..checkpoint_blocks {
            let read_bytes = decompress.read(&mut full_block)?;
            if read_bytes == 0 {
                break 'outer;
            }

            action(&full_block[..read_bytes], &mut closure_block[..read_bytes])?;
            offset += read_bytes as u64;
        }

        ctx.send_msg(StatusMessage::TotalBytes {
            src: decompress.get_mut().stream_position()?,
            dest: offset,
        });
    }

    ctx.send_msg(StatusMessage::TotalBytes {
        src: decompress.get_mut().stream_position()?,
        dest: offset,
    });

    Ok(())
}

struct StatusReporter(LocalSocketStream);

impl StatusReporter {
    fn open(path: &str) -> Self {
        Self(LocalSocketStream::connect(PathBuf::from(path)).unwrap_or_log())
    }

    fn send_msg(&mut self, msg: StatusMessage) {
        trace!("Sending message {:?}", msg);
        serde_json::to_writer(&mut self.0, &msg).expect("Failed to convert message to JSON");
        self.0.write(b"\n").expect("Failed to write to socket");
    }
}
