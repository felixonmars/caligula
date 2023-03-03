use std::{
    env,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    time::Instant,
};

use bytesize::ByteSize;
use interprocess::local_socket::LocalSocketStream;
use tracing::info;

use super::{ipc::*, BURN_ENV};

pub fn is_in_burn_mode() -> bool {
    env::var(BURN_ENV) == Ok("1".to_string())
}

/// This is intended to be run in a forked child process, possibly with
/// escalated permissions.
pub fn main() {
    let cli_args: Vec<String> = env::args().collect();
    let args = serde_json::from_str(&cli_args[2]).unwrap();

    let mut pipe = LocalSocketStream::connect(PathBuf::from(&cli_args[1])).unwrap();

    let result = match run(&mut pipe, args) {
        Ok(r) => r,
        Err(r) => r,
    };
    send_msg(&mut pipe, StatusMessage::Terminate(result)).unwrap();
}

fn run(mut pipe: impl Write, args: BurnConfig) -> Result<TerminateResult, TerminateResult> {
    info!("Running child process");
    let mut src = File::open(&args.src)?;
    let mut dest = OpenOptions::new().write(true).open(&args.dest)?;

    send_msg(
        &mut pipe,
        StatusMessage::InitSuccess(InitialInfo {
            input_file_bytes: src.metadata()?.len(),
        }),
    )?;

    let block_size = ByteSize::kb(128).as_u64() as usize;
    let mut full_block = vec![0u8; block_size];

    let mut written_bytes: usize = 0;

    let stat_checkpoints: usize = 128;
    let checkpoint_blocks: usize = 128;

    loop {
        let start = Instant::now();
        for _ in 0..stat_checkpoints {
            for _ in 0..checkpoint_blocks {
                let read_bytes = src.read(&mut full_block)?;
                if read_bytes == 0 {
                    return Ok(TerminateResult::EndOfInput);
                }

                let write_bytes = dest.write(&full_block[..read_bytes])?;
                written_bytes += write_bytes;
                if written_bytes == 0 {
                    return Ok(TerminateResult::EndOfOutput);
                }
                dest.flush()?;
            }

            send_msg(&mut pipe, StatusMessage::TotalBytesWritten(written_bytes))?;
        }

        let duration = Instant::now().duration_since(start);
        send_msg(
            &mut pipe,
            StatusMessage::BlockSizeSpeedInfo {
                blocks_written: checkpoint_blocks,
                block_size,
                duration_millis: duration.as_millis() as u64,
            },
        )?;
    }
}

fn send_msg(mut pipe: impl Write, msg: StatusMessage) -> Result<(), serde_json::Error> {
    serde_json::to_writer(&mut pipe, &msg)?;
    pipe.write(b"\n").unwrap();
    Ok(())
}
