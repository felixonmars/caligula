use std::{
    fs::File,
    io::{BufReader, Seek},
    path::Path,
    process::exit,
};

use bytesize::ByteSize;
use indicatif::{ProgressBar, ProgressStyle};
use inquire::{Select, Text};

use crate::{
    compression::{decompress, CompressionFormat},
    hash::{parse_hash_input, FileHashInfo, HashAlg, Hashing},
};

pub fn ask_hash(
    input_file: impl AsRef<Path>,
    cf: CompressionFormat,
) -> anyhow::Result<Option<FileHashInfo>> {
    let input_file = input_file.as_ref();

    let params = loop {
        match ask_hash_once(cf) {
            Ok(bhp) => {
                break bhp;
            }
            Err(e) => match e.downcast::<Recoverable>()? {
                Recoverable::AskAgain => {
                    continue;
                }
                Recoverable::Skip => {
                    return Ok(None);
                }
            },
        }
    };

    let hash_result = do_hashing(input_file, &params)?;

    if hash_result.file_hash == params.expected_hash {
        eprintln!("Disk image verified successfully!");
    } else {
        eprintln!("Hash did not match!");
        eprintln!(
            "  Expected: {}",
            base16::encode_lower(&params.expected_hash)
        );
        eprintln!(
            "    Actual: {}",
            base16::encode_lower(&hash_result.file_hash)
        );
        eprintln!("Your disk image may be corrupted!");
        exit(-1);
    }

    Ok(Some(hash_result))
}

fn ask_hash_once(cf: CompressionFormat) -> anyhow::Result<BeginHashParams> {
    let input_hash = Text::new("What is the file's hash?")
        .with_help_message("We will guess the hash algorithm from your input.")
        .prompt_skippable()?;

    let (algs, hash) = match input_hash.as_deref() {
        None | Some("skip") => Err(Recoverable::Skip)?,
        Some(hash) => match parse_hash_input(hash) {
            Ok(hash) => hash,
            Err(e) => {
                eprintln!("{e}");
                Err(Recoverable::AskAgain)?
            }
        },
    };

    let alg = match &algs[..] {
        &[] => {
            eprintln!("Could not detect the hash algorithm from your hash!");
            Err(Recoverable::AskAgain)?
        }
        &[only_alg] => {
            eprintln!("Detected {}", only_alg);
            only_alg
        }
        multiple => {
            let ans = Select::new("Which algorithm is it?", multiple.into()).prompt_skippable()?;
            if let Some(alg) = ans {
                alg
            } else {
                Err(Recoverable::AskAgain)?
            }
        }
    };

    let hasher_compression = if !cf.is_identity() {
        match Select::new(
            "Is the hash calculated before or after compression?",
            vec!["Before", "After"],
        )
        .prompt()?
        {
            "After" => CompressionFormat::Identity,
            "Before" => cf,
            _ => panic!("Impossible state!"),
        }
    } else {
        cf
    };

    Ok(BeginHashParams {
        expected_hash: hash,
        alg,
        hasher_compression,
    })
}

fn do_hashing(path: &Path, params: &BeginHashParams) -> anyhow::Result<FileHashInfo> {
    let mut file = File::open(path)?;

    // Calculate total file size
    let file_size = file.seek(std::io::SeekFrom::End(0))?;
    file.seek(std::io::SeekFrom::Start(0))?;

    let progress_bar = ProgressBar::new(file_size);
    progress_bar.set_style(ProgressStyle::with_template("{bytes} / {total_bytes}").unwrap());

    let decompress = decompress(params.hasher_compression, BufReader::new(file))?;

    let mut hashing = Hashing::new(
        params.alg,
        decompress,
        ByteSize::kib(512).as_u64() as usize, // TODO
    );
    loop {
        for _ in 0..32 {
            match hashing.next() {
                Some(_) => {}
                None => return Ok(hashing.finalize()?),
            }
        }
        progress_bar.set_position(hashing.get_reader_mut().get_mut().stream_position()?);
    }
}

struct BeginHashParams {
    expected_hash: Vec<u8>,
    alg: HashAlg,
    hasher_compression: CompressionFormat,
}

/// A signaling error for the outer loop.
#[derive(Debug, thiserror::Error)]
#[error("Recoverable error")]
enum Recoverable {
    AskAgain,
    Skip,
}
