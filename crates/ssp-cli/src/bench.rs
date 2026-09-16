use std::io::{self, BufReader, Read};
use std::time::Instant;

use ssp_core::parser::{self, AccountHeader};

/// Benchmark each pipeline stage separately to find the bottleneck.
pub fn run(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 1: zstd only — decompress to sink
    let start = Instant::now();
    let mut decoder = zstd::Decoder::with_buffer(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();
    let bytes = io::copy(&mut decoder, &mut io::sink()).expect("zstd decompress failed");
    let elapsed = start.elapsed().as_secs_f64();
    let gb = bytes as f64 / 1_073_741_824.0;
    eprintln!(
        "[zstd only]       {:.2} GB in {:.1}s — {:.0} MB/s decompressed",
        gb,
        elapsed,
        (bytes as f64 / 1_048_576.0) / elapsed
    );
}

pub fn run_tar(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 2: zstd + tar — iterate entries, read data, no parsing
    let start = Instant::now();
    let mut decoder = zstd::Decoder::with_buffer(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();

    let mut header = [0u8; parser::TAR_BLOCK];
    let mut skip_buf = [0u8; 32768];
    let mut total_bytes: u64 = 0;
    let mut entries: u64 = 0;
    let mut account_files: u64 = 0;

    loop {
        match decoder.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("tar header read failed: {e}"),
        }

        if header[..100].iter().all(|&b| b == 0) {
            break;
        }

        let size = parser::parse_octal(&header[124..136]) as usize;
        let padded = (size + parser::TAR_BLOCK - 1) & !(parser::TAR_BLOCK - 1);
        entries += 1;

        if parser::is_accounts_entry(&header) {
            account_files += 1;
            let mut buf = vec![0u8; size];
            decoder.read_exact(&mut buf).expect("read data failed");
            total_bytes += size as u64;

            let padding = padded - size;
            if padding > 0 {
                decoder.read_exact(&mut skip_buf[..padding]).unwrap();
            }
        } else {
            let mut remaining = padded;
            while remaining > 0 {
                let chunk = remaining.min(skip_buf.len());
                decoder.read_exact(&mut skip_buf[..chunk]).unwrap();
                remaining -= chunk;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "[zstd + tar]      {:.2} GB in {:.1}s — {:.0} MB/s ({} entries, {} account files)",
        total_bytes as f64 / 1_073_741_824.0,
        elapsed,
        (total_bytes as f64 / 1_048_576.0) / elapsed,
        entries,
        account_files
    );
}

pub fn run_full(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 3: zstd + tar + parse — full pipeline minus channel/writers
    let start = Instant::now();
    let mut decoder = zstd::Decoder::with_buffer(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();

    let mut header = [0u8; parser::TAR_BLOCK];
    let mut skip_buf = [0u8; 32768];
    let mut total_accounts: u64 = 0;

    loop {
        match decoder.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("tar header read failed: {e}"),
        }

        if header[..100].iter().all(|&b| b == 0) {
            break;
        }

        let size = parser::parse_octal(&header[124..136]) as usize;
        let padded = (size + parser::TAR_BLOCK - 1) & !(parser::TAR_BLOCK - 1);

        if parser::is_accounts_entry(&header) {
            let mut buf = vec![0u8; size];
            decoder.read_exact(&mut buf).expect("read data failed");

            let padding = padded - size;
            if padding > 0 {
                decoder.read_exact(&mut skip_buf[..padding]).unwrap();
            }

            let mut offset = 0;
            while offset + size_of::<AccountHeader>() <= buf.len() {
                let h = bytemuck::from_bytes::<AccountHeader>(
                    &buf[offset..offset + size_of::<AccountHeader>()],
                );
                offset += size_of::<AccountHeader>();
                offset += h.data_len as usize;
                offset = (offset + 7) & !7;
                total_accounts += 1;
            }
        } else {
            let mut remaining = padded;
            while remaining > 0 {
                let chunk = remaining.min(skip_buf.len());
                decoder.read_exact(&mut skip_buf[..chunk]).unwrap();
                remaining -= chunk;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "[zstd + tar + parse] {:.1}s — {} accounts parsed",
        elapsed, total_accounts
    );
}
