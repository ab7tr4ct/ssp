use arrow::array::RecordBatch;
use crossbeam::channel;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use foldhash::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ssp_core::Pubkey;
use ssp_core::decoders::Decoder;
use ssp_core::decoders::known_mints;
use ssp_core::decoders::token_program::mint::MintDecoder;
use ssp_core::decoders::token_program::token_account::TokenAccountDecoder;
use ssp_core::filters::ResolvedFilters;
use ssp_core::parser::AccountHeader;
use ssp_core::record_batch;

const NUM_WRITERS: usize = 2;
const NUM_DECODED_WRITERS: usize = 2;

pub struct PipelineStats {
    pub bytes_read: AtomicU64,
    pub rows_parsed: AtomicU64,
    pub parser_blocked_tx: AtomicU64,
    pub parser_blocked_decoded: AtomicU64,
    pub writer_starved_acct: AtomicU64,
    pub writer_starved_decoded: AtomicU64,
    pub finished: AtomicBool,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self {
            bytes_read: AtomicU64::new(0),
            rows_parsed: AtomicU64::new(0),
            parser_blocked_tx: AtomicU64::new(0),
            parser_blocked_decoded: AtomicU64::new(0),
            writer_starved_acct: AtomicU64::new(0),
            writer_starved_decoded: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        }
    }
}

struct CountingReader<R> {
    inner: R,
    stats: Arc<PipelineStats>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.stats.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

pub fn run(
    reader: impl Read + Send + 'static,
    filters: ResolvedFilters,
    stats: Arc<PipelineStats>,
) -> anyhow::Result<()> {
    let known_mints = Arc::new(known_mints::load());

    let reader = CountingReader {
        inner: reader,
        stats: stats.clone(),
    };

    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let num_parsers = num_cpus.saturating_sub(4).max(4);

    // Stage 1: zstd → tar → raw buffers (buf, valid data size)
    // Pre-allocated fixed buffer pool enforces strict bounded memory backpressure
    let pool_size = num_parsers + 2;
    let (raw_tx, raw_rx) = channel::bounded::<(Vec<u8>, usize)>(num_parsers);
    let (recycle_tx, recycle_rx) = channel::bounded::<Vec<u8>>(pool_size);

    for _ in 0..pool_size {
        let _ = recycle_tx.send(Vec::new());
    }

    let decompress =
        std::thread::spawn(move || AccountHeader::stream_raw(reader, raw_tx, recycle_rx));

    // Stage 2: parse raw buffers → account headers + decoded batches
    let (tx, rx) = channel::bounded::<RecordBatch>(32);
    let (decoded_tx, decoded_rx) = channel::bounded::<(&'static str, RecordBatch)>(256);

    let filters = Arc::new(filters);

    let parsers: Vec<_> = (0..num_parsers)
        .map(|_| {
            let raw_rx = raw_rx.clone();
            let tx = tx.clone();
            let decoded_tx = decoded_tx.clone();
            let filters = filters.clone();
            let stats = stats.clone();
            let recycle_tx = recycle_tx.clone();
            let known_mints = known_mints.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let mut decoders: Vec<Box<dyn Decoder>> = vec![
                    Box::new(MintDecoder::new(known_mints.clone())),
                    Box::new(TokenAccountDecoder::new(known_mints)),
                ];

                let mut decoder_map: HashMap<Pubkey, Vec<usize>> = HashMap::default();
                for (i, dec) in decoders.iter().enumerate() {
                    decoder_map.entry(dec.owner()).or_default().push(i);
                }

                let mut chunk = Vec::with_capacity(65_536);

                while let Ok((buf, size)) = raw_rx.recv() {
                    AccountHeader::parse_accounts(
                        &buf[..size],
                        &filters,
                        &mut decoders,
                        &decoder_map,
                        &decoded_tx,
                        &stats.parser_blocked_decoded,
                        &tx,
                        &stats.parser_blocked_tx,
                        &mut chunk,
                    )?;
                    let _ = recycle_tx.send(buf);
                }

                if !chunk.is_empty() {
                    let record_batch = record_batch::build_record_batch(&chunk)?;
                    let _ = tx.send(record_batch);
                }

                for dec in decoders.iter_mut() {
                    if let Some(batch) = dec.flush() {
                        let _ = decoded_tx.send((dec.name(), batch));
                    }
                }

                Ok(())
            })
        })
        .collect();

    drop(raw_rx);
    drop(tx);
    drop(decoded_tx);

    // Stage 3: write parquet
    let schema = record_batch::account_schema_arc();

    let writers: Vec<_> = (0..NUM_WRITERS)
        .map(|i| {
            let rx = rx.clone();
            let schema = schema.clone();
            let stats = stats.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let file = File::create(format!("accounts_{i}.parquet"))?;
                let props = WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .set_compression(Compression::SNAPPY)
                    .set_max_row_group_size(1_000_000)
                    .build();
                let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

                while let Ok(batch) = {
                    if rx.is_empty() {
                        stats.writer_starved_acct.fetch_add(1, Ordering::Relaxed);
                    }
                    rx.recv()
                } {
                    stats
                        .rows_parsed
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    writer.write(&batch)?;
                }
                writer.close()?;
                Ok(())
            })
        })
        .collect();

    drop(rx);

    let decoded_writers: Vec<_> = (0..NUM_DECODED_WRITERS)
        .map(|i| {
            let decoded_rx = decoded_rx.clone();
            let stats = stats.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let mut writers: HashMap<&'static str, ArrowWriter<File>> = HashMap::default();
                while let Ok((name, batch)) = {
                    if decoded_rx.is_empty() {
                        stats.writer_starved_decoded.fetch_add(1, Ordering::Relaxed);
                    }
                    decoded_rx.recv()
                } {
                    let writer = writers.entry(name).or_insert_with(|| {
                        let file = File::create(format!("{name}_{i}.parquet")).unwrap();
                        let props = WriterProperties::builder()
                            .set_dictionary_enabled(false)
                            .set_compression(Compression::SNAPPY)
                            .set_max_row_group_size(1_000_000)
                            .build();
                        ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap()
                    });
                    writer.write(&batch)?;
                }

                for (_, writer) in writers {
                    writer.close()?;
                }
                Ok(())
            })
        })
        .collect();

    drop(decoded_rx);

    // Join all threads
    decompress.join().expect("decompressor panicked")?;
    for h in parsers {
        h.join().expect("parser panicked")?;
    }
    for h in writers {
        h.join().expect("writer panicked")?;
    }
    for h in decoded_writers {
        h.join().expect("decoded writer panicked")?;
    }

    stats.finished.store(true, Ordering::Release);
    Ok(())
}
