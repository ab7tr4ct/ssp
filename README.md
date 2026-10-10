# SSP — Solana Snapshot Parser

Turn a full Solana mainnet snapshot into Parquet on a laptop. No validator, no indexer, no 500 GB of disk.

SSP finds a snapshot on the network (or takes a local `.tar.zst`), streams it through a multithreaded zstd → tar → AppendVec pipeline, and writes every account, plus decoded SPL Token mints and token accounts, to Parquet files you can query with DuckDB, Polars, Spark or anything else.

**A full mainnet snapshot is processed in ~2 min 10 s on an M1 Pro:** 120.7 GB compressed → 509.7 GB of account state → ~1.2B rows, at ~3.9 GB/s of decompressed data. That is within ~2% of what bare `zstd -dc` achieves on the same file, so the pipeline runs as fast as single-stream zstd decompression allows.

```
RPC discovery → HTTP stream → zstd → custom tar → AppendVec parse → token decode → Parquet
```

## Quick start

```bash
cargo build --release

./target/release/ssp --path snapshot.tar.zst   # parse a local snapshot
./target/release/ssp --discover                # stream from the fastest RPC node
```

## Usage

```bash
ssp --path snapshot.tar.zst                       # parse local file
ssp --path snapshot.tar.zst --owner <base58>      # only accounts owned by a program
ssp --discover                                    # stream full snapshot from fastest RPC node
ssp --discover --incremental                      # stream incremental snapshot (~1 GB)
ssp --download-full                               # download full snapshot to disk, no parsing
ssp --download-incremental --output ~/snapshots   # download incremental to a directory
```

While running, a live progress line shows throughput, rows written, elapsed time / ETA, and pipeline health counters (parsers blocked / writers starved), so you can see which stage is the bottleneck.

### Flags

| Flag                     | Description                                                      |
| ------------------------ | ---------------------------------------------------------------- |
| `--path <file>`          | Parse a local `.tar.zst` snapshot                                |
| `--discover`             | Find the fastest RPC node and stream its snapshot                |
| `--incremental`          | Use the incremental snapshot instead of the full one             |
| `--download-full`        | Download the full snapshot to disk without parsing               |
| `--download-incremental` | Download the incremental snapshot to disk without parsing        |
| `--output <dir>`         | Output directory for downloads (default: `.`)                    |
| `--owner <base58>`       | Filter by account owner                                          |
| `--pubkey <base58>`      | Filter by account pubkey                                         |
| `--hash <base58>`        | Filter by account hash                                           |
| `--include-dead`         | Include dead accounts (`lamports == 0`)                          |
| `--include-spam`         | Decode all mints/token accounts (bypass Jupiter verified filter) |
| `--bench`                | Benchmark pipeline stages in isolation (requires `--path`)       |

## Output

Files are written to the current directory, one per writer thread (`_0`, `_1`, …). Read them as a glob.

| File                       | Columns                                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `accounts_*.parquet`       | `pubkey`, `lamports`, `owner`, `data_len`, `executable`, `rent_epoch`                                    |
| `mints_*.parquet`          | `pubkey`, `mint_authority`, `freeze_authority`, `supply`, `decimals`, `is_initialized`                   |
| `token_accounts_*.parquet` | `pubkey`, `mint`, `owner`, `amount`, `delegate`, `state`, `is_native`, `delegated_amount`, `close_authority` |

Pubkeys are stored as raw 32-byte `BINARY`. Optional fields (`COption` in the on-chain layout) are nullable.

By default only mints from Jupiter's verified token list (4,550 mints, embedded at compile time) and their token accounts are decoded. Use `--include-spam` to decode everything.

Example with the DuckDB CLI:

```sql
SELECT count(*) AS accounts, sum(lamports) / 1e9 AS total_sol
FROM 'accounts_*.parquet';
```

## How it works

A 3-stage pipeline of OS threads connected by bounded `crossbeam` channels:

```
 Decompressor (1 thread)       Parsers (cores − 4, min 4)       Writers (2 + 2)
 ───────────────────────       ──────────────────────────       ───────────────
 zstd → custom tar        →    bytemuck overlay           →     accounts_*.parquet
 one AppendVec per buffer      + filters                        mints_*.parquet
          ▲                    + token decoders                 token_accounts_*.parquet
          │                           │
          └──── recycled buffers ─────┘
```

- **Streaming end to end.** The parser takes any `impl Read`, so a local file and an HTTP response go through identical code. Only the AppendVecs currently being parsed are held in memory.
- **Custom tar reader.** Hand-rolled header parsing replaces the `tar` crate: no per-entry allocations, non-account entries are skipped, and buffers can be reused.
- **Zero-copy parsing.** `bytemuck` overlays account headers and token layouts directly onto the raw bytes, with no deserialization step.
- **Bounded buffer pool.** A fixed set of `Vec<u8>` buffers cycles between the decompressor and parsers, so memory stays flat no matter how big the snapshot is.
- **Backpressure everywhere.** Every channel is bounded, and blocked/starved counters show which stage is waiting on which.
- **Pluggable decoders.** A `Decoder` trait dispatches by owner program and account size, and each decoder writes its own Parquet table.
- **Async only where it pays off.** `tokio` is used only to probe ~300 RPC nodes concurrently during discovery. The data path is plain threads.

## Benchmarks

Full mainnet snapshot `snapshot-447558550` read from a local file, release build, Apple M1 Pro (10 cores):

| Metric                       | Value                  |
| ---------------------------- | ---------------------- |
| Compressed size (`.tar.zst`) | 120.7 GB               |
| Decompressed size            | 509.7 GB (~4.2× ratio) |
| Rows written                 | ~1.2B accounts         |
| Wall time                    | 130.5 s                |
| Compressed throughput        | 924 MB/s               |
| Decompressed throughput      | ~3.9 GB/s              |

**The pipeline is bound by zstd.** A single zstd stream can only be decompressed sequentially, and bare `zstd -dc | wc -c` on the same file takes 128.2 s (~3.97 GB/s). Tar parsing, AppendVec parsing, token decoding and Parquet writing together add ~2% on top.

Compared with the original single-threaded version (baseline measured on `snapshot-389758228`, 109 GB):

| Configuration                   | Compressed throughput | Decompressed throughput |
| ------------------------------- | --------------------- | ----------------------- |
| Single-threaded (parse + write) | ~123 MB/s             | ~530 MB/s               |
| Multi-threaded pipeline         | ~924 MB/s             | ~3.9 GB/s               |

When streaming over the network, the bottleneck is download speed from the RPC node, not the pipeline.

## Project layout

Cargo workspace with two crates:

```
crates/
├── ssp-core/src/                       # library: parsing, filtering, decoding, Arrow batches
│   ├── lib.rs                          # public API
│   ├── parser.rs                       # custom tar reader, AppendVec parsing, stream_raw()
│   ├── filters.rs                      # owner / pubkey / hash / dead-account filters
│   ├── pubkey.rs                       # 32-byte Pubkey (bytemuck Pod, base58)
│   ├── record_batch.rs                 # Arrow schema, RecordBatch construction
│   └── decoders/
│       ├── mod.rs                      # Decoder trait, COptionPubkey
│       ├── known_mints.rs              # Jupiter verified token list (embedded)
│       └── token_program/
│           ├── mod.rs                  # Mint / TokenAccount layouts, COptionU64
│           ├── mint.rs                 # MintDecoder (82-byte accounts)
│           └── token_account.rs        # TokenAccountDecoder (165-byte accounts)
└── ssp-cli/src/                        # binary `ssp`
    ├── main.rs                         # CLI, entry point, live progress
    ├── pipeline.rs                     # thread orchestration, channels, PipelineStats
    ├── rpc.rs                          # RPC node discovery, probing, speed testing
    └── bench.rs                        # per-stage benchmarks
```

## Roadmap

- [x] Streaming parser (zstd → custom tar → AppendVec)
- [x] Multithreaded pipeline with bounded buffer pool and backpressure
- [x] Token Program decoding (Mint + TokenAccount)
- [x] CLI filters (owner, pubkey, hash, dead accounts)
- [x] Spam filter (Jupiter verified token list)
- [x] RPC node discovery + network streaming
- [x] Download-only mode
- [x] Core extracted as a reusable library crate
- [ ] More decoders (System, Stake, Vote, Token-2022)
- [ ] Custom decoders from a user-supplied ABI
- [ ] Configuration file
- [ ] Parallel downloads
- [ ] Incremental snapshot merging
- [ ] Resume on network failure

## Stack

Rust 2024 · bytemuck · zstd · crossbeam · arrow / parquet · reqwest · tokio · clap

## License

MIT
