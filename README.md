# SSP — Solana Snapshot Parser

Run full Solana blockchain analytics on your laptop — zero validator node, zero third-party APIs, and zero local snapshot persistence needed. SSP streams 100 GB+ snapshots directly from Solana mainnet RPC nodes, reconstructing full ledger states (1.05B+ records, ~466 GB) in ~2 min 10 sec on commodity hardware (Apple Silicon / M-series baseline).

By pairing zero-copy binary parsing (`bytemuck`), ring buffer pooling, and bounded channel backpressure, SSP sustains ~840 MB/s compressed input throughput (~3.6 GB/s effective decompressed state throughput) and outputs directly to Parquet + DuckDB for instant SQL queries.

## What it does

SSP discovers Solana RPC nodes, finds the fastest snapshot source, and streams the snapshot directly into a parsing pipeline — no 100GB+ local snapshot persistence needed.

```
RPC Discovery → HTTP Stream → zstd decompress → custom tar → AppendVec parse → Token decode → Parquet → DuckDB
```

## Usage

```bash
ssp --discover                          # stream full snapshot from fastest RPC node
ssp --discover --incremental            # stream incremental snapshot (~1GB)
ssp --download-full                     # download full snapshot to disk (no parsing)
ssp --download-incremental --output ~/snapshots  # download incremental to specific dir
ssp --path snapshot.tar.zst             # parse local file
ssp --path snapshot.tar.zst --owner <base58> --pubkey <base58>
```

During processing, a live progress line updates in the terminal showing progress bar, speed, rows parsed, elapsed/ETA, and pipeline health stats (parser blocked / writer starved counts).

### Flags

| Flag                     | Description                                                      |
| ------------------------ | ---------------------------------------------------------------- |
| `--path <file>`          | Parse a local `.tar.zst` snapshot                                |
| `--discover`             | Find fastest RPC node and stream snapshot                        |
| `--incremental`          | Use incremental snapshot instead of full                         |
| `--download-full`        | Download full snapshot to disk without parsing                   |
| `--download-incremental` | Download incremental snapshot to disk without parsing            |
| `--output <dir>`         | Output directory for downloads (default: `.`)                    |
| `--owner <base58>`       | Filter by account owner                                          |
| `--pubkey <base58>`      | Filter by account pubkey                                         |
| `--hash <base58>`        | Filter by account hash                                           |
| `--include-dead`         | Include dead accounts (lamports == 0)                            |
| `--include-spam`         | Decode all mints/token accounts (bypass Jupiter verified filter) |
| `--bench`                | Run pipeline benchmarks (requires `--path`)                      |

## Architecture

Cargo workspace with two crates:

- **`ssp-core`** — library: parsing, filtering, decoding, record batches
- **`ssp-cli`** — binary (`ssp`): CLI, pipeline orchestration, RPC discovery, DuckDB, live progress

3-stage multithreaded pipeline connected via bounded crossbeam channels:

```
Decompressor thread          4 Parser threads              Writers
─────────────────           ────────────────           ──────────────
zstd → custom tar    →     bytemuck overlay     →     2 account writers
     raw buffers            + filter                   2 decoded writers
                            + token decode             (parquet files)
                                                           │
                                                       DuckDB SQL
```

```
crates/
├── ssp-core/src/
│   ├── lib.rs                          # Public API
│   ├── parser.rs                       # Custom tar parser, AppendVec parsing, stream_raw()
│   ├── filters.rs                      # Account filters (owner/pubkey/hash, dead filtering)
│   ├── pubkey.rs                       # Pubkey type (32 bytes, bytemuck Pod, base58)
│   ├── record_batch.rs                 # Arrow schema, RecordBatch construction
│   └── decoders/
│       ├── mod.rs                      # Decoder trait, COptionPubkey
│       ├── known_mints.rs             # Jupiter verified token list (embedded)
│       └── token_program/
│           ├── mod.rs                  # Mint/TokenAccount structs, COptionU64
│           ├── mint.rs                 # MintDecoder (82-byte accounts)
│           └── token_account.rs        # TokenAccountDecoder (165-byte accounts)
└── ssp-cli/src/
    ├── main.rs                         # CLI args, entry point, live stats printer
    ├── pipeline.rs                     # Pipeline orchestration, threading, PipelineStats
    ├── db.rs                           # DuckDB views, query execution
    ├── rpc.rs                          # RPC node discovery, probing, speed testing (async)
    └── bench.rs                        # Pipeline stage benchmarks
```

### Key design decisions

- **Custom tar parser** — replaced `tar` crate for performance and buffer control (~830 MB/s peak)
- **Zero-copy binary parsing** — `bytemuck` overlays structured types directly onto raw bytes (like Zig's packed struct overlay)
- **Ring buffer pooling** — recycling buffers (`Vec<u8>`) between decompressor and parsers to avoid allocation churn
- **Bounded channel backpressure** — `crossbeam-channel` bounded queues provide backpressure across all pipeline stages
- **Decoder trait** — pluggable token decoding (Mint, TokenAccount), writes to separate parquet files
- **Async (`tokio`) only for RPC discovery** — probing 300+ nodes concurrently; parsing and I/O pipeline use dedicated OS threads
- **Parser accepts `impl Read`** — identical pipeline logic handles both local files and network HTTP streams

## Benchmarks

Reconstructing full ledger states from a 109 GB full snapshot (`snapshot-389758228.tar.zst`, 1.05B+ accounts, ~466 GB decompressed) in **~2 min 10 sec** on commodity hardware (Apple Silicon / M-series baseline):

| Configuration                      | Compressed Throughput (`tar.zst`)   | Decompressed Throughput (Ledger)    | Speedup       |
| ---------------------------------- | ----------------------------------- | ----------------------------------- | ------------- |
| Single-threaded (parse + write)    | ~123 MB/s                           | ~530 MB/s                           | Baseline      |
| Multi-threaded pipeline + filtered | ~840 MB/s sustained (~870 MB/s peak) | ~3.6 GB/s effective (~3.7 GB/s peak)| ~7x speedup   |

Spam filter uses Jupiter's verified token list (4550 mints, embedded at compile time). Only verified mints and their token accounts are decoded. Use `--include-spam` to bypass.

## Roadmap

- [x] Streaming parser (zstd → custom tar → AppendVec)
- [x] Multi-threaded pipeline (decompressor + 4 parsers + 2+2 writers)
- [x] Buffer pooling
- [x] Token Program decoding (Mint + TokenAccount)
- [x] DuckDB integration
- [x] CLI filters (owner, pubkey, hash, dead accounts)
- [x] RPC node discovery + network streaming
- [x] Spam filter (Jupiter verified token list, `--include-spam` to bypass)
- [x] Architecture refactor: extract core as reusable library crate
- [x] CLI with live progress bar and pipeline health stats
- [x] Download-only mode (`--download-full`/`--download-incremental`)
- [ ] UX improvements
  - [ ] Configuration file
  - [ ] DuckDB SQL query interface
- [ ] More decoders (System, Stake, Vote, Token-2022)
- [ ] Custom decoders
  - [ ] Upload a custom ABI
  - [ ] Parse via a dynamic ABI
- [ ] Parallel downloads
- [ ] Incremental snapshot merging
- [ ] Resume on network failure

## Status

In slow development :)

## Stack

Rust 2024, bytemuck, zstd, crossbeam, arrow/parquet, DuckDB, reqwest, tokio, clap
