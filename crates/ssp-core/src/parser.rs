use {
    crate::{Pubkey, filters::ResolvedFilters},
    arrow::array::RecordBatch,
    bytemuck::{Pod, Zeroable},
    crossbeam::channel::{Receiver, Sender},
    foldhash::HashMap,
    std::{
        fmt,
        io::{BufReader, Read},
        sync::atomic::{AtomicU64, Ordering},
    },
};

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct AccountHeader {
    pub write_version: u64,
    pub data_len: u64,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub owner: Pubkey,
    pub executable: u8,
    pub padding: [u8; 7],
    pub hash: [u8; 32],
}
impl fmt::Display for AccountHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "version: {}, data_len: {}, pubkey: {}, lamports: {}, \
             rent_epoch: {}, owner: {}, executable: {}, hash: {}",
            self.write_version,
            self.data_len,
            self.pubkey,
            self.lamports,
            self.rent_epoch,
            self.owner,
            self.executable,
            bs58::encode(&self.hash).into_string(),
        )
    }
}

pub const TAR_BLOCK: usize = 512;

/// Parse octal ASCII (tar stores sizes as octal strings).
pub fn parse_octal(bytes: &[u8]) -> u64 {
    // GNU tar extension: if the high bit is set, it's binary big-endian
    if !bytes.is_empty() && bytes[0] & 0x80 != 0 {
        return bytes[1..]
            .iter()
            .fold(0u64, |acc, &b| (acc << 8) | b as u64);
    }
    let mut n = 0u64;
    for &b in bytes {
        if b == 0 || b == b' ' {
            break;
        }
        n = n * 8 + (b - b'0') as u64;
    }
    n
}

/// Check if tar header path (bytes 0..100) contains "accounts/".
pub fn is_accounts_entry(header: &[u8; TAR_BLOCK]) -> bool {
    // type flag: byte 156, '0' or '\0' = regular file
    let type_flag = header[156];
    if type_flag != b'0' && type_flag != 0 {
        return false;
    }
    header[..100].windows(9).any(|w| w == b"accounts/")
}

impl AccountHeader {
    /// Stage 1: zstd → lightweight tar → send raw buffers.
    ///
    /// Buffers are sent as `(buf, size)` where `buf[..size]` is valid data.
    /// Buffer length never shrinks, so the zero-fill cost of `resize` is paid
    /// once per buffer high-water mark instead of on every reuse.
    pub fn stream_raw(
        reader: impl Read + Send,
        raw_tx: Sender<(Vec<u8>, usize)>,
        recycle_rx: Receiver<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let buffered = BufReader::with_capacity(4 * 1024 * 1024, reader);
        let mut decoder = zstd::Decoder::with_buffer(buffered)?;
        decoder.window_log_max(31)?;

        let mut header = [0u8; TAR_BLOCK];
        let mut skip_buf = [0u8; 65536];

        loop {
            match decoder.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Zero block = end of archive (name starts with NUL)
            if header[0] == 0 {
                break;
            }

            let size = parse_octal(&header[124..136]) as usize;
            let padded = (size + TAR_BLOCK - 1) & !(TAR_BLOCK - 1);

            if is_accounts_entry(&header) {
                let mut buf: Vec<u8> = recycle_rx.recv()?;
                if buf.len() < size {
                    buf.resize(size, 0);
                }
                decoder.read_exact(&mut buf[..size])?;

                // Skip padding bytes to next 512 boundary
                let padding = padded - size;
                if padding > 0 {
                    decoder.read_exact(&mut skip_buf[..padding])?;
                }

                raw_tx.send((buf, size))?;
            } else {
                // Skip entry data efficiently
                let mut remaining = padded;
                while remaining > 0 {
                    let chunk = remaining.min(skip_buf.len());
                    decoder.read_exact(&mut skip_buf[..chunk])?;
                    remaining -= chunk;
                }
            }
        }

        Ok(())
    }

    /// Stage 2: parse raw AppendVec buffer into filtered account headers + decoded batches.
    /// Accumulates accounts in `chunk` and emits `RecordBatch`es of 65,536 accounts directly into `tx`.
    pub fn parse_accounts(
        buf: &[u8],
        filters: &ResolvedFilters,
        decoders: &mut [Box<dyn crate::decoders::Decoder>],
        decoder_map: &HashMap<Pubkey, Vec<usize>>,
        decoded_tx: &Sender<(&'static str, RecordBatch)>,
        blocked_decoded: &AtomicU64,
        tx: &Sender<RecordBatch>,
        blocked_tx: &AtomicU64,
        chunk: &mut Vec<AccountHeader>,
    ) -> anyhow::Result<()> {
        let mut offset = 0;

        while offset + size_of::<AccountHeader>() <= buf.len() {
            let header = bytemuck::from_bytes::<AccountHeader>(
                &buf[offset..offset + size_of::<AccountHeader>()],
            );

            offset += size_of::<AccountHeader>();

            let data_end = offset + header.data_len as usize;
            if data_end > buf.len() {
                break;
            }
            let data = &buf[offset..data_end];

            offset = (data_end + 7) & !7;

            // O(1) lookup by owner — skips entirely for programs without decoders
            if let Some(indices) = decoder_map.get(&header.owner) {
                for &idx in indices {
                    if decoders[idx].matches(&header.owner, header.data_len) {
                        if let Some(batch) =
                            decoders[idx].decode(header.pubkey, data, filters.include_spam)
                        {
                            if decoded_tx.is_full() {
                                blocked_decoded.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = decoded_tx.send((decoders[idx].name(), batch));
                        }
                        break;
                    }
                }
            }

            if !filters.matches(header) {
                continue;
            }

            chunk.push(*header);
            if chunk.len() >= 65_536 {
                let record_batch = crate::record_batch::build_record_batch(chunk)?;
                chunk.clear();
                if tx.is_full() {
                    blocked_tx.fetch_add(1, Ordering::Relaxed);
                }
                tx.send(record_batch)?;
            }
        }

        Ok(())
    }
}

