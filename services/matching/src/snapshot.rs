//! Snapshot files (spec: "Matching Engine → Snapshots").
//!
//! Every [`DEFAULT_SNAPSHOT_INTERVAL`] commands the matcher saves its books,
//! sequence counters, duplicate memory and input offset. A file is written to
//! a temporary name, synced to disk, then renamed, so a crash mid-write leaves
//! only an ignored `.tmp` file. Each file ends with a CRC32 of everything
//! before it, and the newest [`KEEP_SNAPSHOTS`] files are kept so a corrupted
//! newest file falls back to an older one.
//!
//! File layout, version 1 (all integers little-endian):
//!
//! ```text
//! magic        6 bytes  "OXSNAP"
//! version      u16      1
//! partition    u32
//! next_offset  u64
//! dedup_count  u32, then dedup_count × 16-byte command IDs, oldest first
//! book_count   u32, then per book:
//!   symbol_len u8, symbol bytes
//!   last_seq   u64
//!   order_count u32, then per order:
//!     order_id 16 bytes, user_id 16 bytes, side u8 (0 buy, 1 sell),
//!     price_cents i64, remaining i64
//! crc32        u32      over every byte above
//! ```

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use domain::{CommandId, OrderId, Price, Qty, Side, Symbol, UserId};
use order_book::{BookState, RestingView};
use thiserror::Error;

use crate::engine::EngineState;

pub const DEFAULT_SNAPSHOT_INTERVAL: u64 = 100_000;
pub const KEEP_SNAPSHOTS: usize = 3;

const MAGIC: &[u8; 6] = b"OXSNAP";
const VERSION: u16 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("not a snapshot file")]
    BadMagic,
    #[error("unsupported snapshot version {0}")]
    UnsupportedVersion(u16),
    #[error("file is truncated")]
    Truncated,
    #[error("checksum mismatch: file says {stored:#010x}, contents give {actual:#010x}")]
    ChecksumMismatch { stored: u32, actual: u32 },
    #[error("invalid contents: {0}")]
    Invalid(String),
}

// ------------------------------------------------------------------ encoding

pub fn encode(state: &EngineState) -> Vec<u8> {
    let mut w = Vec::with_capacity(64 + state.dedup_ids.len() * 16);
    w.extend_from_slice(MAGIC);
    w.extend_from_slice(&VERSION.to_le_bytes());
    w.extend_from_slice(&state.partition.to_le_bytes());
    w.extend_from_slice(&state.next_offset.to_le_bytes());
    put_len(&mut w, state.dedup_ids.len());
    for id in &state.dedup_ids {
        w.extend_from_slice(id.0.as_bytes());
    }
    put_len(&mut w, state.books.len());
    for b in &state.books {
        let sym = b.symbol.as_str().as_bytes();
        w.push(u8::try_from(sym.len()).expect("symbols are at most 8 bytes"));
        w.extend_from_slice(sym);
        w.extend_from_slice(&b.last_seq.to_le_bytes());
        put_len(&mut w, b.orders.len());
        for o in &b.orders {
            w.extend_from_slice(o.order_id.0.as_bytes());
            w.extend_from_slice(o.user_id.0.as_bytes());
            w.push(match o.side {
                Side::Buy => 0,
                Side::Sell => 1,
            });
            w.extend_from_slice(&o.price.cents().to_le_bytes());
            w.extend_from_slice(&o.remaining.shares().to_le_bytes());
        }
    }
    let crc = crc32fast::hash(&w);
    w.extend_from_slice(&crc.to_le_bytes());
    w
}

fn put_len(w: &mut Vec<u8>, n: usize) {
    let n = u32::try_from(n).expect("snapshot section exceeds u32::MAX entries");
    w.extend_from_slice(&n.to_le_bytes());
}

pub fn decode(bytes: &[u8]) -> Result<EngineState, DecodeError> {
    if bytes.len() < MAGIC.len() + 2 + 4 {
        return Err(DecodeError::Truncated);
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
    let actual = crc32fast::hash(body);
    if stored != actual {
        return Err(DecodeError::ChecksumMismatch { stored, actual });
    }

    let mut r = Reader {
        buf: body,
        pos: MAGIC.len(),
    };
    let version = r.u16()?;
    if version != VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let partition = r.u32()?;
    let next_offset = r.u64()?;

    let dedup_count = r.len(16)?;
    let mut dedup_ids = Vec::with_capacity(dedup_count);
    for _ in 0..dedup_count {
        dedup_ids.push(CommandId(r.uuid()?));
    }

    let book_count = r.len(1)?;
    let mut books = Vec::with_capacity(book_count);
    for _ in 0..book_count {
        let sym_len = r.u8()? as usize;
        let sym = std::str::from_utf8(r.take(sym_len)?)
            .map_err(|_| DecodeError::Invalid("symbol is not UTF-8".into()))?;
        let symbol = Symbol::new(sym).map_err(|e| DecodeError::Invalid(e.to_string()))?;
        let last_seq = r.u64()?;
        let order_count = r.len(16 + 16 + 1 + 8 + 8)?;
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            let order_id = OrderId(r.uuid()?);
            let user_id = UserId(r.uuid()?);
            let side = match r.u8()? {
                0 => Side::Buy,
                1 => Side::Sell,
                s => return Err(DecodeError::Invalid(format!("side byte {s}"))),
            };
            let price = Price::new(r.i64()?).map_err(|e| DecodeError::Invalid(e.to_string()))?;
            let remaining = Qty::new(r.i64()?).map_err(|e| DecodeError::Invalid(e.to_string()))?;
            orders.push(RestingView {
                order_id,
                user_id,
                side,
                price,
                remaining,
            });
        }
        books.push(BookState {
            symbol,
            last_seq,
            orders,
        });
    }
    if r.pos != body.len() {
        return Err(DecodeError::Invalid(format!(
            "{} unexpected trailing bytes",
            body.len() - r.pos
        )));
    }
    Ok(EngineState {
        partition,
        next_offset,
        dedup_ids,
        books,
    })
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        let s = self.buf.get(self.pos..end).ok_or(DecodeError::Truncated)?;
        self.pos = end;
        Ok(s)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }
    fn uuid(&mut self) -> Result<uuid::Uuid, DecodeError> {
        Ok(uuid::Uuid::from_bytes(self.array()?))
    }
    /// A u32 count, refused up front if the entries could not fit in what is
    /// left of the file (so a corrupt count cannot trigger a huge allocation).
    fn len(&mut self, min_entry_bytes: usize) -> Result<usize, DecodeError> {
        let n = self.u32()? as usize;
        let left = self.buf.len() - self.pos;
        if n.saturating_mul(min_entry_bytes) > left {
            return Err(DecodeError::Truncated);
        }
        Ok(n)
    }
}

// --------------------------------------------------------------------- store

/// A snapshot that could not be used, and why.
#[derive(Debug)]
pub struct Skipped {
    pub path: PathBuf,
    pub reason: String,
}

#[derive(Debug)]
pub struct Loaded {
    pub state: EngineState,
    pub path: PathBuf,
}

/// The snapshot directory of one partition.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
    partition: u32,
    keep: usize,
}

impl SnapshotStore {
    pub fn new(dir: impl Into<PathBuf>, partition: u32) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            partition,
            keep: KEEP_SNAPSHOTS,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn prefix(&self) -> String {
        format!("snapshot-p{:03}-", self.partition)
    }

    fn file_name(&self, next_offset: u64) -> String {
        // Zero-padded so name order equals offset order.
        format!("{}{next_offset:020}.snap", self.prefix())
    }

    /// Writes a snapshot durably and prunes old ones. Returns its path.
    pub fn save(&self, state: &EngineState) -> io::Result<PathBuf> {
        assert_eq!(
            state.partition, self.partition,
            "snapshot for the wrong partition"
        );
        let name = self.file_name(state.next_offset);
        let final_path = self.dir.join(&name);
        let tmp_path = self.dir.join(format!("{name}.tmp"));

        let bytes = encode(state);
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.dir)?;
        self.prune()?;
        Ok(final_path)
    }

    /// Snapshot files for this partition, oldest first.
    pub fn list(&self) -> io::Result<Vec<PathBuf>> {
        let prefix = self.prefix();
        let mut files: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix) && n.ends_with(".snap"))
            })
            .collect();
        files.sort();
        Ok(files)
    }

    /// Keeps the newest `keep` snapshots and deletes leftover temporary files.
    fn prune(&self) -> io::Result<()> {
        let files = self.list()?;
        let excess = files.len().saturating_sub(self.keep);
        for old in &files[..excess] {
            fs::remove_file(old)?;
        }
        let prefix = self.prefix();
        for entry in fs::read_dir(&self.dir)?.filter_map(Result::ok) {
            let p = entry.path();
            let is_tmp = p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix) && n.ends_with(".snap.tmp"));
            if is_tmp {
                fs::remove_file(&p)?;
            }
        }
        Ok(())
    }

    /// The newest snapshot that reads, checks and decodes cleanly, plus every
    /// newer one that was skipped. `None` means no usable snapshot exists.
    pub fn load_latest(&self) -> io::Result<(Option<Loaded>, Vec<Skipped>)> {
        let mut skipped = Vec::new();
        for path in self.list()?.into_iter().rev() {
            let reason = match fs::read(&path) {
                Err(e) => e.to_string(),
                Ok(bytes) => match decode(&bytes) {
                    Ok(state) if state.partition == self.partition => {
                        return Ok((Some(Loaded { state, path }), skipped));
                    }
                    Ok(state) => format!("belongs to partition {}", state.partition),
                    Err(e) => e.to_string(),
                },
            };
            skipped.push(Skipped { path, reason });
        }
        Ok((None, skipped))
    }
}

/// Makes the rename itself durable. Windows cannot open a directory as a file;
/// NTFS journals the rename's metadata, so there it is a no-op.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}
