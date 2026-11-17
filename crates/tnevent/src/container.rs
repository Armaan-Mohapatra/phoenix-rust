//! TNEVENT segment container (read-only, bounded, fully checksummed).
//!
//! Reverse-engineered from finalized collector segments and validated against their
//! manifests (441 v2 and 22 v1 segments on the research host; 15 sampled segments matched
//! `event_count` and `sha256` exactly with zero CRC failures).
//!
//! ```text
//! file      = "TNEVENT\0" u16be(version) body
//! v1 body   = event*
//! v2 body   = block*
//! block     = 0x01  u32be(L)  u32be(raw_len)  u32be(crc32(raw))  zlib_stream[L]
//! raw       = event*                       (decompressed block)
//! event     = u32be(total) u32be(crc32(rest)) rest[total]
//! rest      = u32be(meta_len) meta_json[meta_len] message[total-4-meta_len]
//! ```
//! `meta_json` is a JSON array (see `event::Meta`); `message` is the venue's original wire
//! bytes. Nothing here interprets the message.
use crate::event::EventRef;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub const MAGIC: &[u8; 8] = b"TNEVENT\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TnError {
    Io(String),
    BadMagic,
    UnsupportedVersion(u16),
    Truncated(&'static str),
    BadBlock { block: u64, why: &'static str },
    BadEvent { event: u64, why: &'static str },
    ManifestMismatch(&'static str),
    Manifest(String),
    Meta { event: u64, why: String },
}

impl From<std::io::Error> for TnError {
    fn from(e: std::io::Error) -> Self {
        TnError::Io(e.to_string())
    }
}

/// Hard limits; every length read from the file is checked against these first.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_event_bytes: u32,
    pub max_block_compressed: u32,
    pub max_block_raw: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_event_bytes: 4 * 1024 * 1024,
            max_block_compressed: 32 * 1024 * 1024,
            max_block_raw: 128 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub segment_id: u64,
    pub segment_format_version: u16,
    pub event_count: u64,
    pub file_bytes: u64,
    pub record_bytes: u64,
    pub sha256: String,
    #[serde(default)]
    pub max_event_bytes: u64,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub finalized_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VisitStats {
    pub events: u64,
    pub blocks: u64,
    pub raw_bytes: u64,
}

/// Reads one `segment-N.ready` directory. The caller must only pass finalized segments
/// (`.ready`); open segments are never read.
pub struct Segment {
    dir: std::path::PathBuf,
    pub manifest: Manifest,
    pub limits: Limits,
}

impl Segment {
    pub fn open(dir: &Path, limits: Limits) -> Result<Self, TnError> {
        if !dir.to_string_lossy().ends_with(".ready") {
            return Err(TnError::Manifest(
                "refusing a segment directory that is not *.ready".into(),
            ));
        }
        let text = std::fs::read_to_string(dir.join("manifest.json"))?;
        let manifest: Manifest =
            serde_json::from_str(&text).map_err(|e| TnError::Manifest(e.to_string()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            manifest,
            limits,
        })
    }

    /// Streaming SHA-256 and size of `events.bin` against the manifest.
    pub fn verify_hash(&self) -> Result<(), TnError> {
        let mut f = File::open(self.dir.join("events.bin"))?;
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut n = 0u64;
        loop {
            let k = f.read(&mut buf)?;
            if k == 0 {
                break;
            }
            h.update(&buf[..k]);
            n += k as u64;
        }
        if n != self.manifest.file_bytes {
            return Err(TnError::ManifestMismatch("file size"));
        }
        if format!("{:x}", h.finalize()) != self.manifest.sha256 {
            return Err(TnError::ManifestMismatch("sha256"));
        }
        Ok(())
    }

    /// Visits every event in order. Verifies block length/CRC, per-event CRC, structural
    /// bounds and finally the manifest event count. Any failure aborts with context.
    pub fn visit(
        &self,
        f: &mut dyn FnMut(EventRef<'_>) -> Result<(), TnError>,
    ) -> Result<VisitStats, TnError> {
        let file = File::open(self.dir.join("events.bin"))?;
        visit_stream(
            BufReader::with_capacity(1 << 20, file),
            self.limits,
            Some(self.manifest.event_count),
            f,
        )
    }
}

fn read_exact_or<R: Read>(r: &mut R, buf: &mut [u8], what: &'static str) -> Result<(), TnError> {
    r.read_exact(buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            TnError::Truncated(what)
        } else {
            e.into()
        }
    })
}

/// Reads a byte; `None` on clean EOF.
fn read_opt_byte<R: Read>(r: &mut R) -> Result<Option<u8>, TnError> {
    let mut b = [0u8; 1];
    loop {
        match r.read(&mut b) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(b[0])),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Core reader over any byte stream (used by tests with in-memory segments).
pub fn visit_stream<R: Read>(
    mut r: R,
    limits: Limits,
    expect_events: Option<u64>,
    f: &mut dyn FnMut(EventRef<'_>) -> Result<(), TnError>,
) -> Result<VisitStats, TnError> {
    let mut head = [0u8; 10];
    read_exact_or(&mut r, &mut head, "file header")?;
    if &head[..8] != MAGIC {
        return Err(TnError::BadMagic);
    }
    let version = u16::from_be_bytes([head[8], head[9]]);
    let mut stats = VisitStats::default();
    match version {
        1 => {
            // Events directly, no blocks. Bounded by max_event_bytes per event.
            let mut buf = Vec::new();
            while let Some(first) = read_opt_byte(&mut r)? {
                let mut len4 = [first, 0, 0, 0];
                read_exact_or(&mut r, &mut len4[1..], "event length")?;
                let total = u32::from_be_bytes(len4);
                if total < 8 || total > limits.max_event_bytes {
                    return Err(TnError::BadEvent {
                        event: stats.events,
                        why: "event length out of bounds",
                    });
                }
                buf.clear();
                buf.extend_from_slice(&len4);
                buf.resize(8 + total as usize, 0);
                read_exact_or(&mut r, &mut buf[4..], "event body")?;
                let consumed = emit_events(&buf, stats.events, limits, f)?;
                debug_assert_eq!(consumed, 1);
                stats.events += consumed;
                stats.raw_bytes += buf.len() as u64;
            }
        }
        2 => {
            let mut raw = Vec::new();
            let mut comp = Vec::new();
            while let Some(kind) = read_opt_byte(&mut r)? {
                let block = stats.blocks;
                if kind != 1 {
                    return Err(TnError::BadBlock {
                        block,
                        why: "unknown record type",
                    });
                }
                let mut hdr = [0u8; 12];
                read_exact_or(&mut r, &mut hdr, "block header")?;
                let l = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
                let raw_len = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
                let crc = u32::from_be_bytes(hdr[8..12].try_into().unwrap());
                if l == 0 || l > limits.max_block_compressed || raw_len > limits.max_block_raw {
                    return Err(TnError::BadBlock {
                        block,
                        why: "block length out of bounds",
                    });
                }
                comp.clear();
                comp.resize(l as usize, 0);
                read_exact_or(&mut r, &mut comp, "block body")?;
                raw.clear();
                // Bounded inflate: never allocate beyond the declared (and capped) length + 1.
                let mut z = flate2::read::ZlibDecoder::new(&comp[..]).take(u64::from(raw_len) + 1);
                z.read_to_end(&mut raw).map_err(|_| TnError::BadBlock {
                    block,
                    why: "zlib stream invalid",
                })?;
                if raw.len() != raw_len as usize {
                    return Err(TnError::BadBlock {
                        block,
                        why: "raw length mismatch",
                    });
                }
                if crc32fast::hash(&raw) != crc {
                    return Err(TnError::BadBlock {
                        block,
                        why: "raw crc32 mismatch",
                    });
                }
                stats.events += emit_events(&raw, stats.events, limits, f)?;
                stats.blocks += 1;
                stats.raw_bytes += raw.len() as u64;
            }
        }
        v => return Err(TnError::UnsupportedVersion(v)),
    }
    if let Some(n) = expect_events {
        if n != stats.events {
            return Err(TnError::ManifestMismatch("event count"));
        }
    }
    Ok(stats)
}

/// Walks a buffer of back-to-back events; the buffer must tile exactly.
fn emit_events(
    raw: &[u8],
    first_index: u64,
    limits: Limits,
    f: &mut dyn FnMut(EventRef<'_>) -> Result<(), TnError>,
) -> Result<u64, TnError> {
    let (mut p, mut n) = (0usize, 0u64);
    while p < raw.len() {
        let event = first_index + n;
        let bad = |why| TnError::BadEvent { event, why };
        if raw.len() - p < 12 {
            return Err(bad("truncated event header"));
        }
        let total = u32::from_be_bytes(raw[p..p + 4].try_into().unwrap());
        let crc = u32::from_be_bytes(raw[p + 4..p + 8].try_into().unwrap());
        if total < 4 || total > limits.max_event_bytes {
            return Err(bad("event length out of bounds"));
        }
        let end = p + 8 + total as usize;
        if end > raw.len() {
            return Err(bad("event overruns block"));
        }
        if crc32fast::hash(&raw[p + 8..end]) != crc {
            return Err(bad("event crc32 mismatch"));
        }
        let mlen = u32::from_be_bytes(raw[p + 8..p + 12].try_into().unwrap()) as usize;
        if mlen > total as usize - 4 {
            return Err(bad("meta length exceeds event"));
        }
        let meta = &raw[p + 12..p + 12 + mlen];
        let msg = &raw[p + 12 + mlen..end];
        f(EventRef {
            index: event,
            meta,
            msg,
        })?;
        n += 1;
        p = end;
    }
    Ok(n)
}

#[cfg(test)]
pub(crate) mod testutil {
    use flate2::write::ZlibEncoder;
    use std::io::Write;

    pub fn event(meta: &str, msg: &str) -> Vec<u8> {
        let mut rest = (meta.len() as u32).to_be_bytes().to_vec();
        rest.extend_from_slice(meta.as_bytes());
        rest.extend_from_slice(msg.as_bytes());
        let mut e = (rest.len() as u32).to_be_bytes().to_vec();
        e.extend_from_slice(&crc32fast::hash(&rest).to_be_bytes());
        e.extend_from_slice(&rest);
        e
    }
    pub fn v1(events: &[Vec<u8>]) -> Vec<u8> {
        let mut out = b"TNEVENT\0\0\x01".to_vec();
        events.iter().for_each(|e| out.extend_from_slice(e));
        out
    }
    pub fn v2(blocks: &[Vec<Vec<u8>>]) -> Vec<u8> {
        let mut out = b"TNEVENT\0\0\x02".to_vec();
        for b in blocks {
            let raw: Vec<u8> = b.concat();
            let mut z = ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            z.write_all(&raw).unwrap();
            let comp = z.finish().unwrap();
            out.push(1);
            out.extend_from_slice(&(comp.len() as u32).to_be_bytes());
            out.extend_from_slice(&(raw.len() as u32).to_be_bytes());
            out.extend_from_slice(&crc32fast::hash(&raw).to_be_bytes());
            out.extend_from_slice(&comp);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;

    fn count(bytes: &[u8], expect: Option<u64>) -> Result<u64, TnError> {
        visit_stream(bytes, Limits::default(), expect, &mut |_| Ok(())).map(|s| s.events)
    }
    fn evs() -> Vec<Vec<u8>> {
        (0..5)
            .map(|i| event(&format!("[1,\"k\",\"T{i}\"]"), &format!("{{\"i\":{i}}}")))
            .collect()
    }

    #[test]
    fn v1_and_v2_decode_the_same_events() {
        let e = evs();
        assert_eq!(count(&v1(&e), Some(5)), Ok(5));
        assert_eq!(
            count(&v2(&[e[..2].to_vec(), e[2..].to_vec()]), Some(5)),
            Ok(5)
        );
        let mut seen = vec![];
        visit_stream(
            &v2(std::slice::from_ref(&e))[..],
            Limits::default(),
            None,
            &mut |ev| {
                seen.push((ev.index, ev.meta.to_vec(), ev.msg.to_vec()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen[3].1, b"[1,\"k\",\"T3\"]");
        assert_eq!(seen[3].2, b"{\"i\":3}");
    }

    #[test]
    fn manifest_count_mismatch_is_an_error() {
        assert_eq!(
            count(&v1(&evs()), Some(6)),
            Err(TnError::ManifestMismatch("event count"))
        );
    }

    #[test]
    fn corruption_is_detected_never_ignored() {
        let mut b = v1(&evs());
        let n = b.len();
        b[n - 3] ^= 1; // payload byte of the last event
        assert!(matches!(
            count(&b, None),
            Err(TnError::BadEvent {
                why: "event crc32 mismatch",
                ..
            })
        ));
        let mut z = v2(&[evs()]);
        let n = z.len();
        z[n / 2] ^= 0x10; // inside the zlib stream
        assert!(matches!(count(&z, None), Err(TnError::BadBlock { .. })));
        let mut c = v2(&[evs()]);
        c[10 + 1 + 4 + 4 + 3] ^= 1; // block crc field
        assert!(matches!(
            count(&c, None),
            Err(TnError::BadBlock {
                why: "raw crc32 mismatch",
                ..
            })
        ));
    }

    #[test]
    fn truncation_bad_magic_and_versions_are_refused() {
        let b = v2(&[evs()]);
        assert!(matches!(
            count(&b[..b.len() - 5], None),
            Err(TnError::Truncated(_) | TnError::BadBlock { .. })
        ));
        assert_eq!(count(b"NOTAEVENT\0\0", None), Err(TnError::BadMagic));
        assert_eq!(
            count(b"TNEVENT\0\0\x07", None),
            Err(TnError::UnsupportedVersion(7))
        );
        assert!(matches!(
            count(&v1(&evs())[..14], None),
            Err(TnError::Truncated(_))
        ));
    }

    #[test]
    fn hostile_lengths_do_not_allocate() {
        // v1 event claiming 4 GiB, v2 block claiming 4 GiB compressed and 4 GiB raw.
        let mut b = b"TNEVENT\0\0\x01".to_vec();
        b.extend_from_slice(&0xFFFF_FFF0u32.to_be_bytes());
        b.extend_from_slice(&[0; 16]);
        assert!(matches!(
            count(&b, None),
            Err(TnError::BadEvent {
                why: "event length out of bounds",
                ..
            })
        ));
        let mut z = b"TNEVENT\0\0\x02\x01".to_vec();
        z.extend_from_slice(&0xFFFF_FFF0u32.to_be_bytes());
        z.extend_from_slice(&[0; 16]);
        assert!(matches!(
            count(&z, None),
            Err(TnError::BadBlock {
                why: "block length out of bounds",
                ..
            })
        ));
        // zip-bomb shape: tiny stream, declared raw length smaller than the real inflated size.
        let raw = vec![0u8; 1 << 20];
        let mut zz = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
        std::io::Write::write_all(&mut zz, &raw).unwrap();
        let comp = zz.finish().unwrap();
        let mut bomb = b"TNEVENT\0\0\x02\x01".to_vec();
        bomb.extend_from_slice(&(comp.len() as u32).to_be_bytes());
        bomb.extend_from_slice(&16u32.to_be_bytes());
        bomb.extend_from_slice(&crc32fast::hash(&raw[..16]).to_be_bytes());
        bomb.extend_from_slice(&comp);
        assert!(matches!(
            count(&bomb, None),
            Err(TnError::BadBlock {
                why: "raw length mismatch",
                ..
            })
        ));
    }

    #[test]
    fn meta_length_beyond_event_is_refused() {
        let mut e = event("[1]", "{}");
        e[8..12].copy_from_slice(&1000u32.to_be_bytes()); // corrupt mlen, then fix the crc so only the bound catches it
        let crc = crc32fast::hash(&e[8..]);
        e[4..8].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            count(&v1(&[e]), None),
            Err(TnError::BadEvent {
                why: "meta length exceeds event",
                ..
            })
        ));
    }
}
