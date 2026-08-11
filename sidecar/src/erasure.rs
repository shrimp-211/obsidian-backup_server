//! Reed-Solomon (8+2) erasure coding for the CAS object store.
//!
//! Each object is split into 8 equal-size data shards; two parity shards
//! are derived over GF(2^8) with a Reed-Solomon code. Because RS is an
//! *erasure* code, a corrupted/lost shard can be reconstructed as long as
//! no more than 2 of the 10 shards are missing. `verify repair` uses this
//! to self-heal damaged objects without keeping redundant copies.
//!
//! Storage layout (see `storage/object_store.rs`):
//!   objects/{hash}/d0..d7   — 8 data shard files (padded to equal size)
//!   packfiles/parity/{hash} — header (sizes + 10 shard hashes) + 2 parity shards

use std::cell::RefCell;

use anyhow::{bail, Result};
use reed_solomon_erasure::galois_8::ReedSolomon;

pub const DATA_SHARDS: usize = 8;
pub const PARITY_SHARDS: usize = 2;
pub const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;

/// A Reed-Solomon (8, 2) codec operating over GF(2^8).
///
/// `&self` methods are provided via an internal `RefCell`; ReedSolomon
/// requires `&mut self` to encode/reconstruct, but the codec is stateless
/// between calls so interior mutability is safe here.
pub struct ErasureCodec {
    rs: RefCell<ReedSolomon>,
}

/// The encoded form of an object: 8 data shards + 2 parity shards.
pub struct EncodedObject {
    pub data_shards: Vec<Vec<u8>>,
    pub parity_shards: Vec<Vec<u8>>,
    pub shard_size: usize,
    pub original_size: usize,
}

impl ErasureCodec {
    pub fn new() -> Result<Self> {
        Ok(Self {
            rs: RefCell::new(ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS)?),
        })
    }

    /// Split `data` into 8 equal-size shards and derive the 2 parity shards.
    ///
    /// The final data shard is zero-padded so all shards have identical
    /// length `shard_size = ceil(len / 8)`. Callers must truncate the
    /// reassembled stream back to `original_size` on decode.
    pub fn encode(&self, data: &[u8]) -> Result<EncodedObject> {
        let original_size = data.len();
        let shard_size = original_size.div_ceil(DATA_SHARDS);
        if shard_size == 0 {
            bail!("cannot erasure-code an empty object");
        }

        // Pad the payload so every shard has exactly `shard_size` bytes.
        let padded_len = shard_size * DATA_SHARDS;
        let mut padded = data.to_vec();
        padded.resize(padded_len, 0u8);

        let mut shards: Vec<Vec<u8>> = padded.chunks(shard_size).map(|c| c.to_vec()).collect();
        // Parity shards start as zero-filled buffers; `encode` fills them in.
        shards.resize(TOTAL_SHARDS, vec![0u8; shard_size]);

        {
            let mut refs: Vec<&mut [u8]> =
                shards.iter_mut().map(|v| v.as_mut_slice()).collect();
            self.rs.borrow_mut().encode(&mut refs)?;
        }

        let parity_shards = shards.split_off(DATA_SHARDS);
        Ok(EncodedObject {
            data_shards: shards,
            parity_shards,
            shard_size,
            original_size,
        })
    }

    /// Reconstruct corrupted or missing shards.
    ///
    /// Pass a corrupted shard as `None`; it is filled back in on return.
    /// Reconstruction requires that at most `PARITY_SHARDS` shards are
    /// missing — any of the remaining 8 good shards can rebuild the others.
    pub fn reconstruct(
        &self,
        data_shards: Vec<Option<Vec<u8>>>,
        parity_shards: Vec<Option<Vec<u8>>>,
        shard_size: usize,
    ) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
        let missing = data_shards
            .iter()
            .chain(&parity_shards)
            .filter(|s| s.is_none())
            .count();
        if missing > PARITY_SHARDS {
            bail!(
                "too many corrupted shards ({}) to reconstruct (max {})",
                missing,
                PARITY_SHARDS
            );
        }
        if missing == 0 {
            let data = data_shards.into_iter().map(|s| s.unwrap()).collect();
            let parity = parity_shards.into_iter().map(|s| s.unwrap()).collect();
            return Ok((data, parity));
        }

        let mut all: Vec<Option<Vec<u8>>> =
            data_shards.into_iter().chain(parity_shards).collect();
        debug_assert_eq!(all.len(), TOTAL_SHARDS);
        // Sanity-check shard lengths before handing them to the codec.
        for shard in all.iter().flatten() {
            if shard.len() != shard_size {
                bail!(
                    "shard length {} does not match expected shard_size {}",
                    shard.len(),
                    shard_size
                );
            }
        }

        // `reconstruct` requires `Option<Vec<u8>>` shards (Vec<u8> satisfies
        // `FromIterator<u8>`), not `Option<&mut [u8]>`.
        self.rs.borrow_mut().reconstruct(&mut all)?;

        let data = all[..DATA_SHARDS]
            .iter()
            .map(|s| s.clone().unwrap())
            .collect();
        let parity = all[DATA_SHARDS..]
            .iter()
            .map(|s| s.clone().unwrap())
            .collect();
        Ok((data, parity))
    }
}

impl Default for ErasureCodec {
    fn default() -> Self {
        Self::new().expect("RS(8+2) codec construction cannot fail")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_roundtrip() {
        let codec = ErasureCodec::new().unwrap();
        let data = b"the quick brown fox jumps over the lazy dog - content defined chunking backup".to_vec();
        let enc = codec.encode(&data).unwrap();
        assert_eq!(enc.data_shards.len(), DATA_SHARDS);
        assert_eq!(enc.parity_shards.len(), PARITY_SHARDS);

        let mut reassembled = Vec::new();
        for s in &enc.data_shards {
            reassembled.extend_from_slice(s);
        }
        reassembled.truncate(enc.original_size);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_reconstruct_single_data_shard() {
        let codec = ErasureCodec::new().unwrap();
        let data = vec![7u8; 200_000];
        let enc = codec.encode(&data).unwrap();

        let mut data_shards: Vec<Option<Vec<u8>>> =
            enc.data_shards.into_iter().map(Some).collect();
        let expected = data_shards[3].clone().unwrap();
        data_shards[3] = None; // simulate corruption of shard 3

        let (recovered, _) = codec
            .reconstruct(
                data_shards,
                enc.parity_shards.into_iter().map(Some).collect(),
                enc.shard_size,
            )
            .unwrap();
        assert_eq!(recovered[3], expected);
    }

    #[test]
    fn test_reconstruct_two_lost_parity_shards() {
        let codec = ErasureCodec::new().unwrap();
        let data = vec![42u8; 100_000];
        let enc = codec.encode(&data).unwrap();

        let mut parity: Vec<Option<Vec<u8>>> =
            enc.parity_shards.into_iter().map(Some).collect();
        parity[0] = None;
        parity[1] = None;

        let (_, recovered) = codec
            .reconstruct(
                enc.data_shards.into_iter().map(Some).collect(),
                parity,
                enc.shard_size,
            )
            .unwrap();
        assert!(recovered.iter().all(|s| !s.is_empty()));
        assert_eq!(recovered[0].len(), enc.shard_size);
    }

    #[test]
    fn test_reconstruct_one_data_and_one_parity_lost() {
        let codec = ErasureCodec::new().unwrap();
        let data = vec![9u8; 64 * 1024];
        let enc = codec.encode(&data).unwrap();

        let mut data_shards: Vec<Option<Vec<u8>>> =
            enc.data_shards.into_iter().map(Some).collect();
        data_shards[5] = None;
        let mut parity: Vec<Option<Vec<u8>>> =
            enc.parity_shards.into_iter().map(Some).collect();
        parity[1] = None;

        let (recovered_data, recovered_parity) = codec
            .reconstruct(data_shards, parity, enc.shard_size)
            .unwrap();
        // Reassembling the repaired data shards yields the original payload.
        let mut reassembled = Vec::new();
        for s in &recovered_data {
            reassembled.extend_from_slice(s);
        }
        reassembled.truncate(enc.original_size);
        assert_eq!(reassembled, data);
        assert!(recovered_parity.iter().all(|s| !s.is_empty()));
    }

    #[test]
    fn test_reconstruct_too_many_corruptions_fails() {
        let codec = ErasureCodec::new().unwrap();
        let data = vec![1u8; 50_000];
        let enc = codec.encode(&data).unwrap();

        let mut data_shards: Vec<Option<Vec<u8>>> =
            enc.data_shards.into_iter().map(Some).collect();
        data_shards[0] = None;
        data_shards[1] = None;
        data_shards[2] = None; // 3 > PARITY_SHARDS → unrecoverable

        let res = codec.reconstruct(
            data_shards,
            enc.parity_shards.into_iter().map(Some).collect(),
            enc.shard_size,
        );
        assert!(res.is_err());
    }
}
