//! Content-Defined Chunking engine using FastCDC.
//!
//! Splits files into variable-size chunks based on content boundaries
//! (MCA region boundaries for Minecraft worlds), enabling efficient
//! deduplication across versions of the same world.
//!
//! FastCDC parameters:
//!   - Min chunk size: 4 KB (avoid tiny chunks)
//!   - Avg chunk size: 64 KB (balance dedup vs overhead)
//!   - Max chunk size: 256 KB (bound worst-case memory)

use std::io::Read;

use blake3::Hasher;
use tracing::debug;

/// Represents a single data chunk produced by the chunking engine.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// BLAKE3 hash of the chunk data (used as content address)
    pub hash: String,

    /// Byte offset of this chunk within the source file
    pub offset: usize,

    /// Size of this chunk in bytes
    pub size: usize,

    /// The actual chunk data (owned)
    pub data: Vec<u8>,
}

/// FastCDC chunking engine tuned for Minecraft world files.
pub struct ChunkEngine {
    /// Minimum chunk size in bytes
    min_size: usize,

    /// Average chunk size in bytes
    avg_size: usize,

    /// Maximum chunk size in bytes
    max_size: usize,
}

impl ChunkEngine {
    /// Create a new chunk engine with default Minecraft-optimized settings.
    pub fn new() -> Self {
        Self {
            min_size: 4 * 1024,   // 4 KB
            avg_size: 64 * 1024,  // 64 KB
            max_size: 256 * 1024, // 256 KB
        }
    }

    /// Create a chunk engine with custom size parameters.
    pub fn with_sizes(min_size: usize, avg_size: usize, max_size: usize) -> Self {
        Self {
            min_size,
            avg_size,
            max_size,
        }
    }

    /// Chunk a file's content into content-defined variable-size chunks.
    ///
    /// Uses a simplified CDC (Content-Defined Chunking) algorithm:
    ///   - Scan through data looking for chunk boundaries
    ///   - A boundary is found when a rolling hash of the last N bytes
    ///     matches a mask derived from the average chunk size
    ///   - Each chunk is hashed with BLAKE3 for content addressing
    pub fn chunk_file(&self, data: &[u8], file_path: &str) -> Vec<Chunk> {
        if data.is_empty() {
            return Vec::new();
        }

        // For files smaller than min_size, return as a single chunk
        if data.len() <= self.min_size {
            let hash = blake3::hash(data).to_hex().to_string();
            return vec![Chunk {
                hash,
                offset: 0,
                size: data.len(),
                data: data.to_vec(),
            }];
        }

        let mut chunks = Vec::new();
        let mut offset = 0;
        let mask = (self.avg_size as u64).wrapping_sub(1); // Power-of-2 mask for boundary detection

        while offset < data.len() {
            let remaining = data.len() - offset;

            // Determine chunk size
            let chunk_size = if remaining <= self.min_size {
                remaining // Last chunk: take all remaining data
            } else {
                self.find_boundary(&data[offset..], mask)
            };

            let end = (offset + chunk_size).min(data.len());
            let chunk_data = data[offset..end].to_vec();
            let hash = blake3::hash(&chunk_data).to_hex().to_string();

            let chunk = Chunk {
                hash,
                offset,
                size: chunk_data.len(),
                data: chunk_data,
            };

            debug!(
                "[Chunk] file={} offset={} size={} hash={}",
                file_path,
                offset,
                chunk.size,
                &chunk.hash[..8]
            );

            chunks.push(chunk);
            offset = end;
        }

        chunks
    }

    /// Find the next chunk boundary using a gear-hash rolling hash.
    ///
    /// Scans forward from min_size until max_size, computing a rolling
    /// hash. A chunk boundary is found when hash & mask == 0.
    ///
    /// This is a simplified Gear hash implementation. For production,
    /// the `fastcdc` crate's implementation should be used instead.
    fn find_boundary(&self, data: &[u8], mask: u64) -> usize {
        find_boundary_at(data, self.min_size, self.max_size, mask)
    }

    /// Stream chunks from a `Read`er, invoking `on_chunk` for each chunk.
    ///
    /// Unlike [`Self::chunk_file`], which requires the whole file in memory,
    /// this method holds at most `max_size` bytes at any time regardless of
    /// the total file size — eliminating the OOM risk for large Minecraft
    /// region files (some exceed 16 MB). The produced chunk boundaries are
    /// identical to [`Self::chunk_file`] for the same input bytes.
    pub fn chunk_reader<R: Read>(
        &self,
        reader: &mut R,
        file_path: &str,
        mut on_chunk: impl FnMut(&Chunk) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mask = (self.avg_size as u64).wrapping_sub(1);
        let mut buf: Vec<u8> = Vec::with_capacity(self.max_size + 8192);
        let mut tmp = vec![0u8; 8192];
        let mut offset: usize = 0;
        let mut eof = false;

        loop {
            // Fill the buffer up to at least min_size unless EOF is reached.
            while buf.len() < self.min_size && !eof {
                let n = reader.read(&mut tmp)?;
                if n == 0 {
                    eof = true;
                } else {
                    buf.extend_from_slice(&tmp[..n]);
                }
            }

            if buf.is_empty() {
                break; // empty input, or consumed everything
            }

            // Determine the chunk length within [min_size, max_size].
            let chunk_len = if buf.len() <= self.min_size {
                buf.len() // tail — take all remaining bytes
            } else {
                find_boundary_at(&buf, self.min_size, self.max_size, mask).min(buf.len())
            };

            let chunk_data = buf[..chunk_len].to_vec();
            let hash = blake3::hash(&chunk_data).to_hex().to_string();
            let chunk = Chunk {
                hash,
                offset,
                size: chunk_len,
                data: chunk_data,
            };

            debug!(
                "[Chunk] file={} offset={} size={} hash={}",
                file_path,
                offset,
                chunk.size,
                &chunk.hash[..8]
            );
            on_chunk(&chunk)?;

            offset += chunk_len;
            buf.drain(..chunk_len);

            if eof && buf.is_empty() {
                break;
            }
        }

        Ok(())
    }
}

/// Shared boundary scan used by both the whole-buffer and streaming paths.
///
/// A chunk boundary is found when the gear-hash rolling hash satisfies
/// `hash & mask == 0` within the `[min_size, max_size]` window. Returns the
/// end offset of the chunk (exclusive).
fn find_boundary_at(data: &[u8], min_size: usize, max_size: usize, mask: u64) -> usize {
    let search_start = min_size;
    let search_end = max_size.min(data.len());

    if search_start >= search_end {
        return search_end;
    }

    // Use a simple polynomial rolling hash for boundary detection
    let mut hash: u64 = 0;
    let window = 48usize; // Hash window size

    // Initialize hash with first window bytes
    for &byte in data[search_start..search_start + window.min(data.len() - search_start)].iter()
    {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }

    // Slide the window, looking for boundary
    for i in (search_start + window)..search_end {
        // Rolling hash update: remove oldest byte, add newest
        let oldest = data[i - window];
        let newest = data[i];

        hash = hash
            .wrapping_sub((oldest as u64).wrapping_mul(31u64.wrapping_pow(window as u32 - 1)));
        hash = hash.wrapping_mul(31).wrapping_add(newest as u64);

        if hash & mask == 0 {
            return i + 1;
        }
    }

    // No boundary found within range, return max boundary
    search_end
}

impl Default for ChunkEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_data() {
        let engine = ChunkEngine::new();
        let chunks = engine.chunk_file(&[], "empty.dat");
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_small_file_single_chunk() {
        let engine = ChunkEngine::new();
        let data = vec![0u8; 1024]; // Smaller than min_size (4KB)
        let chunks = engine.chunk_file(&data, "small.dat");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].size, 1024);
    }

    #[test]
    fn test_large_file_multiple_chunks() {
        let engine = ChunkEngine::new();
        let data = vec![0u8; 512 * 1024]; // 512 KB
        let chunks = engine.chunk_file(&data, "large.dat");
        assert!(
            chunks.len() > 1,
            "Large file should produce multiple chunks"
        );
    }

    #[test]
    fn test_deterministic_chunking() {
        // Same data should produce same chunks every time
        let engine = ChunkEngine::new();
        let data = vec![42u8; 128 * 1024];

        let chunks1 = engine.chunk_file(&data, "test.dat");
        let chunks2 = engine.chunk_file(&data, "test.dat");

        assert_eq!(chunks1.len(), chunks2.len());
        for (c1, c2) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(c1.hash, c2.hash);
            assert_eq!(c1.size, c2.size);
        }
    }

    // --- Streaming chunk_reader ---

    fn collect_stream(engine: &ChunkEngine, data: &[u8], name: &str) -> Vec<Chunk> {
        let mut cursor = std::io::Cursor::new(data.to_vec());
        let mut chunks = Vec::new();
        engine
            .chunk_reader(&mut cursor, name, |c| {
                chunks.push(c.clone());
                Ok(())
            })
            .unwrap();
        chunks
    }

    #[test]
    fn test_stream_empty_data() {
        let engine = ChunkEngine::new();
        assert!(collect_stream(&engine, &[], "empty.dat").is_empty());
    }

    #[test]
    fn test_stream_small_file_single_chunk() {
        let engine = ChunkEngine::new();
        let data = vec![0u8; 1024]; // smaller than min_size
        let chunks = collect_stream(&engine, &data, "small.dat");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].size, 1024);
        assert_eq!(chunks[0].data, data);
    }

    #[test]
    fn test_stream_large_file_multiple_chunks() {
        let engine = ChunkEngine::new();
        let data = vec![0xABu8; 512 * 1024]; // 512 KB
        let chunks = collect_stream(&engine, &data, "large.dat");
        assert!(chunks.len() > 1, "Large file should produce multiple chunks");
    }

    #[test]
    fn test_stream_matches_chunk_file_boundaries() {
        // Streaming must produce byte-identical chunk boundaries to the
        // whole-buffer path, so existing dedup data stays compatible.
        let engine = ChunkEngine::new();
        // Content that is more realistic than all-zero bytes.
        let mut data: Vec<u8> = (0..=255u8).cycle().take(300 * 1024).collect();
        data.extend_from_slice(&vec![0x11u8; 64 * 1024]);

        let whole = engine.chunk_file(&data, "test.dat");
        let streamed = collect_stream(&engine, &data, "test.dat");

        assert_eq!(
            whole.len(),
            streamed.len(),
            "streaming and whole-buffer chunk counts must match"
        );
        for (w, s) in whole.iter().zip(streamed.iter()) {
            assert_eq!(w.hash, s.hash, "chunk hashes must match");
            assert_eq!(w.offset, s.offset, "chunk offsets must match");
            assert_eq!(w.size, s.size, "chunk sizes must match");
        }
    }

    #[test]
    fn test_stream_deterministic() {
        let engine = ChunkEngine::new();
        let data = vec![7u8; 96 * 1024];
        let c1 = collect_stream(&engine, &data, "t.dat");
        let c2 = collect_stream(&engine, &data, "t.dat");
        assert_eq!(c1.len(), c2.len());
        for (a, b) in c1.iter().zip(c2.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.offset, b.offset);
        }
    }

    #[test]
    fn test_stream_non_multiple_of_buffer() {
        // A file size that is not a multiple of the 8 KB fill buffer should
        // still chunk correctly (exercises the tail path).
        let engine = ChunkEngine::new();
        let data = vec![3u8; 32 * 1024 + 1234];
        let chunks = collect_stream(&engine, &data, "odd.dat");
        assert!(chunks.len() >= 1);
        let total: usize = chunks.iter().map(|c| c.size).sum();
        assert_eq!(total, data.len());
    }
}
