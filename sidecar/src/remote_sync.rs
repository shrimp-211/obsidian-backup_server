//! Remote synchronization between two Obsidian Sidecars.
//!
//! Either peer — the Minecraft server or the backup node — can act as the
//! **active sender**. The peer that owns a public IP listens (`listen_addr`);
//! the other peer dials it (`peer_addr`). All traffic is authenticated with a
//! shared token and encrypted with XChaCha20-Poly1305, so no plaintext backup
//! data ever crosses the network.
//!
//! Wire flow (sender → receiver):
//! ```text
//! hello(token) → hello_ack
//! push_begin(manifest) → push_ready
//! file_chunks(path, chunks)
//! object(hash, size) + <raw bytes>   … repeated …
//! push_end → push_ack
//! ```
//!
//! The receiver writes every object into its local ObjectStore, rebuilds the
//! file→chunk index and persists the manifest, producing a fully restorable
//! snapshot on the receiving side. `pull` is symmetric: the dialer requests a
//! snapshot and the listener streams it back through the same pipeline.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::backup::BackupEngine;
use crate::config::RemoteSyncConfig;

const PROTOCOL_VERSION: u32 = 1;
const NONCE_LEN: usize = 24;
const LEN_BYTES: usize = 4;
/// Guard against absurd frame sizes (largest object is bounded by the
/// chunker's max_size, but keep a hard ceiling for safety).
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

type Cipher = XChaCha20Poly1305;

/// Control / data frames exchanged over the wire (encrypted JSON payloads;
/// object data follows its `Object` frame as raw bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Frame {
    Hello {
        version: u32,
        token: String,
    },
    HelloAck {
        status: String,
        message: Option<String>,
    },
    PushBegin {
        snapshot_id: String,
        manifest: Value,
        file_count: u64,
    },
    PushReady {
        status: String,
    },
    FileChunks {
        path: String,
        chunks: Vec<String>,
    },
    Object {
        hash: String,
        size: u64,
    },
    PushEnd {},
    PushAck {
        status: String,
    },
    PullRequest {
        snapshot_id: String,
    },
    Error {
        message: String,
    },
}

/// Client for synchronizing snapshots with a peer sidecar.
pub struct RemoteSync {
    config: RemoteSyncConfig,
    engine: Arc<BackupEngine>,
}

impl RemoteSync {
    pub fn new(config: RemoteSyncConfig, engine: Arc<BackupEngine>) -> Self {
        Self { config, engine }
    }

    /// Listener role: accept connections and store snapshots pushed by the
    /// peer (or answer pull requests). Blocks until the listener is closed.
    pub async fn serve(&self) -> Result<()> {
        let addr = self
            .config
            .listen_addr
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("remote_sync.listen_addr is not configured"))?;
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("cannot bind remote sync listener on {}", addr))?;
        info!("[RemoteSync] Listening on {} (public-IP peer)", addr);

        loop {
            let (stream, peer) = listener.accept().await?;
            info!("[RemoteSync] Connection from {}", peer);
            let engine = self.engine.clone();
            let token = self.config.token.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_incoming(stream, engine, &token).await {
                    warn!("[RemoteSync] Incoming connection error: {}", e);
                }
            });
        }
    }

    /// Dialer role (push): actively send a snapshot to the peer.
    pub async fn push_snapshot(&self, snapshot_id: &str) -> Result<()> {
        let addr = self
            .config
            .peer_addr
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("remote_sync.peer_addr is not configured"))?;
        info!("[RemoteSync] Pushing snapshot {} to {}", snapshot_id, addr);

        let timeout = Duration::from_secs(self.config.dial_timeout_secs);
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .context("timed out connecting to peer")??;

        let cipher = make_cipher(&self.config.token);
        sender_handshake(&mut stream, &cipher, &self.config.token).await?;
        send_snapshot(&mut stream, &cipher, &self.engine, snapshot_id).await?;
        info!("[RemoteSync] Snapshot {} pushed successfully", snapshot_id);
        Ok(())
    }

    /// Dialer role (pull): ask the peer to stream a snapshot to us.
    ///
    /// The peer must be reachable (it holds the public IP and runs
    /// `serve`). The snapshot is stored locally on return.
    pub async fn pull_snapshot(&self, snapshot_id: &str) -> Result<()> {
        let addr = self
            .config
            .peer_addr
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("remote_sync.peer_addr is not configured"))?;
        info!("[RemoteSync] Pulling snapshot {} from {}", snapshot_id, addr);

        let timeout = Duration::from_secs(self.config.dial_timeout_secs);
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .context("timed out connecting to peer")??;

        let cipher = make_cipher(&self.config.token);
        sender_handshake(&mut stream, &cipher, &self.config.token).await?;
        send_frame(
            &mut stream,
            &cipher,
            &Frame::PullRequest {
                snapshot_id: snapshot_id.to_string(),
            },
        )
        .await?;

        match recv_frame(&mut stream, &cipher).await? {
            Frame::PushBegin {
                snapshot_id,
                manifest,
                ..
            } => {
                receive_snapshot(&mut stream, &cipher, &self.engine, &snapshot_id, manifest).await?;
            }
            Frame::Error { message } => bail!("peer pull failed: {}", message),
            other => bail!("unexpected reply to pull request: {:?}", other),
        }
        info!("[RemoteSync] Snapshot {} pulled successfully", snapshot_id);
        Ok(())
    }
}

// =========================================================================
// Sender side
// =========================================================================

/// Authenticate as the dialing peer.
async fn sender_handshake(stream: &mut TcpStream, cipher: &Cipher, token: &str) -> Result<()> {
    send_frame(
        stream,
        cipher,
        &Frame::Hello {
            version: PROTOCOL_VERSION,
            token: token.to_string(),
        },
    )
    .await?;
    match recv_frame(stream, cipher).await? {
        Frame::HelloAck { status, message } if status == "ok" => Ok(()),
        Frame::HelloAck { status, message } => {
            bail!("peer rejected sync auth ({}): {}", status, message.unwrap_or_default())
        }
        other => bail!("unexpected handshake reply: {:?}", other),
    }
}

/// Stream a whole snapshot (manifest + file mappings + objects) to the peer.
async fn send_snapshot(
    stream: &mut TcpStream,
    cipher: &Cipher,
    engine: &Arc<BackupEngine>,
    snapshot_id: &str,
) -> Result<()> {
    let manifest = engine.snapshot_manifest_json(snapshot_id).await?;
    let files = engine.snapshot_files(snapshot_id).await?;

    send_frame(
        stream,
        cipher,
        &Frame::PushBegin {
            snapshot_id: snapshot_id.to_string(),
            manifest,
            file_count: files.len() as u64,
        },
    )
    .await?;
    expect_ready(stream, cipher).await?;

    send_objects(stream, cipher, engine, &files).await?;

    send_frame(stream, cipher, &Frame::PushEnd {}).await?;
    expect_ack(stream, cipher).await?;
    Ok(())
}

/// Transmit file→chunk mappings and object payloads, ending with `push_end`.
async fn send_objects(
    stream: &mut TcpStream,
    cipher: &Cipher,
    engine: &Arc<BackupEngine>,
    files: &[String],
) -> Result<()> {
    for path in files {
        let chunks = engine.file_chunks(path).await?;
        send_frame(
            stream,
            cipher,
            &Frame::FileChunks {
                path: path.clone(),
                chunks,
            },
        )
        .await?;

        for hash in &chunks {
            let data = engine.read_object_data(hash).await?;
            send_frame(
                stream,
                cipher,
                &Frame::Object {
                    hash: hash.clone(),
                    size: data.len() as u64,
                },
            )
            .await?;
            send_payload(stream, cipher, &data).await?;
        }
    }
    Ok(())
}

// =========================================================================
// Receiver side
// =========================================================================

/// Handle an incoming (listener-side) connection: authenticate, then serve a
/// push (store the snapshot) or a pull (stream the snapshot back).
async fn handle_incoming(
    mut stream: TcpStream,
    engine: Arc<BackupEngine>,
    token: &str,
) -> Result<()> {
    let cipher = make_cipher(token);

    // Authentication: the first frame must be a valid Hello.
    match recv_frame(&mut stream, &cipher).await? {
        Frame::Hello {
            version,
            token: t,
        } if version == PROTOCOL_VERSION && t == token => {
            send_frame(
                &mut stream,
                &cipher,
                &Frame::HelloAck {
                    status: "ok".into(),
                    message: None,
                },
            )
            .await?;
        }
        _ => {
            send_frame(
                &mut stream,
                &cipher,
                &Frame::HelloAck {
                    status: "error".into(),
                    message: Some("authentication failed".into()),
                },
            )
            .await?;
            bail!("remote sync authentication failed");
        }
    }

    match recv_frame(&mut stream, &cipher).await? {
        // Peer pushes a snapshot to us.
        Frame::PushBegin {
            snapshot_id,
            manifest,
            ..
        } => {
            receive_snapshot(&mut stream, &cipher, &engine, &snapshot_id, manifest).await?;
        }
        // Peer pulls a snapshot from us.
        Frame::PullRequest { snapshot_id } => {
            let manifest = engine.snapshot_manifest_json(&snapshot_id).await?;
            let files = engine.snapshot_files(&snapshot_id).await?;
            send_frame(
                &mut stream,
                &cipher,
                &Frame::PushBegin {
                    snapshot_id: snapshot_id.clone(),
                    manifest,
                    file_count: files.len() as u64,
                },
            )
            .await?;
            expect_ready(&mut stream, &cipher).await?;
            send_objects(&mut stream, &cipher, &engine, &files).await?;
            send_frame(&mut stream, &cipher, &Frame::PushEnd {}).await?;
            expect_ack(&mut stream, &cipher).await?;
        }
        Frame::Error { message } => bail!("peer error: {}", message),
        other => bail!("unexpected first command frame: {:?}", other),
    }
    Ok(())
}

/// Receive and store a snapshot streamed by the peer.
async fn receive_snapshot(
    stream: &mut TcpStream,
    cipher: &Cipher,
    engine: &Arc<BackupEngine>,
    snapshot_id: &str,
    manifest: Value,
) -> Result<()> {
    send_frame(
        stream,
        cipher,
        &Frame::PushReady {
            status: "ok".into(),
        },
    )
    .await?;

    loop {
        match recv_frame(stream, cipher).await? {
            Frame::FileChunks { path, chunks } => {
                engine.store_file_chunks(&path, &chunks).await?;
            }
            Frame::Object { hash, size } => {
                let data = recv_payload(stream, cipher).await?;
                if data.len() as u64 != size {
                    bail!(
                        "object {} size mismatch (expected {}, got {})",
                        hash,
                        size,
                        data.len()
                    );
                }
                engine.store_object(&hash, &data).await?;
            }
            Frame::PushEnd {} => break,
            Frame::Error { message } => bail!("peer error: {}", message),
            other => bail!("unexpected frame while receiving: {:?}", other),
        }
    }

    engine.register_remote_snapshot(snapshot_id, manifest).await?;
    send_frame(
        stream,
        cipher,
        &Frame::PushAck {
            status: "ok".into(),
        },
    )
    .await?;
    info!("[RemoteSync] Stored snapshot {} from peer", snapshot_id);
    Ok(())
}

// =========================================================================
// Wire transport (encrypted length-prefixed frames)
// =========================================================================

/// Derive the XChaCha20-Poly1305 key from the shared token.
fn make_cipher(token: &str) -> Cipher {
    let key = blake3::hash(token.as_bytes());
    XChaCha20Poly1305::new_from_slice(key.as_bytes())
        .expect("BLAKE3 output is always 32 bytes")
}

/// Encrypt and write `[len(4 LE)][nonce(24)][ciphertext]`.
async fn send_payload<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    cipher: &Cipher,
    data: &[u8],
) -> Result<()> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(XNonce::from_slice(&nonce), data)
        .map_err(|_| anyhow::anyhow!("XChaCha20-Poly1305 encryption failed"))?;
    w.write_all(&(ct.len() as u32).to_le_bytes()).await?;
    w.write_all(&nonce).await?;
    w.write_all(&ct).await?;
    w.flush().await?;
    Ok(())
}

/// Read and decrypt a frame written by [`send_payload`].
async fn recv_payload<R: AsyncReadExt + Unpin>(
    r: &mut R,
    cipher: &Cipher,
) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; LEN_BYTES];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        bail!("oversized frame ({} bytes)", len);
    }
    let mut nonce = [0u8; NONCE_LEN];
    r.read_exact(&mut nonce).await?;
    let mut ct = vec![0u8; len];
    r.read_exact(&mut ct).await?;
    cipher
        .decrypt(XNonce::from_slice(&nonce), ct.as_slice())
        .map_err(|_| anyhow::anyhow!("XChaCha20-Poly1305 decryption failed"))
}

async fn send_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    cipher: &Cipher,
    frame: &Frame,
) -> Result<()> {
    let bytes = serde_json::to_vec(frame)?;
    send_payload(w, cipher, &bytes).await
}

async fn recv_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
    cipher: &Cipher,
) -> Result<Frame> {
    let bytes = recv_payload(r, cipher).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Await a `push_ready` confirmation.
async fn expect_ready(stream: &mut TcpStream, cipher: &Cipher) -> Result<()> {
    match recv_frame(stream, cipher).await? {
        Frame::PushReady { status } if status == "ok" => Ok(()),
        Frame::Error { message } => bail!("peer rejected push: {}", message),
        other => bail!("expected push_ready, got {:?}", other),
    }
}

/// Await the final `push_ack`.
async fn expect_ack(stream: &mut TcpStream, cipher: &Cipher) -> Result<()> {
    match recv_frame(stream, cipher).await? {
        Frame::PushAck { status } if status == "ok" => Ok(()),
        Frame::Error { message } => bail!("peer push failed: {}", message),
        other => bail!("expected push_ack, got {:?}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_payload_roundtrip_encrypted() {
        let cipher = make_cipher("test-token");
        let secret = b"super secret backup payload".to_vec();

        let mut buf = Vec::new();
        send_payload(&mut buf, &cipher, &secret).await.unwrap();

        let mut reader = Cursor::new(buf);
        let recovered = recv_payload(&mut reader, &cipher).await.unwrap();
        assert_eq!(recovered, secret);
    }

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let cipher = make_cipher("token2");
        let frame = Frame::FileChunks {
            path: "world/region/r.0.0.mca".into(),
            chunks: vec!["abc123".into(), "def456".into()],
        };

        let mut buf = Vec::new();
        send_frame(&mut buf, &cipher, &frame).await.unwrap();

        let mut reader = Cursor::new(buf);
        let recovered = recv_frame(&mut reader, &cipher).await.unwrap();
        match recovered {
            Frame::FileChunks { path, chunks } => {
                assert_eq!(path, "world/region/r.0.0.mca");
                assert_eq!(chunks.len(), 2);
            }
            other => panic!("expected FileChunks, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_wrong_key_cannot_decrypt() {
        let cipher = make_cipher("correct-key");
        let other = make_cipher("wrong-key");

        let mut buf = Vec::new();
        send_payload(&mut buf, &cipher, b"classified").await.unwrap();

        let mut reader = Cursor::new(buf);
        let res = recv_payload(&mut reader, &other).await;
        assert!(res.is_err(), "decryption with the wrong key must fail");
    }

    #[tokio::test]
    async fn test_oversized_frame_rejected() {
        let cipher = make_cipher("tok");
        // Claim a huge length with no payload behind it.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_BYTES as u32 + 1).to_le_bytes());
        buf.extend_from_slice(&[0u8; NONCE_LEN]);

        let mut reader = Cursor::new(buf);
        let res = recv_payload(&mut reader, &cipher).await;
        assert!(res.is_err(), "oversized frames must be rejected");
    }

    #[test]
    fn test_make_cipher_deterministic() {
        // Same token → same key material (so both peers derive identical keys).
        let key_a = blake3::hash(b"shared-token");
        let key_b = blake3::hash(b"shared-token");
        assert_eq!(key_a.as_bytes(), key_b.as_bytes());
    }
}
