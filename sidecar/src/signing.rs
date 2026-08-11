//! Ed25519 snapshot manifest signing.
//!
//! Every committed snapshot writes a `manifest.json`. When signing is
//! enabled the sidecar derives an Ed25519 signature over the exact manifest
//! bytes and stores it next to the manifest as `{snapshot_id}.sig`. Any
//! restore / verify step re-checks the signature first, so an offline
//! tamper with the manifest (or the packfiles it references) is detected
//! and aborts before data is touched.
//!
//! Key management: the signing key pair is persisted under
//! `private_key_secure_path` (or `.obsidian/keys/sign.key`). The key is
//! generated on first run and reused afterwards — rotating it invalidates
//! signatures of all historical snapshots.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

pub const KEY_SIZE: usize = 32;
pub const SIGNATURE_SIZE: usize = 64;

/// Signs snapshot manifests with a persisted Ed25519 key pair.
pub struct SnapshotSigner {
    signing_key: SigningKey,
    key_path: PathBuf,
}

impl SnapshotSigner {
    /// Load the key pair from `key_path`; generate and persist a fresh pair
    /// if the file does not exist yet.
    pub fn load_or_create(key_path: &Path) -> Result<Self> {
        if key_path.exists() {
            let bytes = std::fs::read(key_path)
                .with_context(|| format!("cannot read signing key at {:?}", key_path))?;
            let arr: [u8; KEY_SIZE] = bytes
                .try_into()
                .map_err(|_| anyhow!("invalid Ed25519 signing key size at {:?}", key_path))?;
            Ok(Self {
                signing_key: SigningKey::from_bytes(&arr),
                key_path: key_path.to_path_buf(),
            })
        } else {
            let signing_key = SigningKey::generate(&mut OsRng);
            if let Some(parent) = key_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(key_path, signing_key.to_bytes())?;
            std::fs::write(
                public_key_path(key_path),
                signing_key.verifying_key().to_bytes(),
            )?;
            tracing::info!("[Signer] Generated new Ed25519 key pair at {:?}", key_path);
            Ok(Self {
                signing_key,
                key_path: key_path.to_path_buf(),
            })
        }
    }

    /// Produce a base64-encoded Ed25519 signature over `data`.
    pub fn sign(&self, data: &[u8]) -> String {
        let sig = self.signing_key.sign(data);
        B64.encode(sig.to_bytes())
    }

    /// The 32-byte verifying (public) key.
    pub fn verifying_key_bytes(&self) -> [u8; KEY_SIZE] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Path of the signing key file.
    pub fn key_path(&self) -> &Path {
        &self.key_path
    }
}

/// Path of the public key file corresponding to `key_path` (appends `.pub`).
pub fn public_key_path(key_path: &Path) -> PathBuf {
    let mut os = key_path.as_os_str().to_os_string();
    os.push(".pub");
    PathBuf::from(os)
}

/// Verify a base64-encoded Ed25519 signature over `data`.
///
/// Returns `false` on any malformed input or on a signature mismatch —
/// never panics.
pub fn verify_signature(data: &[u8], signature_b64: &str, public_key: &[u8]) -> bool {
    let Ok(sig_bytes) = B64.decode(signature_b64.trim()) else {
        return false;
    };
    let Ok(sig_arr): Result<[u8; SIGNATURE_SIZE], _> = sig_bytes.as_slice().try_into() else {
        return false;
    };
    let Ok(sig) = Signature::from_bytes(&sig_arr) else {
        return false;
    };
    let Ok(vk_arr): Result<[u8; KEY_SIZE], _> = public_key.try_into() else {
        return false;
    };
    let Ok(vk) = VerifyingKey::from_bytes(&vk_arr) else {
        return false;
    };
    vk.verify(data, &sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sign_and_verify_roundtrip() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("sign.key");
        let signer = SnapshotSigner::load_or_create(&key_path).unwrap();
        assert!(key_path.exists());
        assert!(public_key_path(&key_path).exists());

        let manifest = br#"{"snapshot_id":"snap_abc","timestamp":"2026-08-12T00:00:00Z"}"#;
        let sig = signer.sign(manifest);

        let pub_key = signer.verifying_key_bytes();
        assert!(verify_signature(manifest, &sig, &pub_key));

        // Tampered manifest must fail verification.
        let tampered = br#"{"snapshot_id":"snap_abc","timestamp":"2026-08-12T00:00:00Z","x":1}"#;
        assert!(!verify_signature(tampered, &sig, &pub_key));
    }

    #[test]
    fn test_load_existing_key_is_stable() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("sign.key");
        let a = SnapshotSigner::load_or_create(&key_path).unwrap();
        let pub_a = a.verifying_key_bytes();

        // Loading the persisted key again yields the same verifying key.
        let b = SnapshotSigner::load_or_create(&key_path).unwrap();
        assert_eq!(pub_a, b.verifying_key_bytes());
    }

    #[test]
    fn test_verify_rejects_garbage() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("sign.key");
        let signer = SnapshotSigner::load_or_create(&key_path).unwrap();
        let manifest = b"hello";
        let pub_key = signer.verifying_key_bytes();

        assert!(!verify_signature(manifest, "not-base64!!!", &pub_key));
        assert!(!verify_signature(manifest, &signer.sign(manifest), b"short"));
    }
}
