//! Cross-platform IPC transport.
//!
//! On Unix the Sidecar talks over a Unix Domain Socket; on Windows it uses a
//! Named Pipe. This module abstracts the two behind a single [`IpcStream`]
//! type (implementing `AsyncRead + AsyncWrite`) and an [`IpcListener`] so the
//! request-dispatch code in `server.rs` stays platform-agnostic.
//!
//! The socket/pipe "address" is a plain string:
//!   - Unix: a filesystem path (e.g. `.obsidian/ipc/obsidian.sock`)
//!   - Windows: a named-pipe name (e.g. `obsidian-backup-ipc`)

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A bidirectional IPC stream (server-side accepted or client-side connected).
#[derive(Debug)]
pub enum IpcStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    ServerPipe(tokio::net::windows::named_pipe::NamedPipeServer),
    #[cfg(windows)]
    ClientPipe(tokio::net::windows::named_pipe::NamedPipeClient),
}

impl IpcStream {
    /// Connect to the IPC endpoint as a client.
    #[cfg(unix)]
    pub async fn connect(addr: &str) -> io::Result<Self> {
        Ok(Self::Unix(tokio::net::UnixStream::connect(addr).await?))
    }

    /// Connect to the IPC endpoint as a client.
    #[cfg(windows)]
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let pipe_name = pipe_name(addr);
        let client = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&pipe_name)?;
        Ok(Self::ClientPipe(client))
    }
}

/// An IPC listener that yields one [`IpcStream`] per accepted connection.
pub struct IpcListener {
    #[cfg(unix)]
    unix: tokio::net::UnixListener,
    #[cfg(windows)]
    addr: String,
}

impl IpcListener {
    /// Bind (or prepare) the listener for the given address.
    #[cfg(unix)]
    pub async fn bind(addr: &str) -> io::Result<Self> {
        // Remove a stale socket file left by a previous run, then bind once.
        let _ = std::fs::remove_file(addr);
        let listener = tokio::net::UnixListener::bind(addr)?;
        Ok(Self { unix: listener })
    }

    /// Prepare the listener (Windows named pipes have no persistent bind).
    #[cfg(windows)]
    pub async fn bind(addr: &str) -> io::Result<Self> {
        Ok(Self { addr: addr.to_string() })
    }

    /// Accept the next connection.
    #[cfg(unix)]
    pub async fn accept(&self) -> io::Result<IpcStream> {
        let (stream, _peer) = self.unix.accept().await?;
        Ok(IpcStream::Unix(stream))
    }

    /// Accept the next connection (one pipe instance per client).
    #[cfg(windows)]
    pub async fn accept(&self) -> io::Result<IpcStream> {
        use tokio::net::windows::named_pipe::ServerOptions;
        let pipe_name = pipe_name(&self.addr);
        let server = ServerOptions::new().create(&pipe_name)?;
        server.connect().await?;
        Ok(IpcStream::ServerPipe(server))
    }
}

impl AsyncRead for IpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            Self::ServerPipe(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            Self::ClientPipe(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Self::ServerPipe(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Self::ClientPipe(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Self::ServerPipe(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Self::ClientPipe(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Self::ServerPipe(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Self::ClientPipe(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Translate a logical IPC address into a Windows named-pipe path.
#[cfg(windows)]
fn pipe_name(addr: &str) -> String {
    let sanitized: String = addr
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    format!(r"\\.\pipe\{}", sanitized)
}
