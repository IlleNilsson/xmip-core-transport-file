#![forbid(unsafe_code)]

//! Streams that arrive as files in a directory.
//!
//! The polled case: nothing is pushed to Xmip, Xmip goes and looks. Identity is
//! therefore *inferred* from the Receive Location rather than passed by a caller
//! — ADR-0019 clause 8.

use std::fs;
use std::path::{Path, PathBuf};

use transport::Arrived;
use transport::Directions;
use transport::Transport;
use transport::error::{Result, classify, protocol_error};
use transport::loopback::{FarEnd, Loopback};

pub struct FileTransport {
    root: PathBuf,
}

impl FileTransport {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Read one dropped file.
    fn read_one(path: &Path) -> Result<Arrived> {
        let bytes = fs::read(path).map_err(|e| classify("reading a dropped file", &e))?;

        Ok(Arrived::new(file_uri(path), bytes))
    }
}

impl Transport for FileTransport {
    fn name(&self) -> &'static str {
        "file"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn receive(&self) -> Result<Vec<Arrived>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            // A drop directory that does not exist yet is not a failure. It is
            // a Receive Location nobody has sent to.
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(classify("reading the drop directory", &e)),
        };

        let mut arrived = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| classify("listing the drop directory", &e))?;
            let path = entry.path();

            if path.is_file() {
                arrived.push(Self::read_one(&path)?);
            }
        }

        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let path = self.root.join(target);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| classify("creating the send directory", &e))?;
        }

        fs::write(&path, bytes).map_err(|e| classify("writing the sent file", &e))
    }
}

/// Render a path as a file URI.
///
/// A URI uses forward slashes on every platform, so a Windows path has to be
/// converted rather than printed. `char::from(92)` is a backslash, written this
/// way to keep the escape out of the literal.
#[must_use]
fn file_uri(path: &Path) -> String {
    format!(
        "file:///{}",
        path.display().to_string().replace(char::from(92), "/")
    )
}

impl FileTransport {
    /// Both ends in one directory: send into it, read it back from the same
    /// place. The self-contained case, and the reason file was first.
    #[must_use]
    pub fn loopback(root: impl Into<PathBuf>) -> Self {
        Self::new(root)
    }

    /// One directory per thread: pairs driven at once from several threads
    /// would otherwise pick up each other's file and report it as sent but
    /// not returned (found at Harsh, 2026-09-10). Per thread rather than per
    /// exchange so the directory is made once and the round stays as fast as
    /// the file transport is.
    fn thread_directory(&self) -> PathBuf {
        self.root
            .join(format!("t{:?}", std::thread::current().id()))
    }
}

/// The directory a round drops into. Nothing waits: the round is in order.
struct Directory(PathBuf);

impl FarEnd for Directory {
    fn address(&self) -> &'static str {
        "pingpong"
    }

    fn take_one(self: Box<Self>) -> Result<Arrived> {
        FileTransport::new(self.0)
            .receive()?
            .into_iter()
            .next()
            .ok_or_else(|| protocol_error("sent, but it did not come back"))
    }
}

impl Loopback for FileTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let directory = self.thread_directory();
        fs::create_dir_all(&directory)
            .map_err(|e| classify("creating the exchange directory", &e))?;
        Ok(Box::new(Directory(directory)))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        FileTransport::new(self.thread_directory()).send(address, payload)
    }

    fn unblock(&self, _address: &str) {}

    /// In order on one thread: a directory does not listen, so the send goes
    /// first and the read-back finds it.
    fn round(&self, payload: &[u8]) -> Result<Arrived> {
        let far = self.far_end()?;
        self.send_to(far.address(), payload)?;
        let arrived = far.take_one()?;
        if arrived.bytes != payload {
            return Err(protocol_error("sent, but what came back differs"));
        }
        Ok(arrived)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scratch(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();

        let dir = std::env::temp_dir().join(format!("xmip-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("creating the scratch directory");

        dir
    }

    #[test]
    fn file_transport_declares_both_directions() {
        let transport = FileTransport::new(std::env::temp_dir());

        assert!(transport.directions().receives());
        assert!(transport.directions().sends());
        assert_eq!(transport.name(), "file");
    }

    #[test]
    fn file_receive_is_empty_when_the_directory_is_absent() {
        let transport = FileTransport::new(std::env::temp_dir().join("xmip-definitely-not-here"));

        assert!(
            transport
                .receive()
                .expect("absent directory is not a failure")
                .is_empty()
        );
    }

    #[test]
    fn file_round_trip_carries_bytes_and_origin() {
        let dir = scratch("file-round-trip");
        let transport = FileTransport::new(&dir);

        transport
            .send("order-1001.edi", b"ISA*00*")
            .expect("sending");
        let arrived = transport.receive().expect("receiving");

        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, b"ISA*00*");
        assert!(arrived[0].origin_uri.starts_with("file:///"));
        assert!(arrived[0].origin_uri.contains("order-1001.edi"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_file_uri_uses_forward_slashes_everywhere() {
        let dir = scratch("file-uri");
        let transport = FileTransport::new(&dir);

        transport.send("order.edi", b"x").expect("sending");
        let arrived = transport.receive().expect("receiving");

        assert!(!arrived[0].origin_uri.contains(char::from(92)));

        fs::remove_dir_all(&dir).ok();
    }
}
