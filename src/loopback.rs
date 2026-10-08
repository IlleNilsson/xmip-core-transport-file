//! The file transport's loopback: both ends in one directory, one per
//! thread, sent into and read back from the same place.

use std::fs;
use std::path::PathBuf;

use context::property::{FILE_GROUP, FILE_MODE, FILE_OWNER};
use transport::ArrivalIdentity;
use transport::Transport;
use transport::arrived::next_arrival;
use transport::error::{Result, classify};
use transport::held::Held;
use transport::loopback::{FarEnd, Loopback};

use crate::FileTransport;

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

impl Loopback for FileTransport {
    /// A file names no sender. Its folder, in its origin, is the
    /// circumstance the gates infer an identity from (ADR-0019 clause 7);
    /// on Unix its owner, group and permissions travel beside it, and on
    /// Windows nothing safe reads them.
    fn arrival_identity(&self) -> ArrivalIdentity {
        if cfg!(unix) {
            ArrivalIdentity::Named(&[FILE_OWNER, FILE_GROUP, FILE_MODE])
        } else {
            ArrivalIdentity::Unnamed(
                "a dropped file names no sender, and Windows says its owner only unsafely",
            )
        }
    }

    /// The directory a round drops into. Nothing waits: the round is in
    /// order, so the file is whole when the far end lists it, and its
    /// second listing takes it.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let directory = self.thread_directory();
        fs::create_dir_all(&directory)
            .map_err(|e| classify("creating the exchange directory", &e))?;
        Ok(Box::new(Held::new("round-trip", move || {
            let far = FileTransport::new(directory);
            // The first listing finds the file; the stability check takes
            // it at the second.
            far.receive()?;
            next_arrival(far.receive()?, "sent, but it did not come back")?.taken()
        })))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        FileTransport::new(self.thread_directory()).send(address, payload)
    }

    /// In order on one thread: a directory does not listen, so the send goes
    /// first and the read-back finds it.
    fn exchanges_in_order(&self) -> bool {
        true
    }
}
