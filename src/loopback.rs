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
        self.directory
            .root
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
    /// order, so the file is whole when the far end lists it, and the
    /// listing that takes it is the round's.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let directory = self.thread_directory();
        fs::create_dir_all(&directory)
            .map_err(|e| classify("creating the exchange directory", &e))?;
        Ok(Box::new(Held::new("round-trip", move || {
            let far = FileTransport::new(directory);
            // The first listing takes the file where the system says no
            // writer holds it (Windows); elsewhere it only finds it, and
            // the stability check takes it at the second. What the first
            // took is the round's: let go of, it went back to the
            // directory to be claimed again, two renames more a round
            // (2026-11-09).
            let first = far.receive()?;
            let arrivals = if first.is_empty() {
                far.receive()?
            } else {
                first
            };
            next_arrival(arrivals, "sent, but it did not come back")?.taken()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_round_takes_back_what_it_sent_and_leaves_its_directory_empty() {
        let root = std::env::temp_dir().join(format!("xmip-file-loopback-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let looped = FileTransport::loopback(&root);
        for payload in [&b"ISA*00*one*"[..], b"ISA*00*two*"] {
            let taken = looped.round(payload).expect("a round");
            assert_eq!(taken.bytes, payload);
        }
        let left = fs::read_dir(looped.thread_directory())
            .expect("its directory")
            .count();
        assert_eq!(left, 0, "nothing claimed, returned or left beside it");
        let _ = fs::remove_dir_all(&root);
    }
}
