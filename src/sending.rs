//! A sent file written whole before it is there.
//!
//! A receiver lists a drop directory whenever it likes, so a file written in
//! place can be found half written. A send therefore writes a file of its
//! own beside the target — same directory, so same file system — named so
//! no listing takes it, and named for the node that writes it
//! (`transport::claim::Claimant::sending`; ADR-0024, amendment
//! 2026-10-19): the target's name, the mark `.xmip-sending.`, the node's
//! location percent-encoded, a count of the process's sends and the time
//! in nanoseconds since the Unix epoch:
//!
//! ```text
//! order.edi.xmip-sending.xmip%3A%2F%2F%2F<cluster>%2Fnode%2F<node>.7.1790000000123456789
//! ```
//!
//! The body is written as it is read, a piece at a time from the Ledger
//! (`transport::Outgoing::write_to`), so a file of any size costs the send
//! a piece of memory, never the file. Written, it is synced to disk and
//! renamed over the target in one step:
//! a receiver finds the target as it was before or whole, never in between.
//! A rename replaces what it lands on (`std::fs::rename`: `MoveFileExW`
//! with `MOVEFILE_REPLACE_EXISTING` on Windows, `rename(2)` on Unix), so a
//! send to a name already there replaces that file, as writing it did.
//! Where anything fails, the written file is removed and the target is left
//! as it was. A process that dies between the write and the rename leaves
//! the file; its node removes it as it starts again
//! (`FileTransport::on_node`), and no other node touches it.

use std::fs::{self, File};
use std::io;
use std::path::Path;

use transport::Outgoing;
use transport::claim::Claimant;
use transport::error::{Result, classify};

/// `outgoing`'s body at `path`, whole or not at all, written beside it
/// under a name `claimant` gives.
///
/// # Errors
/// As the body could not be read, or the file system refused the write,
/// the sync or the rename; the target is left as it was.
pub(crate) fn send(path: &Path, claimant: &Claimant, outgoing: &Outgoing<'_>) -> Result<()> {
    send_by(path, claimant, |file| outgoing.write_to(file).map(drop))
}

/// [`send`], writing through `write`: a test hands in a write that fails
/// half way.
fn send_by(
    path: &Path,
    claimant: &Claimant,
    write: impl FnOnce(&mut File) -> Result<()>,
) -> Result<()> {
    let refused = |e: &io::Error| classify("writing the sent file", e);
    let target = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            classify(
                "naming the sent file",
                &io::Error::from(io::ErrorKind::InvalidInput),
            )
        })?;
    // In flight from before the file is created until it is renamed or
    // removed: a node starting meanwhile passes over it.
    let unfinished = claimant.sending(target);
    let sending = path.with_file_name(unfinished.name());
    let written = File::create_new(&sending)
        .map_err(|e| refused(&e))
        .and_then(|mut file| {
            write(&mut file)?;
            file.sync_all().map_err(|e| refused(&e))
        })
        .and_then(|()| fs::rename(&sending, path).map_err(|e| refused(&e)));
    if written.is_err() {
        let _ = fs::remove_file(&sending);
    }
    drop(unfinished);
    written
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileTransport;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use transport::{Configured, NodeLocation};
    use xcore::settings::Applies;

    /// The node these sends are made on.
    fn node() -> NodeLocation {
        NodeLocation::new(format!("process-{}/place-0", std::process::id()))
    }

    fn me() -> Claimant {
        Claimant::of(&node())
    }

    /// A fresh scratch directory.
    fn scratch(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("creating the scratch directory");
        dir
    }

    /// The names in `dir`.
    fn names(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .expect("listing")
            .map(|entry| {
                entry
                    .expect("an entry")
                    .file_name()
                    .into_string()
                    .expect("UTF-8")
            })
            .collect()
    }

    #[test]
    fn a_sent_file_leaves_nothing_beside_it() {
        let dir = scratch("file-send-whole");
        let target = dir.join("order.edi");
        send(&target, &me(), &Outgoing::held(b"old")).expect("sending");
        send(&target, &me(), &Outgoing::held(b"new")).expect("sending again");

        assert_eq!(fs::read(&target).expect("sent"), b"new", "replaced");
        assert_eq!(names(&dir), ["order.edi"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_send_leaves_the_target_as_it_was() {
        let dir = scratch("file-send-failed");
        let target = dir.join("order.edi");
        fs::write(&target, b"before").expect("a file already there");

        let failed = send_by(&target, &me(), |file| {
            file.write_all(b"ISA")
                .map_err(|e| classify("writing", &e))?;
            Err(classify("writing", &io::Error::other("the disk is full")))
        });

        assert!(failed.is_err());
        assert_eq!(fs::read(&target).expect("still there"), b"before");
        assert_eq!(names(&dir), ["order.edi"], "the written file removed");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A body of `length` bytes made as it is read — byte `n` is `n % 251`
    /// — that breaks after `breaks` bytes, if it does: no more of it is
    /// ever in memory than the piece the reader asks for.
    struct Made {
        at: u64,
        length: u64,
        breaks: Option<u64>,
        largest: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl Read for Made {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.breaks.is_some_and(|at| self.at >= at) {
                return Err(io::Error::other("no Storage node answered"));
            }
            let left = usize::try_from(self.length - self.at).unwrap_or(usize::MAX);
            let taken = out.len().min(left);
            self.largest.set(self.largest.get().max(taken));
            for byte in &mut out[..taken] {
                *byte = u8::try_from(self.at % 251).unwrap_or_default();
                self.at += 1;
            }
            Ok(taken)
        }
    }

    #[test]
    fn a_body_is_written_a_piece_at_a_time_and_one_that_breaks_leaves_the_target() {
        const LENGTH: u64 = 4 * 1024 * 1024;
        let dir = scratch("file-send-streamed");
        let target = dir.join("order.edi");
        let largest = std::rc::Rc::new(std::cell::Cell::new(0));
        let made = |breaks| {
            let largest = std::rc::Rc::clone(&largest);
            Outgoing::new(LENGTH, move || {
                Ok(Box::new(Made {
                    at: 0,
                    length: LENGTH,
                    breaks,
                    largest: std::rc::Rc::clone(&largest),
                }))
            })
        };
        send(&target, &me(), &made(None)).expect("sending");
        let written = fs::read(&target).expect("sent");
        assert_eq!(written.len() as u64, LENGTH);
        assert!(
            written
                .iter()
                .enumerate()
                .all(|(at, byte)| usize::from(*byte) == at % 251)
        );
        assert!(
            largest.get() <= 8 * 1024,
            "read {} bytes at once",
            largest.get()
        );

        let broken = send(&target, &me(), &made(Some(LENGTH / 2))).expect_err("broken");
        assert!(broken.retryable, "{broken}");
        assert_eq!(fs::read(&target).expect("still there").len() as u64, LENGTH);
        assert_eq!(names(&dir), ["order.edi"], "the written file removed");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_node_starting_while_a_send_writes_leaves_the_sent_file_be() {
        let dir = scratch("file-send-in-flight");
        let target = dir.join("order.edi");
        let address = |path: &Path| path.to_str().expect("a UTF-8 path").to_string();
        let (directory, sent_to) = (address(&dir), address(&target));
        let written = |file: &mut File, bytes: &[u8]| {
            file.write_all(bytes).map_err(|e| classify("writing", &e))
        };

        let sent = send_by(&target, &me(), |file| {
            written(file, b"ISA")?;
            // The same node builds a Receive Location over the directory
            // and a Send Location to the target, as a node starting does.
            for (at, side) in [(&directory, Applies::Receive), (&sent_to, Applies::Send)] {
                FileTransport::open(at, side, &[])
                    .and_then(|transport| transport.on_node(&node()))
                    .expect("built on its node");
            }
            written(file, b"*00*")
        });

        sent.expect("sent whole");
        assert_eq!(fs::read(&target).expect("sent"), b"ISA*00*");
        assert_eq!(names(&dir), ["order.edi"]);
        let _ = fs::remove_dir_all(&dir);
    }
}
