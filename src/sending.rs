//! A sent file written whole before it is there.
//!
//! A receiver lists a drop directory whenever it likes, so a file written in
//! place can be found half written. A send therefore writes a file of its
//! own beside the target — same directory, so same file system — named so
//! no listing takes it: the target's name, the mark `.xmip-sending.`, the
//! process id, a count within the process and the time in nanoseconds since
//! the Unix epoch:
//!
//! ```text
//! order.edi.xmip-sending.4242.7.1790000000123456789
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
//! as it was.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use transport::Outgoing;
use transport::error::{Result, classify};

/// What a file being sent carries between the target's name and the rest.
const MARK: &str = ".xmip-sending.";

/// Sends made by this process so far: two sends at once, from two threads,
/// never write to one file.
static SENDS: AtomicU64 = AtomicU64::new(0);

/// `outgoing`'s body at `path`, whole or not at all.
///
/// # Errors
/// As the body could not be read, or the file system refused the write,
/// the sync or the rename; the target is left as it was.
pub(crate) fn send(path: &Path, outgoing: &Outgoing<'_>) -> Result<()> {
    send_by(path, |file| outgoing.write_to(file).map(drop))
}

/// [`send`], writing through `write`: a test hands in a write that fails
/// half way.
fn send_by(path: &Path, write: impl FnOnce(&mut File) -> Result<()>) -> Result<()> {
    let refused = |e: &io::Error| classify("writing the sent file", e);
    let sending = sending_name(path).ok_or_else(|| {
        classify(
            "naming the sent file",
            &io::Error::from(io::ErrorKind::InvalidInput),
        )
    })?;
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
    written
}

/// The name `path` is written under before it is renamed into place;
/// nothing where `path` names no file.
fn sending_name(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let count = SENDS.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let process = std::process::id();
    Some(path.with_file_name(format!("{name}{MARK}{process}.{count}.{nanos}")))
}

/// Whether the name of `path` is a file being sent: never an arrival.
pub(crate) fn is_sending(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.rfind(MARK).map(|at| (at, &name[at + MARK.len()..])))
        .is_some_and(|(at, rest)| {
            let parts: Vec<&str> = rest.split('.').collect();
            at > 0
                && parts.len() == 3
                && parts
                    .iter()
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

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
        send(&target, &Outgoing::held(b"old")).expect("sending");
        send(&target, &Outgoing::held(b"new")).expect("sending again");

        assert_eq!(fs::read(&target).expect("sent"), b"new", "replaced");
        assert_eq!(names(&dir), ["order.edi"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_send_leaves_the_target_as_it_was() {
        let dir = scratch("file-send-failed");
        let target = dir.join("order.edi");
        fs::write(&target, b"before").expect("a file already there");

        let failed = send_by(&target, |file| {
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
        send(&target, &made(None)).expect("sending");
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

        let broken = send(&target, &made(Some(LENGTH / 2))).expect_err("broken");
        assert!(broken.retryable, "{broken}");
        assert_eq!(fs::read(&target).expect("still there").len() as u64, LENGTH);
        assert_eq!(names(&dir), ["order.edi"], "the written file removed");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn only_a_name_being_sent_reads_as_one() {
        let named = sending_name(Path::new("in/order.edi")).expect("a name");
        assert!(is_sending(&named), "{}", named.display());
        assert!(!is_sending(Path::new("order.edi")));
        assert!(!is_sending(Path::new(".xmip-sending.1.2.3")));
        assert!(!is_sending(Path::new("order.edi.xmip-sending.1.2")));
        assert!(!is_sending(Path::new("order.edi.xmip-sending.1.x.3")));
    }
}
