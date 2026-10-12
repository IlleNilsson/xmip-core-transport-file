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
//! Written, it is synced to disk and renamed over the target in one step:
//! a receiver finds the target as it was before or whole, never in between.
//! A rename replaces what it lands on (`std::fs::rename`: `MoveFileExW`
//! with `MOVEFILE_REPLACE_EXISTING` on Windows, `rename(2)` on Unix), so a
//! send to a name already there replaces that file, as writing it did.
//! Where anything fails, the written file is removed and the target is left
//! as it was.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use transport::error::{Result, classify};

/// What a file being sent carries between the target's name and the rest.
const MARK: &str = ".xmip-sending.";

/// Sends made by this process so far: two sends at once, from two threads,
/// never write to one file.
static SENDS: AtomicU64 = AtomicU64::new(0);

/// `bytes` at `path`, whole or not at all.
///
/// # Errors
/// As the file system refused the write, the sync or the rename; the
/// target is left as it was.
pub(crate) fn send(path: &Path, bytes: &[u8]) -> Result<()> {
    send_by(path, bytes, Write::write_all)
}

/// [`send`], writing through `write`: a test hands in a write that fails
/// half way.
fn send_by(
    path: &Path,
    bytes: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
) -> Result<()> {
    let sending = sending_name(path).ok_or_else(|| {
        classify(
            "naming the sent file",
            &io::Error::from(io::ErrorKind::InvalidInput),
        )
    })?;
    let written = File::create_new(&sending).and_then(|mut file| {
        write(&mut file, bytes)?;
        file.sync_all()
    });
    match written.and_then(|()| fs::rename(&sending, path)) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&sending);
            Err(classify("writing the sent file", &e))
        }
    }
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
        send(&target, b"old").expect("sending");
        send(&target, b"new").expect("sending again");

        assert_eq!(fs::read(&target).expect("sent"), b"new", "replaced");
        assert_eq!(names(&dir), ["order.edi"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_send_leaves_the_target_as_it_was() {
        let dir = scratch("file-send-failed");
        let target = dir.join("order.edi");
        fs::write(&target, b"before").expect("a file already there");

        let failed = send_by(&target, b"ISA*00*", |file, bytes| {
            file.write_all(&bytes[..3])?;
            Err(io::Error::other("the disk is full"))
        });

        assert!(failed.is_err());
        assert_eq!(fs::read(&target).expect("still there"), b"before");
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
