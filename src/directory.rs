//! A drop directory as the claim reaches it (`transport::claim::ClaimSite`).
//!
//! The claim itself — the turn, the rename to a name recording the node,
//! the return, the recovery of a starting node — is the transport
//! capability's, written once for the file family (ADR-0024, amendments
//! 2026-09-26 and 2026-10-13). What is here is how a file system does each
//! step: a turn is a file created only where none of its name exists,
//! holding its claimant's name; a claim is a rename.
//!
//! **A return is a hard link**, made only where the dropped name is free,
//! then the claimed name removed. Where the file system makes no hard link
//! the file waits: a rename replaces what it lands on, so it could lose a
//! file dropped under that name meanwhile.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use transport::claim::{ClaimSite, Listed, Restore, is_turn};
use transport::error::{Result, classify};

/// A file as a listing found it: its length and its modification time.
/// The same stamp at two listings is a finished file; the same stamp as
/// when it was refused is the refused file, still lying there.
pub type Stamp = (u64, Option<SystemTime>);

/// One drop directory.
#[derive(Clone, Debug)]
pub struct Directory {
    pub(crate) root: PathBuf,
}

impl Directory {
    pub(crate) fn at(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Where `name` lies.
    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    /// Every name in the directory, or none where it does not exist yet:
    /// a Receive Location nobody has sent to, a Send Location never sent.
    pub(crate) fn names(&self) -> Result<Vec<String>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(classify("reading the drop directory", &e)),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| classify("listing the drop directory", &e))?;
            if let Ok(name) = entry.file_name().into_string() {
                names.push(name);
            }
        }
        Ok(names)
    }
}

/// The stamp of the file at `path`; nothing where it is not a file, or
/// is gone.
pub(crate) fn stamp(path: &Path) -> Option<Stamp> {
    let metadata = fs::metadata(path).ok()?;
    metadata
        .is_file()
        .then(|| (metadata.len(), metadata.modified().ok()))
}

impl ClaimSite for Directory {
    type Stamp = Stamp;

    /// Every file; the claim leaves out what is no arrival.
    fn listed(&self) -> Result<Vec<Listed<Stamp>>> {
        Ok(self
            .names()?
            .into_iter()
            .filter_map(|name| {
                let found = stamp(&self.path(&name))?;
                Some((name, Some(found)))
            })
            .collect())
    }

    fn turns(&self) -> Result<Vec<String>> {
        Ok(self
            .names()?
            .into_iter()
            .filter(|name| is_turn(name))
            .collect())
    }

    fn stamp(&self, name: &str) -> Result<Option<Stamp>> {
        Ok(stamp(&self.path(name)))
    }

    /// Created only where nothing of its name exists, holding the
    /// claimant's name. Anything the file system refuses — the name taken,
    /// a turn being removed — is the turn not had now.
    fn take_turn(&self, turn: &str, claimant: &str) -> Result<bool> {
        let Ok(mut held) = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path(turn))
        else {
            return Ok(false);
        };
        if held.write_all(claimant.as_bytes()).is_err() {
            drop(held);
            let _ = fs::remove_file(self.path(turn));
            return Ok(false);
        }
        Ok(true)
    }

    fn holder(&self, turn: &str) -> Result<Option<String>> {
        Ok(fs::read(self.path(turn))
            .ok()
            .and_then(|held| String::from_utf8(held).ok()))
    }

    fn drop_turn(&self, turn: &str, _claimant: &str) -> Result<()> {
        match fs::remove_file(self.path(turn)) {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            done => done.map_err(|e| classify("removing a turn", &e)),
        }
    }

    /// Refused as the operating system refuses a file a producer holds.
    fn rename(&self, from: &str, to: &str) -> Result<bool> {
        Ok(fs::rename(self.path(from), self.path(to)).is_ok())
    }

    fn restore(&self, claimed: &str, dropped: &str) -> Result<Restore> {
        restore_by(&self.path(claimed), &self.path(dropped), |from, to| {
            fs::hard_link(from, to)
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        match fs::remove_file(self.path(name)) {
            // Gone already: consumed, which is what was asked.
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            done => done.map_err(|e| classify("deleting a file", &e)),
        }
    }
}

/// The claimed file at `claimed` back at `dropped`, linking through `link`:
/// a test hands in a file system that makes no hard link. Its length and
/// modification time are kept.
fn restore_by(
    claimed: &Path,
    dropped: &Path,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<Restore> {
    // A hard link is made only where the name is free, so nothing dropped
    // since is overwritten. It is the only way back: a rename replaces what
    // it lands on — on Windows `std::fs::rename` is `MoveFileExW` with
    // `MOVEFILE_REPLACE_EXISTING`, on Unix `rename(2)` — so a rename after
    // finding the name free loses a file a producer drops between the two.
    // The refusing renames (`MoveFileExW` without the flag, `renameat2`
    // with `RENAME_NOREPLACE`) are C calls, and this crate forbids unsafe
    // code (ADR-0050). So where no link is made the file stays claimed and
    // waits: held, never risked.
    match link(claimed, dropped) {
        Ok(()) => {}
        Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
            return Ok(Restore::Kept(format!(
                "returning {}: a file of that name was dropped since",
                dropped.display()
            )));
        }
        Err(e) if claimed.exists() => {
            return Ok(Restore::Kept(format!(
                "returning {}: the file system made no hard link ({e}), and only a link \
                 never replaces a file dropped since",
                dropped.display()
            )));
        }
        Err(_) => return Ok(Restore::Gone),
    }
    fs::remove_file(claimed).map_err(|e| classify("returning a claimed file", &e))?;
    Ok(Restore::Back)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A claimed file `order.edi` in a fresh scratch directory, holding
    /// `claimed`.
    fn claimed_in(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("creating the scratch directory");
        let claimed = dir.join("order.edi.xmip-claim.process-1.1");
        fs::write(&claimed, b"claimed").expect("writing the claimed file");
        claimed
    }

    #[test]
    fn a_name_taken_before_the_return_is_never_replaced() {
        let claimed = claimed_in("file-give-back-taken");
        let dropped = claimed.with_file_name("order.edi");
        fs::write(&dropped, b"newer").expect("a producer drops a newer file");

        let kept = restore_by(&claimed, &dropped, |from, to| fs::hard_link(from, to));

        assert!(
            matches!(kept, Ok(Restore::Kept(ref why)) if why.contains("dropped since")),
            "{kept:?}"
        );
        assert_eq!(fs::read(&dropped).expect("the newer file"), b"newer");
        assert_eq!(fs::read(&claimed).expect("still claimed"), b"claimed");
        let _ = fs::remove_dir_all(claimed.parent().expect("its directory"));
    }

    #[test]
    fn without_hard_links_a_name_taken_during_the_return_is_never_replaced() {
        let claimed = claimed_in("file-give-back-no-link");
        let dropped = claimed.with_file_name("order.edi");

        // The file system makes no link, and a producer drops a newer file
        // just as the return finds out: where a rename once followed.
        let kept = restore_by(&claimed, &dropped, |_, to| {
            fs::write(to, b"newer").expect("a producer drops a newer file");
            Err(io::Error::from(io::ErrorKind::Unsupported))
        });

        assert!(
            matches!(kept, Ok(Restore::Kept(ref why)) if why.contains("no hard link")),
            "{kept:?}"
        );
        assert_eq!(fs::read(&dropped).expect("the newer file"), b"newer");
        assert_eq!(fs::read(&claimed).expect("still claimed"), b"claimed");
        let _ = fs::remove_dir_all(claimed.parent().expect("its directory"));
    }

    #[test]
    fn a_turn_is_had_once_and_says_who_holds_it() {
        let claimed = claimed_in("file-turn");
        let directory = Directory::at(claimed.parent().expect("its directory"));
        assert!(
            directory
                .take_turn("order.edi.xmip-claim", "one")
                .expect("had")
        );
        assert!(
            !directory
                .take_turn("order.edi.xmip-claim", "two")
                .expect("asked")
        );
        assert_eq!(
            directory
                .holder("order.edi.xmip-claim")
                .expect("read")
                .as_deref(),
            Some("one")
        );
        assert_eq!(directory.turns().expect("listed"), ["order.edi.xmip-claim"]);
        directory
            .drop_turn("order.edi.xmip-claim", "one")
            .expect("dropped");
        assert_eq!(directory.turns().expect("listed"), [] as [String; 0]);
        let _ = fs::remove_dir_all(&directory.root);
    }
}
