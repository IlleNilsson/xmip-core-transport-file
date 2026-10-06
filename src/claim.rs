//! A dropped file claimed by renaming it, and returned to its drop
//! directory by renaming it back.
//!
//! ADR-0024, amendment 2026-09-26: asked to settle how a file is claimed,
//! the owner chose *an atomic rename*. A node claims a file by renaming it
//! to a name that says which node took it and when; whichever rename
//! succeeds owns the file, on a local disk and on a network share alike.
//! Nothing reads the file under its dropped name once it is claimed, and
//! nothing else takes it while it is claimed.
//!
//! **The claimed name** is the dropped name, the mark `.xmip-claim.`, the
//! node's location percent-encoded so that a file name carries it on every
//! operating system (`net::percent`), and the time of the claim in
//! nanoseconds since the Unix epoch:
//!
//! ```text
//! order.edi.xmip-claim.xmip%3A%2F%2F%2F<cluster>%2Fnode%2F<node>.1790000000123456789
//! ```
//!
//! **Two renames of one file are made one at a time.** A rename is atomic,
//! and two of the same file to two names are not exclusive: Windows renames
//! through a handle opened on the file first, so two nodes that both open it
//! both rename it, the second moving it from under the first (found by this
//! crate's test of two receivers, 2026-10-03). So the rename is made holding
//! the file's turn, `order.edi.xmip-claim`, created only where it does not
//! exist — the one exclusive step every file system offers alike — holding
//! the claimant's name, and removed once the rename is done. A node that
//! finds the turn taken leaves the file to the next listing.
//!
//! A name carrying the mark, or a turn's name, is never an arrival.
//!
//! **What the rename costs**, as the amendment says: a node that dies after
//! the rename leaves a claimed file behind, so a node that starts finds
//! every claim its own name records in the drop directory and returns it,
//! and removes any turn it held ([`Claimant::recover`]): whatever it held
//! before, nothing holds now. A claim whose dropped name a newer file has
//! taken waits for it (`waiting.rs`), as every return does.
//!
//! **A return is a hard link**, made only where the dropped name is free,
//! then the claimed name removed ([`give_back`]). Where the file system
//! makes no hard link the file waits as well: a rename replaces what it
//! lands on, so it could lose a file dropped under that name meanwhile.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use transport::NodeLocation;
use transport::TransportError;
use transport::error::{Result, classify};

/// What a claimed name carries between the dropped name and the claimant.
const MARK: &str = ".xmip-claim.";

/// What a file's turn adds to its dropped name.
const TURN: &str = ".xmip-claim";

/// Who claims: the node, as a claimed name records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Claimant(String);

impl Claimant {
    /// The node at `node`, every byte outside letters, digits, `-` and `_`
    /// percent-encoded: no `.`, so a claimed name reads back in one way.
    pub(crate) fn of(node: &NodeLocation) -> Self {
        Self(net::percent::encode_keeping(node.as_str(), |byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
        }))
    }

    /// A transport not yet given its node — a loopback, a test: this
    /// process, so that two processes never mistake one another's claims.
    /// Nothing returns its claims; a node does that as it starts.
    pub(crate) fn process() -> Self {
        Self(format!("process-{}", std::process::id()))
    }

    /// Claim the file at `path` where `still` says, holding its turn, that
    /// it is the file that was found: renamed to its claimed name, which is
    /// handed back. `None` where it is not to be had now — its turn is
    /// another's, another node renamed it first, or the operating system
    /// refused, as it does a file a producer holds — so it is left to the
    /// next listing.
    pub(crate) fn claim(&self, path: &Path, still: impl FnOnce() -> bool) -> Option<PathBuf> {
        let name = path.file_name()?.to_str()?;
        let turn = path.with_file_name(format!("{name}{TURN}"));
        let mut held = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&turn)
            .ok()?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let claimed = path.with_file_name(format!("{name}{MARK}{}.{nanos}", self.0));
        let renamed = held.write_all(self.0.as_bytes()).is_ok()
            && still()
            && fs::rename(path, &claimed).is_ok();
        drop(held);
        let _ = fs::remove_file(&turn);
        renamed.then_some(claimed)
    }

    /// Every file this claimant left claimed in `root`, to be returned to
    /// it under the name it was dropped with, and every turn it left,
    /// removed: what a node does as it starts, since whatever it held
    /// before, nothing holds now.
    ///
    /// # Errors
    /// Where `root` could not be read.
    pub(crate) fn recover(&self, root: &Path) -> Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(classify("reading the drop directory", &e)),
        };
        let mut left = Vec::new();
        for entry in entries {
            let path = entry
                .map_err(|e| classify("listing the drop directory", &e))?
                .path();
            if is_turn(&path) {
                if fs::read(&path).is_ok_and(|held| held == self.0.as_bytes()) {
                    let _ = fs::remove_file(&path);
                }
                continue;
            }
            if claim_of(&path).is_some_and(|claim| claim.claimant == self.0) {
                left.push(path);
            }
        }
        Ok(left)
    }
}

/// A claimed name, read back.
struct Claim<'a> {
    /// The name the file was dropped with.
    dropped: &'a str,
    /// Who claimed it.
    claimant: &'a str,
}

/// The claim the name of `path` records; nothing where it records none.
fn claim_of(path: &Path) -> Option<Claim<'_>> {
    let name = path.file_name()?.to_str()?;
    let at = name.rfind(MARK)?;
    let (claimant, nanos) = name[at + MARK.len()..].split_once('.')?;
    let read = at > 0
        && !claimant.is_empty()
        && !nanos.is_empty()
        && nanos.bytes().all(|byte| byte.is_ascii_digit());
    read.then(|| Claim {
        dropped: &name[..at],
        claimant,
    })
}

/// Whether `path` is a file's turn.
fn is_turn(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.len() > TURN.len() && name.ends_with(TURN))
}

/// Whether the name of `path` records a claim or is a file's turn: such a
/// file is never an arrival.
pub(crate) fn is_claimed(path: &Path) -> bool {
    is_turn(path) || claim_of(path).is_some()
}

/// The claimed file at `claimed` back in its drop directory under the name
/// it was dropped with — never over a file dropped there since — and that
/// path. Its length and modification time are kept.
///
/// # Errors
/// Retryable where a file of the dropped name is there again: the claimed
/// one is left as it is. Where the file system makes no hard link, the
/// claimed one is left as it is too, and the error says so. Otherwise as
/// the file system refused.
pub(crate) fn give_back(claimed: &Path) -> Result<PathBuf> {
    give_back_by(claimed, |from, to| fs::hard_link(from, to))
}

/// [`give_back`], linking through `link`: a test hands in a file system
/// that makes no hard link.
fn give_back_by(
    claimed: &Path,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<PathBuf> {
    let dropped = claim_of(claimed)
        .map(|claim| claimed.with_file_name(claim.dropped))
        .ok_or_else(|| {
            TransportError::permanent(format!("{} is not a claimed file", claimed.display()))
        })?;
    // A hard link is made only where the name is free, so nothing dropped
    // since is overwritten. It is the only way back: a rename replaces what
    // it lands on — on Windows `std::fs::rename` is `MoveFileExW` with
    // `MOVEFILE_REPLACE_EXISTING`, on Unix `rename(2)` — so a rename after
    // finding the name free loses a file a producer drops between the two.
    // The refusing renames (`MoveFileExW` without the flag, `renameat2`
    // with `RENAME_NOREPLACE`) are C calls, and this crate forbids unsafe
    // code (ADR-0050). So where no link is made the file stays claimed and
    // waits (`waiting.rs`): held, never risked.
    match link(claimed, &dropped) {
        Ok(()) => {}
        Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
            return Err(TransportError::retryable(format!(
                "returning {}: a file of that name was dropped since",
                dropped.display()
            )));
        }
        Err(e) if claimed.exists() => {
            return Err(TransportError::retryable(format!(
                "returning {}: the file system made no hard link ({e}), and only a link \
                 never replaces a file dropped since",
                dropped.display()
            )));
        }
        Err(e) => return Err(classify("returning a claimed file", &e)),
    }
    fs::remove_file(claimed).map_err(|e| classify("returning a claimed file", &e))?;
    Ok(dropped)
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
        let claimed = dir.join(format!("order.edi{MARK}process-1.1"));
        fs::write(&claimed, b"claimed").expect("writing the claimed file");
        claimed
    }

    #[test]
    fn a_name_taken_before_the_return_is_never_replaced() {
        let claimed = claimed_in("file-give-back-taken");
        let dropped = claimed.with_file_name("order.edi");
        fs::write(&dropped, b"newer").expect("a producer drops a newer file");

        let refused = give_back(&claimed).expect_err("not returned");

        assert!(
            refused.message.contains("dropped since"),
            "{}",
            refused.message
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
        let refused = give_back_by(&claimed, |_, to| {
            fs::write(to, b"newer").expect("a producer drops a newer file");
            Err(io::Error::from(io::ErrorKind::Unsupported))
        })
        .expect_err("not returned");

        assert!(
            refused.message.contains("no hard link"),
            "{}",
            refused.message
        );
        assert_eq!(fs::read(&dropped).expect("the newer file"), b"newer");
        assert_eq!(fs::read(&claimed).expect("still claimed"), b"claimed");
        let _ = fs::remove_dir_all(claimed.parent().expect("its directory"));
    }
}
