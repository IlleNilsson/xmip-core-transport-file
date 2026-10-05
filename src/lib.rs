#![forbid(unsafe_code)]

//! Streams that arrive as files in a directory.
//!
//! The polled case: nothing is pushed to Xmip, Xmip goes and looks. Identity is
//! therefore *inferred* from the Receive Location rather than passed by a caller
//! — ADR-0019 clause 8.
//!
//! **A file is taken once it is finished, and by one node.** A listing takes
//! a file only where its length and modification time are what the listing
//! before found (ADR-0024 clause 5, the stability check), so a file a
//! producer is still writing is left until it stops growing. It is then
//! claimed by renaming it to a name recording the node and the time
//! (`claim.rs`; ADR-0024, amendment 2026-09-26): whichever node's rename
//! succeeds has it, and every other finds it gone. The node is the one the
//! runtime names as it builds the transport ([`Configured::on_node`]), and
//! a node that starts returns to the drop directory whatever its name still
//! holds there, since nothing holds it now.
//!
//! **A file is consumed only on its verdict.** `receive` hands each claimed
//! file back as a reader over it, opened as the runtime first reads it; the
//! file stays claimed until the receive cycle ends. `Accepted` deletes it:
//! the Stream is Xmip's. `Refused` returns it to the drop directory under its
//! dropped name, and this Location does not receive it again while it stays
//! as it was refused: a refusal at a transport gate kept nothing in Xmip
//! (runtime-model section 5), so the file is the only copy, and a drop
//! directory has no refused place to move it to. A file written again under
//! the same name — another length or another modification time — is a new
//! arrival. `Failed`, or no verdict at all, returns it, and a later receive
//! finds it again.
//!
//! **A return never waits for a restart.** A file is never returned over
//! one dropped under its name since; such a file waits, held under its
//! claimed name with why ([`FileTransport::held`]), and goes back as soon
//! as the name is free: once this Location consumes the newer file, or at
//! the next receive (`waiting.rs`).

mod claim;
mod waiting;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use transport::Acknowledgement;
use transport::Arrived;
use transport::Configured;
use transport::Directions;
use transport::NodeLocation;
use transport::Transport;
use transport::Verdict;
use transport::arrived::next_arrival;
use transport::body::opened;
use transport::claim::{Artefact, Claimed, ResourceClaim};
use transport::error::{Result, TransportError, classify};
use transport::held::Held;
use transport::loopback::{FarEnd, Loopback};
use xcore::settings::{Read, Settings};

use crate::claim::{Claimant, is_claimed};
use crate::waiting::Waiting;

pub use crate::waiting::HeldFile;

/// A file as a listing found it: its length and its modification time.
/// The same stamp at two listings is a finished file; the same stamp as
/// when it was refused is the refused file, still lying there.
type Stamp = (u64, Option<SystemTime>);

/// Files by path, each with its stamp.
type Stamps = HashMap<PathBuf, Stamp>;

/// The refused files a Location leaves where they lie, each as it was
/// refused: the capability's one memory of them (`transport::refused`).
type Refused = transport::Refused<PathBuf, Stamp>;

pub struct FileTransport {
    root: PathBuf,
    claimant: Claimant,
    /// What the last listing found, not yet taken: the stability check's
    /// other half.
    listed: Mutex<Stamps>,
    refused: Refused,
    waiting: Waiting,
}

impl FileTransport {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            claimant: Claimant::process(),
            listed: Mutex::default(),
            refused: Refused::default(),
            waiting: Waiting::default(),
        }
    }

    /// The claimed files this Location could not return yet, since a file
    /// was dropped under the same name meanwhile: where each lies, and
    /// why. Each goes back as soon as its name is free.
    #[must_use]
    pub fn held(&self) -> Vec<HeldFile> {
        self.waiting.held()
    }

    /// One claimed file: read as the runtime asks, deleted on `Accepted`,
    /// returned and remembered as refused on `Refused`, returned on
    /// `Failed` or when let go without a verdict.
    fn arrived(&self, dropped: &Path, claimed: PathBuf) -> Arrived {
        // `file://` and the dropped path as `net::uri` writes it:
        // `file:///C:/in/a.edi`.
        let origin = format!("file://{}", net::uri::path_of(dropped));
        let read = claimed.clone();
        let hold = Hold {
            claimed,
            refused: self.refused.clone(),
            waiting: self.waiting.clone(),
            told: false,
        };
        let acknowledgement = Acknowledgement::deferred(move |verdict| hold.tell(verdict));
        // Opened on its first read, so a drop directory of many files holds
        // one handle per file being read, not one per file listed.
        let body =
            opened(move || File::open(&read).map_err(|e| classify("opening a dropped file", &e)));
        Arrived::new(origin, body, acknowledgement)
    }
}

/// A claimed file held through its receive cycle: told its verdict once,
/// and returned to the drop directory if it never is.
struct Hold {
    claimed: PathBuf,
    refused: Refused,
    waiting: Waiting,
    told: bool,
}

impl Hold {
    fn tell(mut self, verdict: Verdict) -> Result<()> {
        self.told = true;
        match verdict {
            Verdict::Accepted => {
                match fs::remove_file(&self.claimed) {
                    // Gone already: consumed, which is what was asked.
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
                    done => done.map_err(|e| classify("deleting a received file", &e))?,
                }
                // Its dropped name is free now: a file waiting for it goes back.
                self.waiting.settle(&self.refused);
                Ok(())
            }
            Verdict::Refused(_) => self.waiting.give_back(&self.claimed, Some(&self.refused)),
            Verdict::Failed => self.waiting.give_back(&self.claimed, None),
        }
    }
}

impl Drop for Hold {
    /// Let go without a verdict: nothing was consumed, so the file goes
    /// back where it was dropped.
    fn drop(&mut self) {
        if !self.told {
            let _ = self.waiting.give_back(&self.claimed, None);
        }
    }
}

impl Transport for FileTransport {
    fn name(&self) -> &'static str {
        "file"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a receive lists again what is not yet told")
    }

    fn receive(&self) -> Result<Vec<Arrived>> {
        // A held file whose name was freed since goes back first, and is
        // listed with the rest.
        self.waiting.settle(&self.refused);
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            // A drop directory that does not exist yet is not a failure. It is
            // a Receive Location nobody has sent to.
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(classify("reading the drop directory", &e)),
        };

        let mut found = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| classify("listing the drop directory", &e))?;
            let path = entry.path();
            if let Some(stamp) = stamp(&path)
                && !is_claimed(&path)
            {
                found.push((path, stamp));
            }
        }
        // A file still as it was refused is left out; a file gone or
        // written again is forgotten, as what this listing did not take is,
        // so neither memory is ever more than the directory holds.
        let found = self
            .refused
            .sift(found, |(path, _)| path, |(_, stamp)| Some(*stamp));
        let mut arrived = Vec::new();
        let mut seen = Stamps::new();
        let mut listed = lock(&self.listed);

        for (path, stamp) in found {
            // Unchanged since the last listing, and still so once its turn
            // is held: finished, and the file that was found.
            let finished = listed.get(&path) == Some(&stamp);
            let unchanged = || self::stamp(&path) == Some(stamp);
            match finished
                .then(|| self.claimant.claim(&path, unchanged))
                .flatten()
            {
                Some(claimed) => {
                    arrived.push(self.arrived(&path, claimed));
                }
                None => {
                    seen.insert(path, stamp);
                }
            }
        }

        *listed = seen;
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let path = self.root.join(target);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| classify("creating the send directory", &e))?;
        }

        fs::write(&path, bytes).map_err(|e| classify("writing the sent file", &e))
    }

    /// A file's own claim: the rename (ADR-0024, amendment 2026-09-26).
    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(self)
    }
}

/// The claim [`FileTransport::receive`] takes, for a caller holding one
/// file by its path: the artefact's address is the path.
impl ResourceClaim for FileTransport {
    fn is_available(&self, artefact: &Artefact) -> Result<bool> {
        let path = Path::new(artefact.address());
        Ok(stamp(path).is_some() && !is_claimed(path))
    }

    fn claim(&self, artefact: &Artefact) -> Result<Claimed> {
        let claimed = self
            .claimant
            .claim(Path::new(artefact.address()), || true)
            .ok_or_else(|| TransportError::retryable(format!("{artefact} is held, or gone")))?;
        Ok(Claimed::new(
            artefact.clone(),
            claimed.to_string_lossy().into_owned(),
        ))
    }

    fn release(&self, claimed: Claimed) -> Result<()> {
        self.waiting.give_back(Path::new(&claimed.token), None)
    }
}

/// The stamp of the file at `path`; nothing where it is not a file, or
/// is gone.
fn stamp(path: &Path) -> Option<Stamp> {
    let metadata = fs::metadata(path).ok()?;
    metadata
        .is_file()
        .then(|| (metadata.len(), metadata.modified().ok()))
}

fn lock(stamps: &Mutex<Stamps>) -> std::sync::MutexGuard<'_, Stamps> {
    stamps.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Configured for FileTransport {
    /// The address is the directory: a Receive Location's drop directory, a
    /// Send Location's target. Nothing else is read.
    const SETTINGS: &'static Settings = &Settings::none(env!("CARGO_PKG_NAME"));

    fn configured(address: &str, _settings: &Read) -> Result<Self> {
        Ok(Self::new(address))
    }

    /// Claims made in `node`'s name from here on, and every claim that name
    /// still holds in the directory returned to it: the node is starting,
    /// so whatever it held before, nothing holds now (ADR-0024, amendment
    /// 2026-09-26). One whose dropped name a newer file has taken is held
    /// until the name is free.
    fn on_node(mut self, node: &NodeLocation) -> Result<Self> {
        self.claimant = Claimant::of(node);
        for claimed in self.claimant.recover(&self.root)? {
            // Held where it cannot go back now, and visible as such.
            let _ = self.waiting.give_back(&claimed, None);
        }
        Ok(self)
    }
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

impl Loopback for FileTransport {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    use transport::Refusal;
    use xcore::settings::Applies;

    fn scratch(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();

        let dir = std::env::temp_dir().join(format!("xmip-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("creating the scratch directory");

        dir
    }

    /// A node location for the node at `place`. It names no node: the test
    /// cluster's names (`configure::fixture`) are not this crate's to
    /// depend on, and a claim compares the location whole, never reads it.
    fn node(place: usize) -> NodeLocation {
        NodeLocation::new(format!("process-{}/place-{place}", std::process::id()))
    }

    /// A Receive Location at `dir` on the node at `place`, built as the
    /// runtime builds it.
    fn on(dir: &Path, place: usize) -> FileTransport {
        let address = dir.to_str().expect("a UTF-8 path");
        FileTransport::open(address, Applies::Receive, &[])
            .and_then(|transport| transport.on_node(&node(place)))
            .expect("built on its node")
    }

    /// What two listings take: the first finds, the second takes what the
    /// first found unchanged.
    fn settled(transport: &FileTransport) -> Vec<Arrived> {
        assert!(
            transport.receive().expect("listing").is_empty(),
            "nothing is taken at its first listing"
        );
        transport.receive().expect("listing again")
    }

    /// The names in `dir`.
    fn names(dir: &Path) -> BTreeSet<String> {
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
    fn file_declares_it_takes_nothing_beyond_its_directory() {
        use xcore::settings::Given;
        assert!(FileTransport::SETTINGS.settings.is_empty());
        let transport = FileTransport::open("in", Applies::Receive, &[]).expect("configured");
        assert_eq!(transport.root, PathBuf::from("in"));
        let unknown = [("colour".to_string(), Given::Text("lime".to_string()))];
        let refused = FileTransport::open("in", Applies::Receive, &unknown)
            .err()
            .expect("refused");
        assert!(
            refused.message.contains("\"colour\""),
            "{}",
            refused.message
        );
    }

    #[test]
    fn file_transport_declares_both_directions_and_claims_its_files() {
        let transport = FileTransport::new(std::env::temp_dir());

        assert!(transport.directions().receives());
        assert!(transport.directions().sends());
        assert_eq!(transport.name(), "file");
        assert!(transport.claims().is_some(), "files are artefacts");
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
        let mut arrived = settled(&transport);

        assert_eq!(arrived.len(), 1);
        let taken = arrived.remove(0).taken().expect("taken");
        assert_eq!(taken.bytes, b"ISA*00*");
        assert!(taken.origin_uri.starts_with("file:///"));
        assert!(
            taken.origin_uri.ends_with("/order-1001.edi"),
            "the origin is the dropped name: {}",
            taken.origin_uri
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_file_uri_uses_forward_slashes_everywhere() {
        let dir = scratch("file-uri");
        let transport = FileTransport::new(&dir);

        transport.send("order.edi", b"x").expect("sending");
        let arrived = settled(&transport);

        assert!(!arrived[0].origin_uri.contains(char::from(92)));
        assert!(
            !arrived[0].origin_uri.starts_with("file:////"),
            "three slashes, not four, before a Unix path"
        );

        drop(arrived);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_claimed_file_is_renamed_to_a_name_recording_its_node() {
        let dir = scratch("file-claimed");
        let transport = on(&dir, 0);
        transport.send("order.edi", b"ISA*00*").expect("sending");

        let arrived = settled(&transport);
        assert_eq!(arrived.len(), 1);
        let held = names(&dir);
        assert!(!held.contains("order.edi"), "claimed, it is gone: {held:?}");
        let claimed = held.iter().next().expect("the claimed file");
        let encoded = net::percent::encode_keeping(node(0).as_str(), |byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
        });
        assert!(
            claimed.starts_with(&format!("order.edi.xmip-claim.{encoded}.")),
            "{claimed}"
        );

        drop(arrived);
        assert_eq!(
            names(&dir),
            BTreeSet::from(["order.edi".to_string()]),
            "let go without a verdict, it is returned"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_receivers_over_one_folder_take_each_file_once() {
        const FILES: usize = 200;
        let dir = scratch("file-two-receivers");
        let sender = FileTransport::new(&dir);
        for file in 0..FILES {
            sender
                .send(&format!("order-{file}.edi"), file.to_string().as_bytes())
                .expect("sending");
        }

        let receivers = [on(&dir, 0), on(&dir, 1)];
        let dir = dir.as_path();
        let took: Vec<Vec<String>> = std::thread::scope(|scope| {
            let running: Vec<_> = receivers
                .iter()
                .map(|receiver| {
                    scope.spawn(move || {
                        let mut took = Vec::new();
                        while !names(dir).is_empty() {
                            for arrived in receiver.receive().expect("receiving") {
                                let taken = arrived.taken().expect("taken");
                                took.push(String::from_utf8(taken.bytes).expect("UTF-8"));
                            }
                        }
                        took
                    })
                })
                .collect();
            running
                .into_iter()
                .map(|thread| thread.join().expect("a receiver"))
                .collect()
        });

        let all: Vec<&String> = took.iter().flatten().collect();
        let once: BTreeSet<&String> = all.iter().copied().collect();
        assert_eq!(all.len(), FILES, "every file taken, none twice");
        assert_eq!(once.len(), FILES);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_file_still_growing_is_not_taken_until_it_stops() {
        let dir = scratch("file-growing");
        let transport = on(&dir, 0);
        let mut producer = File::create(dir.join("order.edi")).expect("a producer");
        producer.write_all(b"ISA*00*").expect("writing");
        producer.flush().expect("flushed");

        assert!(transport.receive().expect("listing").is_empty());
        producer.write_all(b"GS*PO*").expect("writing more");
        producer.flush().expect("flushed");
        assert!(
            transport.receive().expect("listing").is_empty(),
            "it grew since the last listing, so it is not finished"
        );
        drop(producer);

        let mut arrived = transport.receive().expect("listing");
        assert_eq!(
            arrived.len(),
            1,
            "unchanged across two listings, it is taken"
        );
        let taken = arrived.remove(0).taken().expect("taken");
        assert_eq!(taken.bytes, b"ISA*00*GS*PO*", "taken whole");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_refused_file_is_returned_and_not_received_again_until_written_again() {
        let dir = scratch("file-refused");
        let transport = on(&dir, 0);
        transport.send("order.edi", b"ISA*00*").expect("sending");
        let mut arrived = settled(&transport);
        arrived
            .remove(0)
            .refused(Refusal::Unidentified)
            .expect("refused");
        assert_eq!(
            names(&dir),
            BTreeSet::from(["order.edi".to_string()]),
            "a refused file is the only copy, and is returned under its name"
        );
        assert_eq!(fs::read(dir.join("order.edi")).expect("kept"), b"ISA*00*");
        for _ in 0..3 {
            assert!(transport.receive().expect("receiving").is_empty());
        }
        let other = settled(&on(&dir, 1));
        assert_eq!(other.len(), 1, "another node finds it, not lost");
        drop(other);

        transport
            .send("order.edi", b"ISA*00*ZZ*")
            .expect("written again");
        assert_eq!(settled(&transport).len(), 1, "written again, it is new");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_failed_file_is_returned_and_an_accepted_one_is_deleted() {
        let dir = scratch("file-verdict");
        let transport = on(&dir, 0);
        transport.send("order.edi", b"ISA*00*").expect("sending");

        let mut arrived = settled(&transport);
        assert!(arrived[0].defers());
        arrived.remove(0).failed().expect("failed");
        assert_eq!(
            names(&dir),
            BTreeSet::from(["order.edi".to_string()]),
            "a failed file is returned"
        );

        let again = settled(&transport);
        assert_eq!(again.len(), 1, "the failed file is received again");
        let taken = again
            .into_iter()
            .next()
            .expect("one")
            .taken()
            .expect("taken");
        assert_eq!(taken.bytes, b"ISA*00*");
        assert!(names(&dir).is_empty(), "an accepted file is deleted");
        assert!(transport.receive().expect("receiving").is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    /// The bytes of the one arrival two listings of `transport` take.
    fn the_one(transport: &FileTransport) -> Vec<u8> {
        let mut arrived = settled(transport);
        assert_eq!(arrived.len(), 1);
        arrived.remove(0).taken().expect("taken").bytes
    }

    #[test]
    fn a_return_kept_from_its_name_goes_back_once_the_newer_file_is_consumed() {
        let dir = scratch("file-held-accepted");
        let transport = on(&dir, 0);
        transport.send("order.edi", b"old").expect("sending");
        let mut arrived = settled(&transport);
        transport
            .send("order.edi", b"new")
            .expect("a newer file, same name");

        let told = arrived.remove(0).failed().expect_err("not returned now");
        assert!(told.retryable, "{}", told.message);
        let held = transport.held();
        assert_eq!(held.len(), 1, "visible as held: {held:?}");
        assert!(held[0].why.contains("dropped since"), "{}", held[0].why);
        assert!(
            names(&dir).contains(
                held[0]
                    .claimed
                    .file_name()
                    .and_then(|n| n.to_str())
                    .expect("a name")
            )
        );

        assert_eq!(the_one(&transport), b"new", "the newer file is received");
        assert_eq!(
            names(&dir),
            BTreeSet::from(["order.edi".to_string()]),
            "its name freed, the older goes back at once, no restart"
        );
        assert!(transport.held().is_empty());
        assert_eq!(the_one(&transport), b"old", "and is received again");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_refused_return_kept_from_its_name_goes_back_refused_once_it_is_free() {
        let dir = scratch("file-held-refused");
        let transport = on(&dir, 0);
        transport.send("order.edi", b"old").expect("sending");
        let mut arrived = settled(&transport);
        transport
            .send("order.edi", b"new")
            .expect("a newer file, same name");
        let refusal = arrived.remove(0).refused(Refusal::Unidentified);
        assert!(refusal.is_err_and(|told| told.retryable));
        assert!(transport.held().iter().all(|file| file.refused));

        // Freed by somebody else: the next receive returns it.
        fs::remove_file(dir.join("order.edi")).expect("the newer file taken away");
        assert!(transport.receive().expect("receiving").is_empty());
        assert!(transport.held().is_empty());
        assert_eq!(fs::read(dir.join("order.edi")).expect("returned"), b"old");
        for _ in 0..3 {
            assert!(
                transport.receive().expect("receiving").is_empty(),
                "returned, it is remembered as refused"
            );
        }
        assert_eq!(
            the_one(&on(&dir, 1)),
            b"old",
            "another node finds it, not lost"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_starting_node_holds_a_claim_whose_name_is_taken_until_it_is_free() {
        let dir = scratch("file-held-start");
        let sender = FileTransport::new(&dir);
        sender.send("order.edi", b"old").expect("sending");
        std::mem::forget(settled(&on(&dir, 0)));
        sender
            .send("order.edi", b"new")
            .expect("a newer file, same name");

        let restarted = on(&dir, 0);
        assert_eq!(restarted.held().len(), 1, "held, and visible as held");
        assert_eq!(names(&dir).len(), 2);
        assert_eq!(the_one(&restarted), b"new");
        assert!(
            restarted.held().is_empty(),
            "returned once the newer was consumed"
        );
        assert_eq!(the_one(&restarted), b"old");
        assert!(names(&dir).is_empty());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_starting_node_returns_the_claims_its_name_left_and_no_others() {
        let dir = scratch("file-dead-owner");
        let sender = FileTransport::new(&dir);
        sender.send("mine.edi", b"ISA*00*").expect("sending");
        let mut held = settled(&on(&dir, 0));
        sender.send("theirs.edi", b"ISA*01*").expect("sending");
        held.extend(settled(&on(&dir, 1)));
        assert_eq!(held.len(), 2, "each node claims what it found");
        // Both die holding their claims: nothing is told, nothing returned.
        std::mem::forget(held);
        let left = names(&dir);
        assert!(
            left.iter().all(|name| name.contains(".xmip-claim.")),
            "{left:?}"
        );

        let restarted = on(&dir, 0);
        let after = names(&dir);
        assert_eq!(after.len(), 2, "{after:?}");
        let returned: Vec<_> = after
            .iter()
            .filter(|name| !name.contains(".xmip-claim."))
            .collect();
        assert_eq!(returned, ["mine.edi"], "its own claim is returned");
        let theirs_claimed = after.iter().find(|name| name.contains(".xmip-claim."));
        assert!(
            theirs_claimed.is_some_and(|name| name.starts_with("theirs.edi")),
            "another node's claim is its own: {after:?}"
        );
        assert_eq!(
            fs::read(dir.join(returned[0])).expect("returned whole"),
            b"ISA*00*"
        );
        assert_eq!(settled(&restarted).len(), 1, "and received again");
        fs::remove_dir_all(&dir).ok();
    }
}
