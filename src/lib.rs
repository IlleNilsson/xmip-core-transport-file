#![forbid(unsafe_code)]

//! Streams that arrive as files in a directory.
//!
//! The polled case: nothing is pushed to Xmip, Xmip goes and looks. Identity is
//! therefore *inferred* from the Receive Location rather than passed by a caller
//! — ADR-0019 clause 8.
//!
//! **A file is taken once it is finished, and by one node.** The claim is
//! the transport capability's, written once for the file family
//! (`transport::claim`; ADR-0024, amendments 2026-09-26 and 2026-10-13): a
//! listing takes a file only where its length and modification time are
//! what the listing before found — the stability check — so a file a
//! producer is still writing is left until it stops growing; it is then
//! claimed by taking its turn and renaming it to a name recording the node
//! and the time, and whichever node's rename succeeds has it. The node is
//! the one the runtime names as it builds the transport
//! ([`Configured::on_node`]), and a node that starts returns to the drop
//! directory whatever its name still holds there, since nothing holds it
//! now, and removes what its sends left half sent (`sending.rs`; ADR-0024,
//! amendment 2026-10-19). How a file system takes each step is
//! `directory.rs`.
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
//! the next receive.

mod directory;
#[cfg(windows)]
mod holding;
mod loopback;
mod sending;

use std::fs::File;
use std::path::{Path, PathBuf};

use context::property::FILE_NAME;
use transport::Arrived;
use transport::Configured;
use transport::Directions;
use transport::NodeLocation;
use transport::Transport;
use transport::TransportError;
use transport::body::opened;
use transport::claim::{Claimant, Claiming, Held, ResourceClaim, Site, Sited};
use transport::error::{Result, classify};
use xcore::settings::{Applies, Read, Settings};

pub use crate::directory::{Directory, Stamp};

pub struct FileTransport {
    directory: Directory,
    /// The claims made here, the stability check's memory, the refused
    /// files and those waiting to go back; its claimant names the files
    /// its sends write beside their targets.
    claiming: Claiming<Stamp>,
    /// Whether it was built for a Send Location, whose address names the
    /// directory it writes its files in, not a drop directory.
    sends: bool,
}

impl FileTransport {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            directory: Directory::at(root),
            claiming: Claiming::new(Claimant::process()),
            sends: false,
        }
    }

    /// The file a send writes in the directory: the name the Message
    /// carries in its context (`context::property::FILE_NAME`), where the
    /// runtime sent it — the Message's — or `target`, where a test or a
    /// tool sent bytes it holds and named the file itself.
    ///
    /// # Errors
    /// A Message carrying no file name, or a name that is not one file's in
    /// this directory — empty, a path, `.` or `..` — permanently: asking
    /// again names it no differently.
    fn sent_to(&self, target: &str, outgoing: &transport::Outgoing<'_>) -> Result<PathBuf> {
        let name = match outgoing.context() {
            Some(context) => context
                .get(FILE_NAME)
                .and_then(xcore::ScalarValue::text)
                .map(std::borrow::Cow::into_owned)
                .ok_or_else(|| {
                    TransportError::permanent(format!(
                        "the Message carries no {FILE_NAME} to name its file by in '{}'",
                        self.directory.root.display()
                    ))
                })?,
            None => target.to_string(),
        };
        let plain = !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains(['/', '\\', ':'])
            && Path::new(&name)
                .file_name()
                .is_some_and(|file| file == name.as_str());
        if !plain {
            return Err(TransportError::permanent(format!(
                "'{name}' names no file of its own in '{}'",
                self.directory.root.display()
            )));
        }
        Ok(self.directory.path(&name))
    }

    /// The claimed files this Location could not return yet, since a file
    /// was dropped under the same name meanwhile: each by its claimed name,
    /// and why. Each goes back as soon as its name is free.
    #[must_use]
    pub fn held(&self) -> Vec<Held> {
        self.claiming.held()
    }

    /// One claimed file: read as the runtime asks, deleted on `Accepted`,
    /// returned and remembered as refused on `Refused`, returned on
    /// `Failed` or when let go without a verdict.
    fn arrived(&self, dropped: &str, claimed: String) -> Arrived {
        // `file://` and the dropped path as `net::uri` writes it:
        // `file:///C:/in/a.edi`.
        let origin = format!(
            "file://{}",
            net::uri::path_of(&self.directory.path(dropped))
        );
        let read = self.directory.path(&claimed);
        // Its owner where the system says, and the name it was dropped
        // under, which the Message Context keeps for a send to write it by.
        let mut owned = owner(&read);
        owned.push((FILE_NAME.to_string(), dropped.to_string()));
        let acknowledgement = self
            .claiming
            .acknowledgement(self.directory.clone(), claimed, true);
        // Opened on its first read, so a drop directory of many files holds
        // one handle per file being read, not one per file listed.
        let body =
            opened(move || File::open(&read).map_err(|e| classify("opening a dropped file", &e)));
        // Found waiting in a folder this Location watches (ADR-0019 clause 8).
        Arrived::new(origin, body, acknowledgement)
            .detected()
            .observing_all(owned)
    }
}

/// Who owns the file at `path` and what it permits, as the system says
/// without unsafe code: on Unix its user and group ids and its permission
/// bits; nothing on Windows, whose owner is a security descriptor only an
/// unsafe call reads.
#[cfg(unix)]
fn owner(path: &Path) -> Vec<(String, String)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).map_or_else(
        |_| Vec::new(),
        |metadata| {
            transport::arrival_identity::file_owner(metadata.uid(), metadata.gid(), metadata.mode())
        },
    )
}

/// Nothing: see the Unix `owner`.
#[cfg(not(unix))]
fn owner(_: &Path) -> Vec<(String, String)> {
    Vec::new()
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

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Polled("a receive lists the drop directory and returns")
    }

    /// The drop directory, whose file system's change notification tells
    /// of a file dropped in it; nothing for a Send Location's directory.
    fn notice(&self) -> Option<transport::Notice> {
        (!self.sends).then(|| transport::Notice::Directory(self.directory.root.clone()))
    }

    fn receive_up_to(&self, most: usize) -> Result<Vec<Arrived>> {
        Ok(self
            .claiming
            .take(&self.directory, most)?
            .into_iter()
            .map(|taken| self.arrived(&taken.dropped, taken.claimed))
            .collect())
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Streamed("the file is written as the body is read")
    }

    /// The body as a file in the directory the address names, under the
    /// name the Message was received under ([`FileTransport::sent_to`]),
    /// written beside it as it is read — a piece at a time, never whole in
    /// memory — and renamed into place once whole, so no receiver finds it
    /// half written (`sending.rs`). A file already of that name is
    /// replaced: two Messages received under one name leave the last one's
    /// file (ADR-0024, amendment 2026-10-20).
    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        let path = self.sent_to(target, &outgoing)?;
        sending::send(&path, self.claiming.claimant(), &outgoing)
    }

    fn holds_back(&self) -> Option<&'static str> {
        Some("the file is written beside its target and renamed into place once kept")
    }

    /// A file's own claim: the turn and the rename (ADR-0024, amendments
    /// 2026-09-26 and 2026-10-13).
    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(self)
    }
}

/// The claim [`FileTransport::receive`] takes, for a caller holding one
/// file by its address, a path or its name in the directory.
impl Sited for FileTransport {
    type Stamp = Stamp;

    fn claiming(&self) -> &Claiming<Stamp> {
        &self.claiming
    }

    fn with_site<T>(&self, act: &mut dyn FnMut(Site<'_, Stamp>) -> Result<T>) -> Result<T> {
        act(&self.directory)
    }
}

impl Configured for FileTransport {
    /// The address is the directory: a Receive Location's drop directory, a
    /// Send Location's directory it writes each Message's file in. Nothing
    /// else is read.
    const SETTINGS: &'static Settings = &Settings::none(env!("CARGO_PKG_NAME"));

    fn configured(address: &str, _settings: &Read) -> Result<Self> {
        Ok(Self::new(address))
    }

    /// Whether the Location sends: its address is then the directory it
    /// writes in.
    fn sided(self, side: Applies) -> Self {
        Self {
            sends: side == Applies::Send,
            ..self
        }
    }

    /// Claims made, and sent files named, in `node`'s name from here on;
    /// what that name left before, recovered: the node is starting, so
    /// whatever it held or was writing before, nothing holds now (ADR-0024,
    /// amendments 2026-09-26 and 2026-10-19). A Receive Location returns
    /// every claim the name still holds in its drop directory — one whose
    /// dropped name a newer file has taken is held until the name is free
    /// — and removes its turns and the sent files its sends left
    /// unfinished there. A Send Location claims nothing, and removes the
    /// sent files its node's sends left unfinished in the directory it
    /// writes in. Another node's file is never touched, nor one a send of
    /// this process is still writing.
    fn on_node(mut self, node: &NodeLocation) -> Result<Self> {
        self.claiming = Claiming::new(Claimant::of(node));
        if self.sends {
            let claimant = self.claiming.claimant();
            claimant.remove_unfinished(&self.directory, self.directory.names()?)?;
        } else {
            self.claiming.recover(&self.directory)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::property::FILE_MODE;
    use std::collections::BTreeSet;
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    use transport::Refusal;
    use transport::claim::{Artifact, Claimant};
    use transport::loopback::Loopback;
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

    /// What a finished file's listings take: the first, where the system
    /// says no writer holds it (Windows); else the second, which finds it
    /// unchanged since the first.
    fn settled(transport: &FileTransport) -> Vec<Arrived> {
        let first = transport.receive().expect("listing");
        if cfg!(windows) {
            return first;
        }
        assert!(first.is_empty(), "nothing is taken at its first listing");
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
        assert_eq!(FileTransport::SETTINGS.settings, []);
        let transport = FileTransport::open("in", Applies::Receive, &[]).expect("configured");
        assert_eq!(transport.directory.root, PathBuf::from("in"));
        let unknown = [("color".to_string(), Given::Text("lime".to_string()))];
        let refused = FileTransport::open("in", Applies::Receive, &unknown)
            .err()
            .expect("refused");
        assert!(refused.message.contains("\"color\""), "{}", refused.message);
    }

    #[test]
    fn file_transport_declares_both_directions_and_claims_its_files() {
        let transport = FileTransport::new(std::env::temp_dir());

        assert!(transport.directions().receives());
        assert!(transport.directions().sends());
        assert_eq!(transport.name(), "file");
        assert!(transport.claims().is_some(), "files are artifacts");
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
        assert_eq!(arrived[0].arriving(), xcore::Arriving::Detected);
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
    fn a_file_still_being_sent_is_never_received() {
        let dir = scratch("file-sending");
        let transport = FileTransport::new(&dir);
        let writing = Claimant::of(&node(1)).sending("order.edi");
        let sending = dir.join(writing.name());
        fs::write(&sending, b"ISA*").expect("a send half way");

        assert!(settled(&transport).is_empty(), "not an arrival");
        let artifact = Artifact::new(sending.to_string_lossy().into_owned());
        assert!(!transport.is_available(&artifact).expect("asked"));
        assert_eq!(fs::read(&sending).expect("left alone"), b"ISA*");

        transport.send("order.edi", b"ISA*00*").expect("sending");
        assert_eq!(the_one(&transport), b"ISA*00*");
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
    fn a_receive_claims_no_more_than_it_was_given_room_for_and_leaves_the_rest() {
        let dir = scratch("file-room");
        let sender = FileTransport::new(&dir);
        for file in 0..10 {
            sender
                .send(&format!("order-{file}.edi"), b"ISA")
                .expect("sending");
        }
        let receiver = on(&dir, 0);
        let first = receiver.receive_up_to(3).expect("found");
        if cfg!(windows) {
            // Taken at once, no writer holding them, and no more than room
            // for; let go without a verdict, they go back to be taken below.
            assert_eq!(first.len(), 3, "as many as there was room for");
        } else {
            assert!(first.is_empty(), "found first");
        }
        drop(first);
        let mut took = 0;
        while took < 10 {
            let arrived = receiver.receive_up_to(3).expect("receiving");
            assert!(
                (1..=3).contains(&arrived.len()),
                "{} at once",
                arrived.len()
            );
            let claimed = names(&dir)
                .iter()
                .filter(|name| transport::claim::is_claimed(name))
                .count();
            assert_eq!(claimed, arrived.len(), "the rest left as dropped");
            took += arrived.len();
            for one in arrived {
                one.taken().expect("accepted");
            }
        }
        assert!(names(&dir).is_empty(), "every file taken in its turn");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[cfg(windows)]
    fn a_file_a_writer_holds_is_never_taken_and_one_let_go_is_taken_at_once() {
        let dir = scratch("file-held");
        let transport = on(&dir, 0);
        let mut producer = File::create(dir.join("order.edi")).expect("a producer");
        producer.write_all(b"ISA*00*").expect("writing");
        producer.flush().expect("flushed");
        // Unchanged across listings, and still held: a producer paused.
        for _ in 0..3 {
            assert!(transport.receive().expect("listing").is_empty(), "held");
        }
        drop(producer);
        fs::write(dir.join("whole.edi"), b"GS*PO*").expect("written whole");
        let arrived = transport.receive().expect("listing");
        assert_eq!(arrived.len(), 2, "both taken by the first listing after");
        for one in arrived {
            one.taken().expect("accepted");
        }
        fs::remove_dir_all(&dir).ok();
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
        assert!(names(&dir).contains(&held[0].claimed));

        assert_eq!(the_one(&transport), b"new", "the newer file is received");
        assert_eq!(
            names(&dir),
            BTreeSet::from(["order.edi".to_string()]),
            "its name freed, the older goes back at once, no restart"
        );
        assert_eq!(transport.held(), [] as [Held; 0]);
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
        assert_eq!(transport.held(), [] as [Held; 0]);
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

    /// A file `place`'s node left half sent to `target` in `dir`: a send
    /// its process did not finish.
    fn left_half_sent(dir: &Path, place: usize, target: &str) -> String {
        let name = Claimant::of(&node(place))
            .sending(target)
            .name()
            .to_string();
        fs::write(dir.join(&name), b"ISA*").expect("a send half way");
        name
    }

    #[test]
    fn a_starting_node_removes_the_sends_it_left_in_its_drop_directory_and_no_others() {
        let dir = scratch("file-left-receive");
        let mine = left_half_sent(&dir, 0, "order.edi");
        let theirs = left_half_sent(&dir, 1, "order.edi");
        FileTransport::new(&dir)
            .send("dropped.edi", b"ISA*00*")
            .expect("sending");

        let restarted = on(&dir, 0);

        let after = names(&dir);
        assert!(!after.contains(&mine), "its own removed: {after:?}");
        assert!(after.contains(&theirs), "another node's kept: {after:?}");
        let mut arrived = settled(&restarted);
        assert_eq!(arrived.len(), 1, "a half sent file is never an arrival");
        assert_eq!(arrived.remove(0).taken().expect("taken").bytes, b"ISA*00*");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_starting_send_location_removes_what_its_node_left_in_its_directory() {
        let dir = scratch("file-left-send");
        let target = dir.join("billing.edi");
        fs::write(&target, b"sent before").expect("a file sent before");
        let mine = left_half_sent(&dir, 0, "billing.edi");
        let theirs = left_half_sent(&dir, 1, "billing.edi");
        let address = dir.to_str().expect("a UTF-8 path");

        let sending = FileTransport::open(address, Applies::Send, &[])
            .and_then(|transport| transport.on_node(&node(0)))
            .expect("a node with a file sent before starts");

        let after = names(&dir);
        assert!(!after.contains(&mine), "its own removed: {after:?}");
        assert!(after.contains(&theirs), "another node's kept: {after:?}");
        sending.send("billing.edi", b"ISA*00*").expect("sending");
        assert_eq!(fs::read(&target).expect("sent"), b"ISA*00*");
        assert_eq!(names(&dir).len(), 2, "{:?}", names(&dir));
        fs::remove_dir_all(&dir).ok();
    }

    /// A Message's context, received as the file `name`.
    fn received_as(name: &str) -> context::MessageContext {
        context::MessageContext::new()
            .with_value(FILE_NAME, xcore::ScalarValue::Text(name.to_string()))
    }

    #[test]
    fn a_message_is_written_in_the_directory_under_the_name_it_was_received_under() {
        let dir = scratch("file-send-named");
        let address = dir.join("out");
        let address = address.to_str().expect("a UTF-8 path");
        let sending = FileTransport::open(address, Applies::Send, &[])
            .and_then(|transport| transport.on_node(&node(0)))
            .expect("built on its node");
        let context = received_as("order-1001.edi");

        sending
            .send_from(
                address,
                transport::Outgoing::held(b"ISA").carrying(&context),
            )
            .expect("sent");

        let written = fs::read(dir.join("out").join("order-1001.edi")).expect("written");
        assert_eq!(written, b"ISA");
        assert_eq!(names(&dir.join("out")).len(), 1);
        // Relative, as a configuration writes it: the file in the
        // directory, never in the directory inside itself.
        let relative = FileTransport::open("out", Applies::Send, &[]).expect("configured");
        let held = transport::Outgoing::held(b"ISA").carrying(&context);
        assert_eq!(
            relative.sent_to("out", &held).expect("named"),
            Path::new("out").join("order-1001.edi")
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_message_with_no_file_name_or_a_path_for_one_is_refused_in_words() {
        let dir = scratch("file-send-unnamed");
        let sending = FileTransport::new(&dir);
        let unnamed = context::MessageContext::new();
        let outgoing = transport::Outgoing::held(b"ISA").carrying(&unnamed);

        let refused = sending.send_from("ignored", outgoing).expect_err("no name");
        assert!(!refused.retryable);
        assert!(refused.message.contains(FILE_NAME), "{}", refused.message);
        for name in ["../escaped.edi", "sub/order.edi", "..", ""] {
            let context = received_as(name);
            let outgoing = transport::Outgoing::held(b"ISA").carrying(&context);
            let refused = sending.send_from("ignored", outgoing).expect_err(name);
            assert!(
                refused.message.contains("names no file"),
                "{}",
                refused.message
            );
        }
        assert!(names(&dir).is_empty(), "{:?}", names(&dir));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_dropped_file_says_the_name_it_was_dropped_under() {
        let dir = scratch("file-dropped-name");
        let transport = FileTransport::new(&dir);
        transport.send("order-7.edi", b"ISA").expect("sending");

        let mut arrived = settled(&transport);
        let taken = arrived.remove(0).taken().expect("taken");
        assert_eq!(taken.observation(FILE_NAME), Some("order-7.edi"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_round_says_who_owns_the_file_where_the_system_says() {
        let dir = scratch("file-round-identity");
        let taken = FileTransport::loopback(&dir)
            .round(b"who sent this")
            .expect("a round");
        assert!(taken.origin_uri.starts_with("file:///"), "{taken:?}");
        // The round itself holds the arrival to what the transport declares.
        assert_eq!(
            taken.observation(FILE_MODE).is_some(),
            cfg!(unix),
            "{taken:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }
}
