//! Claimed files waiting for their dropped name, each with why.
//!
//! A claimed file goes back under the name it was dropped with — on
//! `Refused`, on `Failed`, let go without a verdict, released, or found
//! claimed by its own node as it starts — and never over a file dropped
//! there since ([`give_back`]). Where one was, the claimed file cannot go
//! back yet, and nothing consumed it: what arrived is kept until the receive
//! cycle's verdict, and a refusal or a failure keeps it still (ADR-0013,
//! runtime-model section 5). Until 2026-10-05 it stayed claimed, skipped by
//! every receive as a claimed name is, until its node started again — even
//! long after the newer file was consumed.
//!
//! So the Location keeps it here, by its claimed name, with why, and tries
//! again as soon as the name may be free: right after it consumes a file
//! (`Accepted`, which frees the name it was dropped with), and at every
//! receive, before it lists (the name freed by anything else). Returned, it
//! is forgotten here and is an arrival again — remembered as refused first,
//! where its verdict was a refusal. Gone from under its claimed name, it is
//! forgotten. Meanwhile it is a held file anyone can see
//! ([`crate::FileTransport::held`]), and the telling that could not return
//! it says so.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use transport::TransportError;
use transport::error::Result;

use crate::claim::give_back;
use crate::{Refused, stamp};

/// A claimed file that could not go back to its drop directory yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeldFile {
    /// Where it lies: its claimed name.
    pub claimed: PathBuf,
    /// Why it lies there and not under the name it was dropped with.
    pub why: String,
    /// Whether its verdict was a refusal: returned, it is remembered as
    /// refused, as it would have been had it gone back at once.
    pub refused: bool,
}

/// The claimed files of one Location waiting for their dropped name.
/// Cloned, it is the same memory: a transport and the acknowledgements it
/// hands out share it.
#[derive(Clone, Default)]
pub(crate) struct Waiting {
    held: Arc<Mutex<BTreeMap<PathBuf, HeldFile>>>,
}

impl Waiting {
    /// The claimed file at `claimed` returned under its dropped name, and
    /// remembered in `refused` where `refusal` says its verdict was one.
    /// Where it cannot go back, it is kept here with why, and tried again
    /// as soon as its name may be free.
    ///
    /// # Errors
    /// Where it could not go back now: why, and that it is held. Retryable,
    /// since it is returned without anyone asking again.
    pub(crate) fn give_back(&self, claimed: &Path, refusal: Option<&Refused>) -> Result<()> {
        match give_back(claimed) {
            Ok(dropped) => {
                remember(refusal, dropped);
                Ok(())
            }
            Err(failed) if !claimed.exists() => Err(failed),
            Err(failed) => {
                let why = held_because(&failed, claimed);
                lock(&self.held).insert(
                    claimed.to_path_buf(),
                    HeldFile {
                        claimed: claimed.to_path_buf(),
                        why: why.clone(),
                        refused: refusal.is_some(),
                    },
                );
                Err(TransportError::retryable(why))
            }
        }
    }

    /// Every held file whose dropped name is free returned under it, each
    /// refused one remembered in `refused`; every one gone from under its
    /// claimed name forgotten. Nothing to do costs one lock.
    pub(crate) fn settle(&self, refused: &Refused) {
        let mut held = lock(&self.held);
        held.retain(|claimed, file| {
            if !claimed.exists() {
                return false;
            }
            match give_back(claimed) {
                Ok(dropped) => {
                    remember(file.refused.then_some(refused), dropped);
                    false
                }
                Err(still) => {
                    file.why = held_because(&still, claimed);
                    true
                }
            }
        });
    }

    /// What is held now, by claimed name.
    pub(crate) fn held(&self) -> Vec<HeldFile> {
        lock(&self.held).values().cloned().collect()
    }
}

/// Why the claimed file at `claimed` is held: `failed`, and what becomes
/// of it.
fn held_because(failed: &TransportError, claimed: &Path) -> String {
    format!(
        "{}; held as {} and returned once its name is free",
        failed.message,
        claimed.display()
    )
}

/// `dropped` remembered as refused, as it lies now, where `refusal` is.
fn remember(refusal: Option<&Refused>, dropped: PathBuf) {
    if let Some(refused) = refusal
        && let Some(stamp) = stamp(&dropped)
    {
        refused.remember(dropped, stamp);
    }
}

fn lock(held: &Mutex<BTreeMap<PathBuf, HeldFile>>) -> MutexGuard<'_, BTreeMap<PathBuf, HeldFile>> {
    held.lock().unwrap_or_else(PoisonError::into_inner)
}
