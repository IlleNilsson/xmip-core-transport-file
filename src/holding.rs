//! On Windows a dropped file's turn is a handle on the file itself: opened
//! to write and sharing reading and deleting, never writing, so no other
//! claimant opens it the same way while this one holds it, and no producer
//! still writing it lets it be opened at all (ADR-0024, amendment
//! 2026-10-21). One open is the whole of what a turn file took — created,
//! written, closed and removed again — and of the open that asked whether a
//! writer still held the file: four changes to the directory and three
//! opens, each one a busy disk and a scanning virus guard make slower,
//! became one open (open problem 33, 2026-10-21). Nothing is written
//! through the handle; a rename by name still moves the file, since a
//! rename opens it to delete it, which the handle shares; and the turn ends
//! with the handle, so a node that dies leaves none behind.
//!
//! Where the open is refused for anything but another's handle — a file
//! this account may not write — the claim takes its turn as a file, as
//! every other file system does (`directory.rs`).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::windows::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use crate::directory::Stamp;

/// `FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE`: what the handle
/// may do, which is what makes it exclusive among claimants, and read its
/// stamp.
const ACCESS: u32 = 0x2 | 0x80 | 0x0010_0000;

/// `FILE_SHARE_READ | FILE_SHARE_DELETE`: a reader and the rename are let
/// in, a second writer — another claimant, or a producer — is not.
const SHARE: u32 = 0x1 | 0x4;

/// `ERROR_SHARING_VIOLATION`.
const SHARING_VIOLATION: i32 = 32;

/// The dropped files one Location holds open as their turns, by name.
#[derive(Clone, Debug, Default)]
pub(crate) struct Holding(Arc<Mutex<HashMap<String, File>>>);

impl Holding {
    /// The turn of `name` in `root` taken, by opening it: `Some(true)`
    /// held now, `Some(false)` while another handle writes it — another
    /// claimant's or a producer's — and `None` where the open was refused
    /// for anything else. One held already is held.
    pub(crate) fn take(&self, root: &Path, name: &str) -> Option<bool> {
        let mut held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if held.contains_key(name) {
            return Some(true);
        }
        match OpenOptions::new()
            .access_mode(ACCESS)
            .share_mode(SHARE)
            .open(root.join(name))
        {
            Ok(file) => {
                held.insert(name.to_string(), file);
                Some(true)
            }
            Err(e) if e.raw_os_error() == Some(SHARING_VIOLATION) => Some(false),
            Err(_) => None,
        }
    }

    /// The stamp of `name` through its handle, where it is held and the
    /// handle answers: `None` otherwise, for the path to answer.
    pub(crate) fn stamp(&self, name: &str) -> Option<Stamp> {
        let held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let metadata = held.get(name)?.metadata().ok()?;
        Some((metadata.len(), metadata.modified().ok()))
    }

    /// The turn of `name` let go: whether it was held.
    pub(crate) fn let_go(&self, name: &str) -> bool {
        let mut held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        held.remove(name).is_some()
    }
}
