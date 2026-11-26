//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the directory: a
/// Receive Location's drop directory, a Send Location's directory it writes
/// each Message's file in. A Send Location's `name` names the file of a
/// Message carrying no `file.name`.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[Setting {
        name: "name",
        kind: Kind::Text,
        presence: Presence::Optional,
        meaning: "The file name a Message carrying no file.name is written under, \
                  {message} in it the Message's identifier; the identifier alone when \
                  left out.",
        applies: Applies::Send,
    }],
};

/// `file://`, and a path entered with no scheme at all (RFC 8089).
pub const SCHEMES: &[&str] = &["file"];
