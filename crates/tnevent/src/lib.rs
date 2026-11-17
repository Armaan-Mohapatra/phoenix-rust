//! Decoder, book verifier and case builder for the TradingNext collector's TNEVENT segments.
//!
//! Read-only and offline. Never opens `.open` segments, never follows links, never writes
//! beside the source. See `container` for the wire format and `book` for what "verified" means.
pub mod book;
pub mod cases;
pub mod container;
pub mod corpus;
pub mod event;
