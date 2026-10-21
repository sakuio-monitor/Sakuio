use serde::{Deserialize, Serialize};

/// Kind of byte relay (terminal keystrokes, file stream, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayKind {
    Shell,
    FileRead,
    FileWrite,
}
