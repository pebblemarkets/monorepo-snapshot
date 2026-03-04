use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FluidError {
    InsufficientBalance {
        available: i64,
        required: i64,
    },
    AccountNotFound(String),
    AccountNotActive(String),
    MarketNotFound(String),
    MarketNotOpen(String),
    OrderNotFound(String),
    Unauthorized,
    NonceMismatch {
        expected: i64,
        got: i64,
    },
    NoncePending {
        account_id: String,
        pending_nonce: i64,
    },
    MatchFailed(String),
    InstrumentMismatch,
    Overflow(String),
    EngineUnhealthy(String),
    Wal(String),
    Replication(String),
    ReplayDivergence(String),
}

impl fmt::Display for FluidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientBalance {
                available,
                required,
            } => write!(
                f,
                "insufficient balance: available={available}, required={required}"
            ),
            Self::AccountNotFound(id) => write!(f, "account not found: {id}"),
            Self::AccountNotActive(id) => write!(f, "account not active: {id}"),
            Self::MarketNotFound(id) => write!(f, "market not found: {id}"),
            Self::MarketNotOpen(id) => write!(f, "market not open: {id}"),
            Self::OrderNotFound(id) => write!(f, "order not found: {id}"),
            Self::Unauthorized => write!(f, "unauthorized"),
            Self::NonceMismatch { expected, got } => {
                write!(f, "nonce mismatch: expected={expected}, got={got}")
            }
            Self::MatchFailed(msg) => write!(f, "match failed: {msg}"),
            Self::NoncePending {
                account_id,
                pending_nonce,
            } => write!(
                f,
                "nonce {pending_nonce} is already in-flight for account {account_id}"
            ),
            Self::InstrumentMismatch => write!(f, "account instrument does not match market"),
            Self::Overflow(msg) => write!(f, "arithmetic overflow: {msg}"),
            Self::EngineUnhealthy(msg) => write!(f, "engine unhealthy: {msg}"),
            Self::Wal(msg) => write!(f, "wal error: {msg}"),
            Self::Replication(msg) => write!(f, "replication error: {msg}"),
            Self::ReplayDivergence(msg) => {
                write!(f, "replay divergence: {msg}")
            }
        }
    }
}

impl std::error::Error for FluidError {}
