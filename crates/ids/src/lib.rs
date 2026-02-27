use std::fmt::{Display, Formatter};

const MAX_IDENTIFIER_LEN: usize = 128;
const MAX_BATCH_HASH_LEN: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    Empty { label: &'static str },
    TooLong { label: &'static str, max_len: usize },
    InvalidChar { label: &'static str, ch: char },
    InvalidEpoch { value: i64 },
    InvalidPositiveEpoch { value: i64 },
}

impl Display for IdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty { label } => write!(f, "{label} must be non-empty"),
            Self::TooLong { label, max_len } => {
                write!(f, "{label} must be <= {max_len} characters")
            }
            Self::InvalidChar { label, ch } => write!(
                f,
                "{label} contains invalid character {ch:?}; allowed: ASCII alphanumeric, '-', '_', '.', ':'"
            ),
            Self::InvalidEpoch { value } => write!(f, "epoch must be >= 0 (got {value})"),
            Self::InvalidPositiveEpoch { value } => {
                write!(f, "epoch must be > 0 (got {value})")
            }
        }
    }
}

impl std::error::Error for IdError {}

pub fn parse_account_id(raw: &str) -> Result<String, IdError> {
    parse_identifier(raw, "account_id", MAX_IDENTIFIER_LEN)
}

pub fn parse_market_id(raw: &str) -> Result<String, IdError> {
    parse_identifier(raw, "market_id", MAX_IDENTIFIER_LEN)
}

pub fn parse_deposit_id(raw: &str) -> Result<String, IdError> {
    parse_identifier(raw, "deposit_id", MAX_IDENTIFIER_LEN)
}

pub fn parse_withdrawal_id(raw: &str) -> Result<String, IdError> {
    parse_identifier(raw, "withdrawal_id", MAX_IDENTIFIER_LEN)
}

pub fn parse_batch_hash(raw: &str) -> Result<String, IdError> {
    parse_batch_hash_internal(raw, "batch_hash")
}

pub fn validate_epoch(epoch: i64) -> Result<i64, IdError> {
    if epoch < 0 {
        return Err(IdError::InvalidEpoch { value: epoch });
    }

    Ok(epoch)
}

pub fn validate_positive_epoch(epoch: i64) -> Result<i64, IdError> {
    if epoch <= 0 {
        return Err(IdError::InvalidPositiveEpoch { value: epoch });
    }

    Ok(epoch)
}

fn parse_identifier(raw: &str, label: &'static str, max_len: usize) -> Result<String, IdError> {
    parse_with_charset(raw, label, max_len, false)
}

fn parse_batch_hash_internal(raw: &str, label: &'static str) -> Result<String, IdError> {
    parse_with_charset(raw, label, MAX_BATCH_HASH_LEN, true)
}

fn parse_with_charset(
    raw: &str,
    label: &'static str,
    max_len: usize,
    allow_equals: bool,
) -> Result<String, IdError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(IdError::Empty { label });
    }

    if value.len() > max_len {
        return Err(IdError::TooLong { label, max_len });
    }

    for ch in value.chars() {
        if !is_allowed_char(ch, allow_equals) {
            return Err(IdError::InvalidChar { label, ch });
        }
    }

    Ok(value.to_string())
}

fn is_allowed_char(ch: char, allow_equals: bool) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':') || (allow_equals && ch == '=')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_account_id_accepts_valid_ascii_identifier() {
        let got = parse_account_id("acc-001:a.b_c").unwrap();
        assert_eq!(got, "acc-001:a.b_c");
    }

    #[test]
    fn parse_account_id_rejects_invalid_characters() {
        let err = parse_account_id("acc/001").unwrap_err();
        assert!(matches!(
            err,
            IdError::InvalidChar {
                label: "account_id",
                ch: '/'
            }
        ));
    }

    #[test]
    fn parse_batch_hash_accepts_equals() {
        let got = parse_batch_hash("m3:ia:iid:7:ev1:count=2:abs=4").unwrap();
        assert_eq!(got, "m3:ia:iid:7:ev1:count=2:abs=4");
    }

    #[test]
    fn validate_epoch_guards_negative_values() {
        let err = validate_epoch(-1).unwrap_err();
        assert!(matches!(err, IdError::InvalidEpoch { value: -1 }));
    }

    #[test]
    fn validate_positive_epoch_guards_non_positive_values() {
        let err = validate_positive_epoch(0).unwrap_err();
        assert!(matches!(err, IdError::InvalidPositiveEpoch { value: 0 }));
    }
}
