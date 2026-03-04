use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    EmptyField(&'static str),
    NonPositive(&'static str),
    Negative(&'static str),
    Invalid(&'static str),
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyField(name) => write!(f, "{name} must be non-empty"),
            Self::NonPositive(name) => write!(f, "{name} must be > 0"),
            Self::Negative(name) => write!(f, "{name} must be >= 0"),
            Self::Invalid(name) => write!(f, "{name} is invalid"),
        }
    }
}

impl std::error::Error for DomainError {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketId(pub String);

impl MarketId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyField("market_id"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderId(pub String);

impl OrderId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyField("order_id"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InternalOrderId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InternalAccountId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderNonce(pub i64);

impl OrderNonce {
    pub fn new(value: i64) -> Result<Self, DomainError> {
        if value < 0 {
            return Err(DomainError::Negative("nonce"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instrument {
    pub instrument_admin: String,
    pub instrument_id: String,
}

impl Instrument {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.instrument_admin.trim().is_empty() {
            return Err(DomainError::EmptyField("instrument_admin"));
        }
        if self.instrument_id.trim().is_empty() {
            return Err(DomainError::EmptyField("instrument_id"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Market {
    pub market_id: MarketId,
    pub instrument: Instrument,
    pub outcomes: Vec<String>,
    pub tick_size_minor: i64,
}

impl Market {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.instrument.validate()?;
        if self.outcomes.len() < 2 {
            return Err(DomainError::NonPositive("outcomes"));
        }
        if self.outcomes.iter().any(|x| x.trim().is_empty()) {
            return Err(DomainError::EmptyField("outcome"));
        }
        if self.tick_size_minor <= 0 {
            return Err(DomainError::NonPositive("tick_size_minor"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    Ioc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncomingOrder {
    pub order_id: OrderId,
    pub account_id: String,
    pub market_id: MarketId,
    pub outcome: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub nonce: OrderNonce,
    pub price_ticks: i64,
    pub quantity_minor: i64,
    pub submitted_at_micros: i64,
}

impl IncomingOrder {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.account_id.trim().is_empty() {
            return Err(DomainError::EmptyField("account_id"));
        }
        if self.outcome.trim().is_empty() {
            return Err(DomainError::EmptyField("outcome"));
        }
        match self.order_type {
            OrderType::Limit => {
                if self.price_ticks <= 0 {
                    return Err(DomainError::NonPositive("price_ticks"));
                }
                if self.tif != TimeInForce::Gtc {
                    return Err(DomainError::Invalid("tif must be Gtc for limit orders"));
                }
            }
            OrderType::Market => {
                if self.price_ticks < 0 {
                    return Err(DomainError::Negative("price_ticks"));
                }
                if self.tif != TimeInForce::Ioc {
                    return Err(DomainError::Invalid("tif must be Ioc for market orders"));
                }
            }
        }
        if self.quantity_minor <= 0 {
            return Err(DomainError::NonPositive("quantity_minor"));
        }
        if self.submitted_at_micros < 0 {
            return Err(DomainError::Negative("submitted_at_micros"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub account_id: String,
    pub side: OrderSide,
    pub outcome: String,
    pub price_ticks: i64,
    pub remaining_minor: i64,
    pub submitted_at_micros: i64,
}

impl RestingOrder {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.account_id.trim().is_empty() {
            return Err(DomainError::EmptyField("account_id"));
        }
        if self.outcome.trim().is_empty() {
            return Err(DomainError::EmptyField("outcome"));
        }
        if self.price_ticks <= 0 {
            return Err(DomainError::NonPositive("price_ticks"));
        }
        if self.remaining_minor <= 0 {
            return Err(DomainError::NonPositive("remaining_minor"));
        }
        if self.submitted_at_micros < 0 {
            return Err(DomainError::Negative("submitted_at_micros"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub fill_id: String,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_account_id: String,
    pub taker_account_id: String,
    pub outcome: String,
    pub price_ticks: i64,
    pub quantity_minor: i64,
    pub sequence: i64,
    pub engine_version: String,
    pub canonical_price_ticks: i64,
    pub maker_outcome: String,
    pub maker_side: OrderSide,
    pub maker_price_ticks: i64,
    pub taker_outcome: String,
    pub taker_side: OrderSide,
    pub taker_price_ticks: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTradeDelta {
    pub account_id: String,
    pub delta_minor: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedOpenOrder {
    pub account_id: String,
    pub locked_minor: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRiskState {
    pub cleared_cash_minor: i64,
    pub delta_pending_trades_minor: i64,
    pub locked_open_orders_minor: i64,
}

impl AccountRiskState {
    pub fn available_minor(self) -> i64 {
        self.cleared_cash_minor + self.delta_pending_trades_minor - self.locked_open_orders_minor
    }
}
