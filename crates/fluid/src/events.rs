use pebble_trading::{Fill, MarketId, OrderId};

/// Status of an order after processing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
}

impl OrderStatus {
    /// Convert to the string representation used in the database.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Open => "Open",
            Self::PartiallyFilled => "PartiallyFilled",
            Self::Filled => "Filled",
            Self::Cancelled => "Cancelled",
        }
    }
}

/// Events emitted by the engine. Informational only in Phase 1.
#[derive(Clone, Debug)]
pub enum EngineEvent {
    OrderProcessed {
        market_id: MarketId,
        order_id: OrderId,
        account_id: String,
        status: OrderStatus,
        fills: Vec<Fill>,
        remaining_minor: i64,
    },
    OrderCancelled {
        market_id: MarketId,
        order_id: OrderId,
        account_id: String,
        released_locked_minor: i64,
    },
}
