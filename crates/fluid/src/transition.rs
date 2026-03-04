use crate::events::OrderStatus;
use pebble_trading::{Fill, MarketId, OrderId, OrderSide};

/// Computed result of `place_order` — describes all mutations but does not apply them.
#[derive(Clone, Debug)]
pub struct PlaceOrderTransition {
    pub market_id: MarketId,
    pub book_transition: BookTransition,
    pub risk_transition: RiskTransition,
    pub fills: Vec<Fill>,
    pub taker_status: OrderStatus,
    pub taker_remaining: i64,
    pub taker_locked: i64,
    pub available_minor_after: i64,
    pub maker_updates: Vec<MakerUpdate>,
    pub next_fill_sequence: i64,
    pub next_event_sequence: i64,
}

/// Mutations to apply to the in-memory order book.
#[derive(Clone, Debug, Default)]
pub struct BookTransition {
    /// Partially filled makers: (order_id, new_remaining, new_locked).
    pub maker_remaining_updates: Vec<(OrderId, i64, i64)>,
    /// Fully filled makers to remove from the book.
    pub maker_removals: Vec<OrderId>,
    /// If the taker order rests (limit, not fully filled), insert this entry.
    pub taker_insertion: Option<PendingRestingEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRestingEntry {
    pub order_id: OrderId,
    pub account_id: String,
    pub outcome: String,
    pub side: OrderSide,
    pub price_ticks: i64,
    pub remaining_minor: i64,
    pub locked_minor: i64,
    pub submitted_at_micros: i64,
}

/// Mutations to apply to the in-memory risk state.
#[derive(Clone, Debug, Default)]
pub struct RiskTransition {
    pub deltas: Vec<AccountRiskDelta>,
    /// If set, advance the account's nonce to this value.
    pub nonce_advance: Option<(String, i64)>,
}

/// Per-account risk delta from a single order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskDelta {
    pub account_id: String,
    pub delta_pending_minor: i64,
    pub locked_delta_minor: i64,
}

/// Update to a maker order after partial/full fill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MakerUpdate {
    pub order_id: OrderId,
    pub new_remaining_minor: i64,
}

/// Computed result of `cancel_order` — describes mutations but does not apply them.
#[derive(Clone, Debug)]
pub struct CancelOrderTransition {
    pub market_id: MarketId,
    pub order_id: OrderId,
    pub account_id: String,
    pub released_locked_minor: i64,
}
