use crate::domain::{Fill, IncomingOrder, OrderId, OrderSide, OrderType, RestingOrder};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchError {
    InvalidIncoming(String),
    InvalidResting(String),
    SideMismatch {
        order_id: String,
        expected: OrderSide,
        got: OrderSide,
    },
    OutcomeMismatch {
        order_id: String,
        expected: String,
        got: String,
    },
}

impl std::fmt::Display for MatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIncoming(msg) => write!(f, "invalid incoming order: {msg}"),
            Self::InvalidResting(msg) => write!(f, "invalid resting order: {msg}"),
            Self::SideMismatch {
                order_id,
                expected,
                got,
            } => write!(
                f,
                "resting order {order_id} side mismatch: expected {expected:?}, got {got:?}"
            ),
            Self::OutcomeMismatch {
                order_id,
                expected,
                got,
            } => write!(
                f,
                "resting order {order_id} outcome mismatch: expected {expected}, got {got}"
            ),
        }
    }
}

impl std::error::Error for MatchError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MakerUpdate {
    pub order_id: OrderId,
    pub new_remaining_minor: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchResult {
    pub fills: Vec<Fill>,
    pub maker_updates: Vec<MakerUpdate>,
    pub incoming_remaining_minor: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct OrderBookView {
    pub bids: Vec<RestingOrder>,
    pub asks: Vec<RestingOrder>,
}

impl OrderBookView {
    pub fn normalized(mut self) -> Self {
        self.bids.sort_by(|a, b| {
            b.price_ticks
                .cmp(&a.price_ticks)
                .then(a.submitted_at_micros.cmp(&b.submitted_at_micros))
                .then(a.order_id.0.cmp(&b.order_id.0))
        });
        self.asks.sort_by(|a, b| {
            a.price_ticks
                .cmp(&b.price_ticks)
                .then(a.submitted_at_micros.cmp(&b.submitted_at_micros))
                .then(a.order_id.0.cmp(&b.order_id.0))
        });
        self
    }
}

pub fn match_order(
    book: OrderBookView,
    incoming: &IncomingOrder,
    engine_version: &str,
    first_sequence: i64,
) -> Result<MatchResult, MatchError> {
    let normalized_book = book.normalized();
    match_order_with_book(normalized_book, incoming, engine_version, first_sequence)
}

pub fn match_order_presorted(
    book: OrderBookView,
    incoming: &IncomingOrder,
    engine_version: &str,
    first_sequence: i64,
) -> Result<MatchResult, MatchError> {
    match_order_with_book(book, incoming, engine_version, first_sequence)
}

fn match_order_with_book(
    book: OrderBookView,
    incoming: &IncomingOrder,
    engine_version: &str,
    first_sequence: i64,
) -> Result<MatchResult, MatchError> {
    incoming
        .validate()
        .map_err(|e| MatchError::InvalidIncoming(e.to_string()))?;
    if engine_version.trim().is_empty() {
        return Err(MatchError::InvalidIncoming(
            "engine_version must be non-empty".to_string(),
        ));
    }
    if first_sequence < 0 {
        return Err(MatchError::InvalidIncoming(
            "first_sequence must be >= 0".to_string(),
        ));
    }

    let resting_side = opposite(incoming.side);
    let resting_orders = match incoming.side {
        OrderSide::Buy => &book.asks,
        OrderSide::Sell => &book.bids,
    };

    let mut incoming_remaining_minor = incoming.quantity_minor;
    let mut fills = Vec::new();
    let mut maker_updates = Vec::new();

    for resting in resting_orders {
        resting
            .validate()
            .map_err(|e| MatchError::InvalidResting(e.to_string()))?;
        if resting.side != resting_side {
            return Err(MatchError::SideMismatch {
                order_id: resting.order_id.0.clone(),
                expected: resting_side,
                got: resting.side,
            });
        }
        if resting.outcome != incoming.outcome {
            return Err(MatchError::OutcomeMismatch {
                order_id: resting.order_id.0.clone(),
                expected: incoming.outcome.clone(),
                got: resting.outcome.clone(),
            });
        }

        let is_crossing = match incoming.order_type {
            OrderType::Limit => crosses(incoming.side, incoming.price_ticks, resting.price_ticks),
            OrderType::Market => true,
        };
        if !is_crossing {
            break;
        }

        let matched_qty = incoming_remaining_minor.min(resting.remaining_minor);
        if matched_qty <= 0 {
            continue;
        }

        let sequence = first_sequence + i64::try_from(fills.len()).unwrap_or(i64::MAX);
        // Fill sequence is per-market, so the fill identifier must include market_id
        // to remain globally unique across markets.
        let fill_id = format!(
            "fill:{engine_version}:{}:{sequence:020}",
            incoming.market_id.0
        );

        fills.push(Fill {
            fill_id,
            maker_order_id: resting.order_id.clone(),
            taker_order_id: incoming.order_id.clone(),
            maker_account_id: resting.account_id.clone(),
            taker_account_id: incoming.account_id.clone(),
            outcome: incoming.outcome.clone(),
            price_ticks: resting.price_ticks,
            quantity_minor: matched_qty,
            sequence,
            engine_version: engine_version.to_string(),
            canonical_price_ticks: resting.price_ticks,
            maker_outcome: resting.outcome.clone(),
            maker_side: resting.side,
            maker_price_ticks: resting.price_ticks,
            taker_outcome: incoming.outcome.clone(),
            taker_side: incoming.side,
            taker_price_ticks: resting.price_ticks,
        });

        maker_updates.push(MakerUpdate {
            order_id: resting.order_id.clone(),
            new_remaining_minor: resting.remaining_minor - matched_qty,
        });

        incoming_remaining_minor -= matched_qty;
        if incoming_remaining_minor == 0 {
            break;
        }
    }

    Ok(MatchResult {
        fills,
        maker_updates,
        incoming_remaining_minor,
    })
}

pub fn match_limit_order(
    book: OrderBookView,
    incoming: &IncomingOrder,
    engine_version: &str,
    first_sequence: i64,
) -> Result<MatchResult, MatchError> {
    match_order(book, incoming, engine_version, first_sequence)
}

fn opposite(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn crosses(side: OrderSide, taker_price_ticks: i64, maker_price_ticks: i64) -> bool {
    match side {
        OrderSide::Buy => taker_price_ticks >= maker_price_ticks,
        OrderSide::Sell => taker_price_ticks <= maker_price_ticks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{IncomingOrder, MarketId, OrderNonce, OrderType, TimeInForce};

    fn incoming_buy_with_market(
        market_id: &str,
        quantity_minor: i64,
        price_ticks: i64,
    ) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId("taker-1".to_string()),
            account_id: "acc-taker".to_string(),
            market_id: MarketId(market_id.to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            nonce: OrderNonce(7),
            price_ticks,
            quantity_minor,
            submitted_at_micros: 1_700_000_000_000_000,
        }
    }

    fn incoming_buy(quantity_minor: i64, price_ticks: i64) -> IncomingOrder {
        incoming_buy_with_market("market-1", quantity_minor, price_ticks)
    }

    fn incoming_market_buy(quantity_minor: i64) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId("taker-market".to_string()),
            account_id: "acc-taker".to_string(),
            market_id: MarketId("market-1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            tif: TimeInForce::Ioc,
            nonce: OrderNonce(8),
            price_ticks: 0,
            quantity_minor,
            submitted_at_micros: 1_700_000_000_000_001,
        }
    }

    fn ask(
        order_id: &str,
        price_ticks: i64,
        remaining_minor: i64,
        submitted_at_micros: i64,
    ) -> RestingOrder {
        RestingOrder {
            order_id: OrderId(order_id.to_string()),
            account_id: format!("maker-{order_id}"),
            side: OrderSide::Sell,
            outcome: "YES".to_string(),
            price_ticks,
            remaining_minor,
            submitted_at_micros,
        }
    }

    #[test]
    fn matches_buy_against_best_price_then_time() {
        let book = OrderBookView {
            bids: vec![],
            asks: vec![
                ask("ask-3", 104, 4, 3),
                ask("ask-2", 101, 2, 2),
                ask("ask-1", 101, 2, 1),
            ],
        };
        let incoming = incoming_buy(3, 102);

        let res = match_limit_order(book, &incoming, "m3-test-v1", 10).unwrap();

        assert_eq!(res.fills.len(), 2);
        assert_eq!(res.fills[0].maker_order_id.0, "ask-1");
        assert_eq!(res.fills[0].price_ticks, 101);
        assert_eq!(res.fills[0].quantity_minor, 2);

        assert_eq!(res.fills[1].maker_order_id.0, "ask-2");
        assert_eq!(res.fills[1].price_ticks, 101);
        assert_eq!(res.fills[1].quantity_minor, 1);

        assert_eq!(res.incoming_remaining_minor, 0);
        assert_eq!(
            res.maker_updates,
            vec![
                MakerUpdate {
                    order_id: OrderId("ask-1".to_string()),
                    new_remaining_minor: 0
                },
                MakerUpdate {
                    order_id: OrderId("ask-2".to_string()),
                    new_remaining_minor: 1
                }
            ]
        );
    }

    #[test]
    fn deterministic_across_input_permutations() {
        let asks = vec![
            ask("ask-a", 100, 1, 3),
            ask("ask-b", 100, 1, 2),
            ask("ask-c", 99, 1, 1),
        ];
        let book_a = OrderBookView {
            bids: vec![],
            asks: asks.clone(),
        };
        let mut asks_reversed = asks;
        asks_reversed.reverse();
        let book_b = OrderBookView {
            bids: vec![],
            asks: asks_reversed,
        };

        let incoming = incoming_buy(2, 105);

        let res_a = match_limit_order(book_a, &incoming, "m3-test-v1", 100).unwrap();
        let res_b = match_limit_order(book_b, &incoming, "m3-test-v1", 100).unwrap();

        assert_eq!(res_a, res_b);
        assert_eq!(
            res_a
                .fills
                .iter()
                .map(|x| x.maker_order_id.0.clone())
                .collect::<Vec<_>>(),
            vec!["ask-c".to_string(), "ask-b".to_string()]
        );
    }

    #[test]
    fn no_cross_returns_no_fills() {
        let book = OrderBookView {
            bids: vec![],
            asks: vec![ask("ask-1", 110, 5, 1)],
        };
        let incoming = incoming_buy(3, 100);

        let res = match_limit_order(book, &incoming, "m3-test-v1", 0).unwrap();
        assert!(res.fills.is_empty());
        assert!(res.maker_updates.is_empty());
        assert_eq!(res.incoming_remaining_minor, 3);
    }

    #[test]
    fn market_order_matches_even_when_limit_cross_would_fail() {
        let book = OrderBookView {
            bids: vec![],
            asks: vec![ask("ask-1", 110, 2, 1)],
        };
        let incoming = incoming_market_buy(1);

        let res = match_order(book, &incoming, "m3-test-v1", 0).unwrap();
        assert_eq!(res.fills.len(), 1);
        assert_eq!(res.fills[0].maker_order_id.0, "ask-1");
        assert_eq!(res.fills[0].price_ticks, 110);
        assert_eq!(res.incoming_remaining_minor, 0);
    }

    #[test]
    fn market_order_still_respects_best_price_then_time() {
        let book = OrderBookView {
            bids: vec![],
            asks: vec![
                ask("ask-3", 104, 4, 3),
                ask("ask-2", 101, 2, 2),
                ask("ask-1", 101, 2, 1),
            ],
        };
        let incoming = incoming_market_buy(3);

        let res = match_order(book, &incoming, "m3-test-v1", 0).unwrap();
        assert_eq!(res.fills.len(), 2);
        assert_eq!(res.fills[0].maker_order_id.0, "ask-1");
        assert_eq!(res.fills[1].maker_order_id.0, "ask-2");
        assert_eq!(res.incoming_remaining_minor, 0);
    }

    #[test]
    fn fill_ids_are_unique_across_markets_for_same_sequence() {
        let book = OrderBookView {
            bids: vec![],
            asks: vec![ask("ask-1", 100, 1, 1)],
        };

        let incoming_a = incoming_buy_with_market("market-a", 1, 100);
        let incoming_b = incoming_buy_with_market("market-b", 1, 100);

        let res_a = match_limit_order(book.clone(), &incoming_a, "m3-test-v1", 0).unwrap();
        let res_b = match_limit_order(book, &incoming_b, "m3-test-v1", 0).unwrap();

        assert_eq!(res_a.fills.len(), 1);
        assert_eq!(res_b.fills.len(), 1);
        assert_ne!(res_a.fills[0].fill_id, res_b.fills[0].fill_id);
    }
}
