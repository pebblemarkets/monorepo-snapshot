use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::ControlFlow;

use pebble_trading::{
    Fill, IncomingOrder, InternalAccountId, InternalOrderId, OrderBookView, OrderId, OrderSide,
    OrderType, RestingOrder,
};
use slab::Slab;

use crate::engine::BinaryConfig;
use crate::error::FluidError;
use crate::ids::StringInterner;
use crate::transition::BookTransition;

/// An order resting in the in-memory book.
/// Extends `RestingOrder` with `locked_minor` which the trading crate doesn't track.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestingEntry {
    pub order_id: InternalOrderId,
    pub account_id: InternalAccountId,
    pub outcome: String,
    pub side: OrderSide,
    pub price_ticks: i64,
    pub remaining_minor: i64,
    pub locked_minor: i64,
    pub submitted_at_micros: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Yes,
    No,
}

/// Resting order in the unified binary book (canonical YES-space).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnifiedEntry {
    pub order_id: InternalOrderId,
    pub account_id: InternalAccountId,
    pub original_outcome: Outcome,
    pub original_side: OrderSide,
    pub original_price_ticks: i64,
    pub canonical_side: OrderSide,
    pub canonical_price_ticks: i64,
    pub remaining_minor: i64,
    pub locked_minor: i64,
    pub submitted_at_micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchOutcome {
    pub fills: Vec<Fill>,
    pub maker_updates: Vec<MakerFillUpdate>,
    pub incoming_remaining_minor: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MakerFillUpdate {
    pub order_id: OrderId,
    pub new_remaining_minor: i64,
}

impl RestingEntry {
    fn validate(&self) -> Result<(), FluidError> {
        if self.outcome.trim().is_empty() {
            return Err(FluidError::MatchFailed(
                "invalid resting order: outcome cannot be empty".to_string(),
            ));
        }
        if self.price_ticks <= 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: price_ticks must be positive".to_string(),
            ));
        }
        if self.remaining_minor <= 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: remaining_minor must be positive".to_string(),
            ));
        }
        if self.submitted_at_micros < 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: submitted_at_micros must be non-negative".to_string(),
            ));
        }
        Ok(())
    }

    fn to_resting_order(
        &self,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<RestingOrder, FluidError> {
        let order_id = order_interner.to_external(self.order_id).ok_or_else(|| {
            FluidError::MatchFailed(format!(
                "missing external order mapping for internal id {}",
                self.order_id.0
            ))
        })?;
        let account_id = account_interner
            .to_external(self.account_id)
            .ok_or_else(|| {
                FluidError::MatchFailed(format!(
                    "missing external account mapping for internal id {}",
                    self.account_id.0
                ))
            })?;

        Ok(RestingOrder {
            order_id: OrderId(order_id.to_string()),
            account_id: account_id.to_string(),
            side: self.side,
            outcome: self.outcome.clone(),
            price_ticks: self.price_ticks,
            remaining_minor: self.remaining_minor,
            submitted_at_micros: self.submitted_at_micros,
        })
    }
}

impl UnifiedEntry {
    fn validate(&self) -> Result<(), FluidError> {
        if self.original_price_ticks <= 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: original_price_ticks must be positive".to_string(),
            ));
        }
        if self.canonical_price_ticks <= 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: canonical_price_ticks must be positive".to_string(),
            ));
        }
        if self.remaining_minor <= 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: remaining_minor must be positive".to_string(),
            ));
        }
        if self.submitted_at_micros < 0 {
            return Err(FluidError::MatchFailed(
                "invalid resting order: submitted_at_micros must be non-negative".to_string(),
            ));
        }
        Ok(())
    }

    fn to_resting_entry(&self, binary_config: &BinaryConfig) -> RestingEntry {
        RestingEntry {
            order_id: self.order_id,
            account_id: self.account_id,
            outcome: outcome_to_label(self.original_outcome, binary_config).to_string(),
            side: self.original_side,
            price_ticks: self.original_price_ticks,
            remaining_minor: self.remaining_minor,
            locked_minor: self.locked_minor,
            submitted_at_micros: self.submitted_at_micros,
        }
    }

    fn to_resting_order(
        &self,
        binary_config: &BinaryConfig,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<RestingOrder, FluidError> {
        self.to_resting_entry(binary_config)
            .to_resting_order(order_interner, account_interner)
    }
}

fn parse_outcome_label(outcome: &str, binary_config: &BinaryConfig) -> Result<Outcome, FluidError> {
    if outcome == binary_config.yes_outcome {
        Ok(Outcome::Yes)
    } else if outcome == binary_config.no_outcome {
        Ok(Outcome::No)
    } else {
        Err(FluidError::MatchFailed(format!(
            "outcome '{}' not in binary config",
            outcome
        )))
    }
}

fn outcome_to_label(outcome: Outcome, binary_config: &BinaryConfig) -> &str {
    match outcome {
        Outcome::Yes => &binary_config.yes_outcome,
        Outcome::No => &binary_config.no_outcome,
    }
}

fn flipped_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn canonical_side_for_outcome(outcome: Outcome, side: OrderSide) -> OrderSide {
    match outcome {
        Outcome::Yes => side,
        Outcome::No => flipped_side(side),
    }
}

fn normalize_to_yes_space(
    outcome: &str,
    side: OrderSide,
    price_ticks: i64,
    order_type: OrderType,
    binary_config: &BinaryConfig,
) -> Result<(Outcome, OrderSide, i64), FluidError> {
    if binary_config.max_price_ticks <= 1 {
        return Err(FluidError::MatchFailed(format!(
            "invalid binary max_price_ticks={} (must be > 1)",
            binary_config.max_price_ticks
        )));
    }

    let parsed_outcome = parse_outcome_label(outcome, binary_config)?;
    let canonical_side = canonical_side_for_outcome(parsed_outcome, side);
    match order_type {
        OrderType::Limit => {
            if price_ticks <= 0 || price_ticks >= binary_config.max_price_ticks {
                return Err(FluidError::MatchFailed(format!(
                    "binary limit price_ticks must satisfy 1 <= price_ticks < {}",
                    binary_config.max_price_ticks
                )));
            }

            let canonical_price_ticks = match parsed_outcome {
                Outcome::Yes => price_ticks,
                Outcome::No => binary_config
                    .max_price_ticks
                    .checked_sub(price_ticks)
                    .ok_or_else(|| FluidError::Overflow("complement price overflow".to_string()))?,
            };
            if canonical_price_ticks <= 0 {
                return Err(FluidError::MatchFailed(format!(
                    "canonical_price_ticks must be positive, got {}",
                    canonical_price_ticks
                )));
            }
            Ok((parsed_outcome, canonical_side, canonical_price_ticks))
        }
        OrderType::Market => Ok((parsed_outcome, canonical_side, 0)),
    }
}

fn execution_price_in_outcome_space(
    canonical_price_ticks: i64,
    outcome: Outcome,
    binary_config: &BinaryConfig,
) -> Result<i64, FluidError> {
    match outcome {
        Outcome::Yes => Ok(canonical_price_ticks),
        Outcome::No => binary_config
            .max_price_ticks
            .checked_sub(canonical_price_ticks)
            .ok_or_else(|| FluidError::Overflow("execution complement overflow".to_string())),
    }
}

/// Index entry tracking where an order lives in the multi-outcome book.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OrderLocation {
    outcome: String,
    side: OrderSide,
    price_ticks: i64,
    slot: usize,
}

/// Index entry tracking where an order lives in the binary canonical YES-space book.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BinaryOrderLocation {
    canonical_side: OrderSide,
    canonical_price_ticks: i64,
    slot: usize,
}

/// One side of a book (bids or asks).
#[derive(Clone, Debug, Default)]
struct HalfBook {
    /// Price level -> FIFO queue of orders at that level.
    levels: BTreeMap<i64, VecDeque<usize>>,
}

impl HalfBook {
    fn insert(&mut self, price_ticks: i64, slot: usize) {
        self.levels.entry(price_ticks).or_default().push_back(slot);
    }

    fn remove(&mut self, slot: usize, price_ticks: i64) -> bool {
        let Some(level) = self.levels.get_mut(&price_ticks) else {
            return false;
        };
        let Some(pos) = level.iter().position(|entry_slot| *entry_slot == slot) else {
            return false;
        };
        let _ = level.remove(pos);
        if level.is_empty() {
            self.levels.remove(&price_ticks);
        }
        true
    }

    fn order_count(&self) -> usize {
        self.levels.values().map(VecDeque::len).sum()
    }
}

fn walk_levels<B, F>(half: &HalfBook, ascending: bool, mut f: F) -> ControlFlow<B>
where
    F: FnMut(&i64, &VecDeque<usize>) -> ControlFlow<B>,
{
    if ascending {
        for entry in &half.levels {
            if let ControlFlow::Break(value) = f(entry.0, entry.1) {
                return ControlFlow::Break(value);
            }
        }
    } else {
        for entry in half.levels.iter().rev() {
            if let ControlFlow::Break(value) = f(entry.0, entry.1) {
                return ControlFlow::Break(value);
            }
        }
    }
    ControlFlow::Continue(())
}

/// Book for a single outcome (bids + asks).
#[derive(Clone, Debug, Default)]
struct OutcomeBook {
    bids: HalfBook,
    asks: HalfBook,
}

impl OutcomeBook {
    fn half_mut(&mut self, side: OrderSide) -> &mut HalfBook {
        match side {
            OrderSide::Buy => &mut self.bids,
            OrderSide::Sell => &mut self.asks,
        }
    }
}

/// Existing per-outcome market book implementation.
#[derive(Clone, Debug, Default)]
pub struct MultiOutcomeBook {
    arena: Slab<RestingEntry>,
    outcomes: HashMap<String, OutcomeBook>,
    index: HashMap<InternalOrderId, OrderLocation>,
}

impl MultiOutcomeBook {
    fn try_insert(&mut self, entry: RestingEntry) -> Result<(), FluidError> {
        entry.validate()?;
        let outcome = entry.outcome.clone();
        let side = entry.side;
        let price_ticks = entry.price_ticks;
        let order_id = entry.order_id;
        let slot = self.arena.insert(entry);
        let loc = OrderLocation {
            outcome: outcome.clone(),
            side,
            price_ticks,
            slot,
        };
        self.index.insert(order_id, loc);
        let ob = self.outcomes.entry(outcome).or_default();
        ob.half_mut(side).insert(price_ticks, slot);
        Ok(())
    }

    fn cancel(&mut self, order_id: InternalOrderId) -> Option<RestingEntry> {
        let loc = self.index.get(&order_id)?.clone();
        {
            let ob = self.outcomes.get_mut(&loc.outcome)?;
            if !ob.half_mut(loc.side).remove(loc.slot, loc.price_ticks) {
                return None;
            }
        }
        if !self.arena.contains(loc.slot) {
            return None;
        }
        let entry = self.arena.remove(loc.slot);
        let _ = self.index.remove(&order_id);
        Some(entry)
    }

    fn update_remaining_and_locked(
        &mut self,
        order_id: InternalOrderId,
        new_remaining: i64,
        new_locked: i64,
    ) -> Result<(), FluidError> {
        let loc = self
            .index
            .get(&order_id)
            .cloned()
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.to_string()))?;
        let entry = self
            .arena
            .get_mut(loc.slot)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.to_string()))?;
        if entry.order_id != order_id {
            return Err(FluidError::OrderNotFound(order_id.0.to_string()));
        }
        entry.remaining_minor = new_remaining;
        entry.locked_minor = new_locked;
        Ok(())
    }

    fn order_count(&self) -> usize {
        self.outcomes
            .values()
            .map(|ob| ob.bids.order_count() + ob.asks.order_count())
            .sum()
    }

    fn order_ids(&self) -> Vec<InternalOrderId> {
        self.index.keys().copied().collect()
    }

    fn get_entry(&self, order_id: InternalOrderId) -> Option<RestingEntry> {
        let loc = self.index.get(&order_id)?;
        let entry = self.arena.get(loc.slot)?;
        if entry.order_id == order_id {
            Some(entry.clone())
        } else {
            None
        }
    }

    fn view_for_match(
        &self,
        outcome: &str,
        incoming_side: OrderSide,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<OrderBookView, FluidError> {
        let Some(ob) = self.outcomes.get(outcome) else {
            return Ok(OrderBookView::default());
        };
        match incoming_side {
            OrderSide::Buy => {
                let mut asks = Vec::new();
                if let ControlFlow::Break(result) = walk_levels(&ob.asks, true, |_, level| {
                    for slot in level {
                        let Some(entry) = self.arena.get(*slot) else {
                            return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                                "book invariant violated while building asks view: missing slot {}",
                                slot
                            ))));
                        };
                        match entry.to_resting_order(order_interner, account_interner) {
                            Ok(resting) => asks.push(resting),
                            Err(error) => return ControlFlow::Break(Err(error)),
                        }
                    }
                    ControlFlow::Continue(())
                }) {
                    result?;
                }
                Ok(OrderBookView {
                    bids: Vec::new(),
                    asks,
                })
            }
            OrderSide::Sell => {
                let mut bids = Vec::new();
                if let ControlFlow::Break(result) = walk_levels(&ob.bids, false, |_, level| {
                    for slot in level {
                        let Some(entry) = self.arena.get(*slot) else {
                            return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                                "book invariant violated while building bids view: missing slot {}",
                                slot
                            ))));
                        };
                        match entry.to_resting_order(order_interner, account_interner) {
                            Ok(resting) => bids.push(resting),
                            Err(error) => return ControlFlow::Break(Err(error)),
                        }
                    }
                    ControlFlow::Continue(())
                }) {
                    result?;
                }
                Ok(OrderBookView {
                    bids,
                    asks: Vec::new(),
                })
            }
        }
    }

    fn match_order(
        &self,
        incoming: &IncomingOrder,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
        engine_version: &str,
        first_sequence: i64,
    ) -> Result<MatchOutcome, FluidError> {
        incoming
            .validate()
            .map_err(|e| FluidError::MatchFailed(format!("invalid incoming order: {e}")))?;
        if engine_version.trim().is_empty() {
            return Err(FluidError::MatchFailed(
                "engine_version must be non-empty".to_string(),
            ));
        }
        if first_sequence < 0 {
            return Err(FluidError::MatchFailed(
                "first_sequence must be >= 0".to_string(),
            ));
        }

        let mut fills = Vec::new();
        let mut maker_updates = Vec::new();
        let mut incoming_remaining_minor = incoming.quantity_minor;

        let Some(ob) = self.outcomes.get(&incoming.outcome) else {
            return Ok(MatchOutcome {
                fills,
                maker_updates,
                incoming_remaining_minor,
            });
        };

        let (contra, ascending) = match incoming.side {
            OrderSide::Buy => (&ob.asks, true),
            OrderSide::Sell => (&ob.bids, false),
        };
        let contra_side = flipped_side(incoming.side);

        if let ControlFlow::Break(result) = walk_levels(contra, ascending, |price_ticks, level| {
            let is_crossing = match incoming.order_type {
                OrderType::Limit => crosses(incoming.side, incoming.price_ticks, *price_ticks),
                OrderType::Market => true,
            };
            if !is_crossing {
                return ControlFlow::Break(Ok(()));
            }

            for slot in level {
                let Some(entry) = self.arena.get(*slot) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "book invariant violated: missing slot {}",
                        slot
                    ))));
                };

                if let Err(error) = entry.validate() {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "invalid resting order: {error}"
                    ))));
                }
                let Some(maker_order_id) = order_interner.to_external(entry.order_id) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "missing external order mapping for internal id {}",
                        entry.order_id.0
                    ))));
                };
                let Some(maker_account_id) = account_interner.to_external(entry.account_id) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "missing external account mapping for internal id {}",
                        entry.account_id.0
                    ))));
                };
                if entry.side != contra_side {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "resting order {} side mismatch: expected {contra_side:?}, got {:?}",
                        maker_order_id, entry.side
                    ))));
                }
                if entry.outcome != incoming.outcome {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "resting order {} outcome mismatch: expected {}, got {}",
                        maker_order_id, incoming.outcome, entry.outcome
                    ))));
                }

                let matched_qty = incoming_remaining_minor.min(entry.remaining_minor);
                if matched_qty <= 0 {
                    continue;
                }

                let fill_offset = match i64::try_from(fills.len()) {
                    Ok(value) => value,
                    Err(_) => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "fill sequence conversion".to_string(),
                        )));
                    }
                };
                let sequence = match first_sequence.checked_add(fill_offset) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "fill sequence overflow".to_string(),
                        )));
                    }
                };

                let fill_id = format!(
                    "fill:{engine_version}:{}:{sequence:020}",
                    incoming.market_id.0
                );
                fills.push(Fill {
                    fill_id,
                    maker_order_id: OrderId(maker_order_id.to_string()),
                    taker_order_id: incoming.order_id.clone(),
                    maker_account_id: maker_account_id.to_string(),
                    taker_account_id: incoming.account_id.clone(),
                    outcome: incoming.outcome.clone(),
                    price_ticks: *price_ticks,
                    quantity_minor: matched_qty,
                    sequence,
                    engine_version: engine_version.to_string(),
                    canonical_price_ticks: *price_ticks,
                    maker_outcome: entry.outcome.clone(),
                    maker_side: entry.side,
                    maker_price_ticks: *price_ticks,
                    taker_outcome: incoming.outcome.clone(),
                    taker_side: incoming.side,
                    taker_price_ticks: *price_ticks,
                });
                let new_remaining_minor = match entry.remaining_minor.checked_sub(matched_qty) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(format!(
                            "maker remaining underflow for {}",
                            maker_order_id
                        ))));
                    }
                };
                maker_updates.push(MakerFillUpdate {
                    order_id: OrderId(maker_order_id.to_string()),
                    new_remaining_minor,
                });

                incoming_remaining_minor = match incoming_remaining_minor.checked_sub(matched_qty) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "incoming remaining underflow".to_string(),
                        )));
                    }
                };

                if incoming_remaining_minor == 0 {
                    return ControlFlow::Break(Ok(()));
                }
            }

            ControlFlow::Continue(())
        }) {
            result?;
        }

        Ok(MatchOutcome {
            fills,
            maker_updates,
            incoming_remaining_minor,
        })
    }
}

/// Unified binary market book in canonical YES-space.
#[derive(Clone, Debug)]
pub struct BinaryBook {
    arena: Slab<UnifiedEntry>,
    yes_bids: HalfBook,
    yes_asks: HalfBook,
    index: HashMap<InternalOrderId, BinaryOrderLocation>,
    binary_config: BinaryConfig,
}

impl BinaryBook {
    fn new(binary_config: BinaryConfig) -> Self {
        Self {
            arena: Slab::new(),
            yes_bids: HalfBook::default(),
            yes_asks: HalfBook::default(),
            index: HashMap::new(),
            binary_config,
        }
    }

    fn try_insert(&mut self, entry: RestingEntry) -> Result<(), FluidError> {
        entry.validate()?;
        let (original_outcome, canonical_side, canonical_price_ticks) = normalize_to_yes_space(
            &entry.outcome,
            entry.side,
            entry.price_ticks,
            OrderType::Limit,
            &self.binary_config,
        )?;

        let unified = UnifiedEntry {
            order_id: entry.order_id,
            account_id: entry.account_id,
            original_outcome,
            original_side: entry.side,
            original_price_ticks: entry.price_ticks,
            canonical_side,
            canonical_price_ticks,
            remaining_minor: entry.remaining_minor,
            locked_minor: entry.locked_minor,
            submitted_at_micros: entry.submitted_at_micros,
        };
        unified.validate()?;

        let slot = self.arena.insert(unified);
        self.index.insert(
            entry.order_id,
            BinaryOrderLocation {
                canonical_side,
                canonical_price_ticks,
                slot,
            },
        );
        match canonical_side {
            OrderSide::Buy => self.yes_bids.insert(canonical_price_ticks, slot),
            OrderSide::Sell => self.yes_asks.insert(canonical_price_ticks, slot),
        }

        Ok(())
    }

    fn cancel(&mut self, order_id: InternalOrderId) -> Option<RestingEntry> {
        let loc = self.index.get(&order_id)?.clone();
        let removed = match loc.canonical_side {
            OrderSide::Buy => self.yes_bids.remove(loc.slot, loc.canonical_price_ticks),
            OrderSide::Sell => self.yes_asks.remove(loc.slot, loc.canonical_price_ticks),
        };
        if !removed || !self.arena.contains(loc.slot) {
            return None;
        }

        let entry = self.arena.remove(loc.slot);
        let _ = self.index.remove(&order_id);
        Some(entry.to_resting_entry(&self.binary_config))
    }

    fn update_remaining_and_locked(
        &mut self,
        order_id: InternalOrderId,
        new_remaining: i64,
        new_locked: i64,
    ) -> Result<(), FluidError> {
        let loc = self
            .index
            .get(&order_id)
            .cloned()
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.to_string()))?;
        let entry = self
            .arena
            .get_mut(loc.slot)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.to_string()))?;
        if entry.order_id != order_id {
            return Err(FluidError::OrderNotFound(order_id.0.to_string()));
        }
        entry.remaining_minor = new_remaining;
        entry.locked_minor = new_locked;
        Ok(())
    }

    fn order_count(&self) -> usize {
        self.yes_bids.order_count() + self.yes_asks.order_count()
    }

    fn order_ids(&self) -> Vec<InternalOrderId> {
        self.index.keys().copied().collect()
    }

    fn get_entry(&self, order_id: InternalOrderId) -> Option<RestingEntry> {
        let loc = self.index.get(&order_id)?;
        let entry = self.arena.get(loc.slot)?;
        if entry.order_id == order_id {
            Some(entry.to_resting_entry(&self.binary_config))
        } else {
            None
        }
    }

    fn view_for_match(
        &self,
        outcome: &str,
        incoming_side: OrderSide,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<OrderBookView, FluidError> {
        let parsed_outcome = parse_outcome_label(outcome, &self.binary_config)?;
        let canonical_side = canonical_side_for_outcome(parsed_outcome, incoming_side);
        let (contra, ascending) = match canonical_side {
            OrderSide::Buy => (&self.yes_asks, true),
            OrderSide::Sell => (&self.yes_bids, false),
        };

        let mut contra_orders = Vec::new();
        if let ControlFlow::Break(result) = walk_levels(contra, ascending, |_, level| {
            for slot in level {
                let Some(entry) = self.arena.get(*slot) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "book invariant violated while building binary view: missing slot {}",
                        slot
                    ))));
                };
                match entry.to_resting_order(&self.binary_config, order_interner, account_interner)
                {
                    Ok(resting) => contra_orders.push(resting),
                    Err(error) => return ControlFlow::Break(Err(error)),
                }
            }
            ControlFlow::Continue(())
        }) {
            result?;
        }

        match incoming_side {
            OrderSide::Buy => Ok(OrderBookView {
                bids: Vec::new(),
                asks: contra_orders,
            }),
            OrderSide::Sell => Ok(OrderBookView {
                bids: contra_orders,
                asks: Vec::new(),
            }),
        }
    }

    fn match_order(
        &self,
        incoming: &IncomingOrder,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
        engine_version: &str,
        first_sequence: i64,
    ) -> Result<MatchOutcome, FluidError> {
        incoming
            .validate()
            .map_err(|e| FluidError::MatchFailed(format!("invalid incoming order: {e}")))?;
        if engine_version.trim().is_empty() {
            return Err(FluidError::MatchFailed(
                "engine_version must be non-empty".to_string(),
            ));
        }
        if first_sequence < 0 {
            return Err(FluidError::MatchFailed(
                "first_sequence must be >= 0".to_string(),
            ));
        }

        let (incoming_outcome, incoming_canonical_side, incoming_canonical_price_ticks) =
            normalize_to_yes_space(
                &incoming.outcome,
                incoming.side,
                incoming.price_ticks,
                incoming.order_type,
                &self.binary_config,
            )?;
        let taker_outcome = outcome_to_label(incoming_outcome, &self.binary_config).to_string();

        let mut fills = Vec::new();
        let mut maker_updates = Vec::new();
        let mut incoming_remaining_minor = incoming.quantity_minor;

        let (contra, ascending) = match incoming_canonical_side {
            OrderSide::Buy => (&self.yes_asks, true),
            OrderSide::Sell => (&self.yes_bids, false),
        };
        let contra_side = flipped_side(incoming_canonical_side);

        if let ControlFlow::Break(result) = walk_levels(contra, ascending, |price_ticks, level| {
            let is_crossing = match incoming.order_type {
                OrderType::Limit => crosses(
                    incoming_canonical_side,
                    incoming_canonical_price_ticks,
                    *price_ticks,
                ),
                OrderType::Market => true,
            };
            if !is_crossing {
                return ControlFlow::Break(Ok(()));
            }

            for slot in level {
                let Some(entry) = self.arena.get(*slot) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "book invariant violated: missing slot {}",
                        slot
                    ))));
                };

                if let Err(error) = entry.validate() {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "invalid resting order: {error}"
                    ))));
                }
                let Some(maker_order_id) = order_interner.to_external(entry.order_id) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "missing external order mapping for internal id {}",
                        entry.order_id.0
                    ))));
                };
                let Some(maker_account_id) = account_interner.to_external(entry.account_id) else {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "missing external account mapping for internal id {}",
                        entry.account_id.0
                    ))));
                };
                if entry.canonical_side != contra_side {
                    return ControlFlow::Break(Err(FluidError::MatchFailed(format!(
                        "resting order {} canonical side mismatch: expected {contra_side:?}, got {:?}",
                        maker_order_id, entry.canonical_side
                    ))));
                }

                let matched_qty = incoming_remaining_minor.min(entry.remaining_minor);
                if matched_qty <= 0 {
                    continue;
                }

                let fill_offset = match i64::try_from(fills.len()) {
                    Ok(value) => value,
                    Err(_) => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "fill sequence conversion".to_string(),
                        )));
                    }
                };
                let sequence = match first_sequence.checked_add(fill_offset) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "fill sequence overflow".to_string(),
                        )));
                    }
                };

                let maker_price_ticks = match execution_price_in_outcome_space(
                    *price_ticks,
                    entry.original_outcome,
                    &self.binary_config,
                ) {
                    Ok(value) => value,
                    Err(error) => return ControlFlow::Break(Err(error)),
                };
                let taker_price_ticks = match execution_price_in_outcome_space(
                    *price_ticks,
                    incoming_outcome,
                    &self.binary_config,
                ) {
                    Ok(value) => value,
                    Err(error) => return ControlFlow::Break(Err(error)),
                };

                let fill_id = format!(
                    "fill:{engine_version}:{}:{sequence:020}",
                    incoming.market_id.0
                );
                fills.push(Fill {
                    fill_id,
                    maker_order_id: OrderId(maker_order_id.to_string()),
                    taker_order_id: incoming.order_id.clone(),
                    maker_account_id: maker_account_id.to_string(),
                    taker_account_id: incoming.account_id.clone(),
                    outcome: taker_outcome.clone(),
                    price_ticks: taker_price_ticks,
                    quantity_minor: matched_qty,
                    sequence,
                    engine_version: engine_version.to_string(),
                    canonical_price_ticks: *price_ticks,
                    maker_outcome: outcome_to_label(entry.original_outcome, &self.binary_config)
                        .to_string(),
                    maker_side: entry.original_side,
                    maker_price_ticks,
                    taker_outcome: taker_outcome.clone(),
                    taker_side: incoming.side,
                    taker_price_ticks,
                });
                let new_remaining_minor = match entry.remaining_minor.checked_sub(matched_qty) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(format!(
                            "maker remaining underflow for {}",
                            maker_order_id
                        ))));
                    }
                };
                maker_updates.push(MakerFillUpdate {
                    order_id: OrderId(maker_order_id.to_string()),
                    new_remaining_minor,
                });

                incoming_remaining_minor = match incoming_remaining_minor.checked_sub(matched_qty) {
                    Some(value) => value,
                    None => {
                        return ControlFlow::Break(Err(FluidError::Overflow(
                            "incoming remaining underflow".to_string(),
                        )));
                    }
                };

                if incoming_remaining_minor == 0 {
                    return ControlFlow::Break(Ok(()));
                }
            }

            ControlFlow::Continue(())
        }) {
            result?;
        }

        Ok(MatchOutcome {
            fills,
            maker_updates,
            incoming_remaining_minor,
        })
    }
}

/// Full market order book.
#[derive(Clone, Debug)]
pub enum MarketBook {
    Binary(BinaryBook),
    MultiOutcome(MultiOutcomeBook),
}

impl MarketBook {
    pub fn new_binary(binary_config: BinaryConfig) -> Self {
        Self::Binary(BinaryBook::new(binary_config))
    }

    pub fn new_multi_outcome() -> Self {
        Self::MultiOutcome(MultiOutcomeBook::default())
    }

    /// Insert a resting order into the book.
    ///
    /// Orders at the same price level are matched in FIFO (insertion) order.
    /// Bootstrap/resync callers must insert orders sorted by `(submitted_at, order_id)`
    /// within each price level to preserve price-time-id priority.
    pub fn insert(&mut self, entry: RestingEntry) {
        if let Err(error) = self.insert_checked(entry) {
            tracing::error!(error = ?error, "book insert rejected invalid resting entry");
        }
    }

    pub fn insert_checked(&mut self, entry: RestingEntry) -> Result<(), FluidError> {
        match self {
            Self::Binary(book) => book.try_insert(entry),
            Self::MultiOutcome(book) => book.try_insert(entry),
        }
    }

    /// Match incoming order directly against the book.
    /// Read-only (no mutation).
    pub fn match_order(
        &self,
        incoming: &IncomingOrder,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
        engine_version: &str,
        first_sequence: i64,
    ) -> Result<MatchOutcome, FluidError> {
        match self {
            Self::Binary(book) => book.match_order(
                incoming,
                order_interner,
                account_interner,
                engine_version,
                first_sequence,
            ),
            Self::MultiOutcome(book) => book.match_order(
                incoming,
                order_interner,
                account_interner,
                engine_version,
                first_sequence,
            ),
        }
    }

    /// Cancel an order, removing it from the book. Returns the entry if found.
    pub fn cancel(&mut self, order_id: InternalOrderId) -> Option<RestingEntry> {
        match self {
            Self::Binary(book) => book.cancel(order_id),
            Self::MultiOutcome(book) => book.cancel(order_id),
        }
    }

    /// Build an `OrderBookView` for matching an incoming order.
    /// The contra side is populated with orders sorted in price-time-id priority.
    pub fn view_for_match(
        &self,
        outcome: &str,
        incoming_side: OrderSide,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<OrderBookView, FluidError> {
        match self {
            Self::Binary(book) => {
                book.view_for_match(outcome, incoming_side, order_interner, account_interner)
            }
            Self::MultiOutcome(book) => {
                book.view_for_match(outcome, incoming_side, order_interner, account_interner)
            }
        }
    }

    /// Apply book mutations from a computed transition.
    pub fn apply_book_transition(
        &mut self,
        bt: &BookTransition,
        order_interner: &mut StringInterner<InternalOrderId>,
        account_interner: &mut StringInterner<InternalAccountId>,
    ) -> Result<(), FluidError> {
        for (order_id, new_remaining, new_locked) in &bt.maker_remaining_updates {
            let internal_order_id = order_interner
                .lookup(&order_id.0)
                .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;
            self.update_remaining_and_locked(internal_order_id, *new_remaining, *new_locked)?;
        }

        for order_id in &bt.maker_removals {
            let internal_order_id = order_interner
                .lookup(&order_id.0)
                .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;
            if self.cancel(internal_order_id).is_none() {
                return Err(FluidError::OrderNotFound(order_id.0.clone()));
            }
        }

        if let Some(ref entry) = bt.taker_insertion {
            self.insert_checked(RestingEntry {
                order_id: order_interner.intern(&entry.order_id.0),
                account_id: account_interner.intern(&entry.account_id),
                outcome: entry.outcome.clone(),
                side: entry.side,
                price_ticks: entry.price_ticks,
                remaining_minor: entry.remaining_minor,
                locked_minor: entry.locked_minor,
                submitted_at_micros: entry.submitted_at_micros,
            })?;
        }

        Ok(())
    }

    fn update_remaining_and_locked(
        &mut self,
        order_id: InternalOrderId,
        new_remaining: i64,
        new_locked: i64,
    ) -> Result<(), FluidError> {
        match self {
            Self::Binary(book) => {
                book.update_remaining_and_locked(order_id, new_remaining, new_locked)
            }
            Self::MultiOutcome(book) => {
                book.update_remaining_and_locked(order_id, new_remaining, new_locked)
            }
        }
    }

    /// Total number of resting orders.
    pub fn order_count(&self) -> usize {
        match self {
            Self::Binary(book) => book.order_count(),
            Self::MultiOutcome(book) => book.order_count(),
        }
    }

    pub fn order_ids(&self) -> Vec<InternalOrderId> {
        match self {
            Self::Binary(book) => book.order_ids(),
            Self::MultiOutcome(book) => book.order_ids(),
        }
    }

    /// Look up a resting entry by internal order ID.
    pub fn get_entry(&self, order_id: InternalOrderId) -> Option<RestingEntry> {
        match self {
            Self::Binary(book) => book.get_entry(order_id),
            Self::MultiOutcome(book) => book.get_entry(order_id),
        }
    }

    pub fn get_entry_by_external_order_id(
        &self,
        order_id: &OrderId,
        order_interner: &StringInterner<InternalOrderId>,
    ) -> Option<RestingEntry> {
        let internal_order_id = order_interner.lookup(&order_id.0)?;
        self.get_entry(internal_order_id)
    }
}

fn crosses(side: OrderSide, taker_price_ticks: i64, maker_price_ticks: i64) -> bool {
    match side {
        OrderSide::Buy => taker_price_ticks >= maker_price_ticks,
        OrderSide::Sell => taker_price_ticks <= maker_price_ticks,
    }
}
#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::too_many_arguments)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestIds {
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    }

    impl TestIds {
        fn order_id(&self, order_id: &str) -> InternalOrderId {
            self.order_interner.lookup(order_id).unwrap()
        }
    }

    fn make_entry(
        ids: &mut TestIds,
        id: &str,
        account: &str,
        outcome: &str,
        side: OrderSide,
        price: i64,
        remaining: i64,
        locked: i64,
        time: i64,
    ) -> RestingEntry {
        RestingEntry {
            order_id: ids.order_interner.intern(id),
            account_id: ids.account_interner.intern(account),
            outcome: outcome.to_string(),
            side,
            price_ticks: price,
            remaining_minor: remaining,
            locked_minor: locked,
            submitted_at_micros: time,
        }
    }

    fn match_order_for_test(
        book: &MarketBook,
        ids: &TestIds,
        incoming: &IncomingOrder,
        engine_version: &str,
        first_sequence: i64,
    ) -> Result<MatchOutcome, FluidError> {
        book.match_order(
            incoming,
            &ids.order_interner,
            &ids.account_interner,
            engine_version,
            first_sequence,
        )
    }

    fn view_for_match_for_test(
        book: &MarketBook,
        ids: &TestIds,
        outcome: &str,
        incoming_side: OrderSide,
    ) -> Result<OrderBookView, FluidError> {
        book.view_for_match(
            outcome,
            incoming_side,
            &ids.order_interner,
            &ids.account_interner,
        )
    }

    fn make_limit_incoming(
        order_id: &str,
        outcome: &str,
        side: OrderSide,
        price_ticks: i64,
        quantity_minor: i64,
    ) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId(order_id.to_string()),
            account_id: "taker".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: outcome.to_string(),
            side,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks,
            quantity_minor,
            submitted_at_micros: 1,
        }
    }

    fn binary_config() -> BinaryConfig {
        BinaryConfig {
            yes_outcome: "YES".to_string(),
            no_outcome: "NO".to_string(),
            max_price_ticks: 10_000,
        }
    }

    fn make_binary_book() -> MarketBook {
        MarketBook::new_binary(binary_config())
    }

    fn multi_outcome_book(book: &MarketBook) -> Option<&MultiOutcomeBook> {
        if let MarketBook::MultiOutcome(inner) = book {
            Some(inner)
        } else {
            None
        }
    }

    fn multi_outcome_book_mut(book: &mut MarketBook) -> Option<&mut MultiOutcomeBook> {
        if let MarketBook::MultiOutcome(inner) = book {
            Some(inner)
        } else {
            None
        }
    }

    #[test]
    fn test_match_rejects_empty_engine_version() {
        let ids = TestIds::default();
        let book = MarketBook::new_multi_outcome();
        let incoming = make_limit_incoming("o1", "YES", OrderSide::Buy, 50, 10);

        let result = match_order_for_test(&book, &ids, &incoming, "", 0);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message)) if message == "engine_version must be non-empty"
        ));
    }

    #[test]
    fn test_match_rejects_negative_first_sequence() {
        let ids = TestIds::default();
        let book = MarketBook::new_multi_outcome();
        let incoming = make_limit_incoming("o1", "YES", OrderSide::Buy, 50, 10);

        let result = match_order_for_test(&book, &ids, &incoming, "v1", -1);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message)) if message == "first_sequence must be >= 0"
        ));
    }

    #[test]
    fn test_apply_book_transition_missing_maker_update_returns_not_found() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let result = book.apply_book_transition(
            &BookTransition {
                maker_remaining_updates: vec![(OrderId("missing".to_string()), 1, 1)],
                maker_removals: vec![],
                taker_insertion: None,
            },
            &mut ids.order_interner,
            &mut ids.account_interner,
        );

        assert_eq!(
            result,
            Err(FluidError::OrderNotFound("missing".to_string()))
        );
    }

    #[test]
    fn test_apply_book_transition_missing_maker_removal_returns_not_found() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let result = book.apply_book_transition(
            &BookTransition {
                maker_remaining_updates: vec![],
                maker_removals: vec![OrderId("missing".to_string())],
                taker_insertion: None,
            },
            &mut ids.order_interner,
            &mut ids.account_interner,
        );

        assert_eq!(
            result,
            Err(FluidError::OrderNotFound("missing".to_string()))
        );
    }

    #[test]
    fn test_get_entry_returns_none_when_index_slot_points_to_other_order() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "o1",
            "acc1",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "o2",
            "acc2",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            2,
        ));

        let slot_for_o2 = multi_outcome_book(&book)
            .unwrap()
            .index
            .get(&ids.order_id("o2"))
            .map(|location| location.slot)
            .unwrap();
        if let Some(location) = multi_outcome_book_mut(&mut book)
            .unwrap()
            .index
            .get_mut(&ids.order_id("o1"))
        {
            location.slot = slot_for_o2;
        }

        assert!(book.get_entry(ids.order_id("o1")).is_none());
    }

    #[test]
    fn test_cancel_returns_none_when_index_points_to_unknown_outcome() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "o1",
            "acc1",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            1,
        ));
        if let Some(location) = multi_outcome_book_mut(&mut book)
            .unwrap()
            .index
            .get_mut(&ids.order_id("o1"))
        {
            location.outcome = "UNKNOWN".to_string();
        }

        assert!(book.cancel(ids.order_id("o1")).is_none());
    }

    #[test]
    fn test_match_rejects_invalid_resting_entry_state() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let order_id = OrderId("maker-1".to_string());
        book.insert(make_entry(
            &mut ids,
            &order_id.0,
            "acc1",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            1,
        ));
        let slot = multi_outcome_book(&book)
            .unwrap()
            .index
            .get(&ids.order_id(&order_id.0))
            .map(|location| location.slot)
            .unwrap();
        if let Some(entry) = multi_outcome_book_mut(&mut book)
            .unwrap()
            .arena
            .get_mut(slot)
        {
            entry.remaining_minor = 0;
        }

        let incoming = make_limit_incoming("taker-1", "YES", OrderSide::Buy, 50, 5);
        let result = match_order_for_test(&book, &ids, &incoming, "v1", 0);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message))
                if message.contains("invalid resting order")
                    && message.contains("remaining_minor must be positive")
        ));
    }

    #[test]
    fn test_match_rejects_resting_side_mismatch() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let order_id = OrderId("maker-1".to_string());
        book.insert(make_entry(
            &mut ids,
            &order_id.0,
            "acc1",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            1,
        ));
        let slot = multi_outcome_book(&book)
            .unwrap()
            .index
            .get(&ids.order_id(&order_id.0))
            .map(|location| location.slot)
            .unwrap();
        if let Some(entry) = multi_outcome_book_mut(&mut book)
            .unwrap()
            .arena
            .get_mut(slot)
        {
            entry.side = OrderSide::Buy;
        }

        let incoming = make_limit_incoming("taker-1", "YES", OrderSide::Buy, 50, 5);
        let result = match_order_for_test(&book, &ids, &incoming, "v1", 0);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message))
                if message.contains("side mismatch")
                    && message.contains("expected Sell")
        ));
    }

    #[test]
    fn test_match_rejects_resting_outcome_mismatch() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let order_id = OrderId("maker-1".to_string());
        book.insert(make_entry(
            &mut ids,
            &order_id.0,
            "acc1",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            1,
        ));
        let slot = multi_outcome_book(&book)
            .unwrap()
            .index
            .get(&ids.order_id(&order_id.0))
            .map(|location| location.slot)
            .unwrap();
        if let Some(entry) = multi_outcome_book_mut(&mut book)
            .unwrap()
            .arena
            .get_mut(slot)
        {
            entry.outcome = "NO".to_string();
        }

        let incoming = make_limit_incoming("taker-1", "YES", OrderSide::Buy, 50, 5);
        let result = match_order_for_test(&book, &ids, &incoming, "v1", 0);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message))
                if message.contains("outcome mismatch")
                    && message.contains("expected YES")
                    && message.contains("got NO")
        ));
    }

    #[test]
    fn test_insert_and_view_returns_correct_book() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let ask1 = make_entry(
            &mut ids,
            "a1",
            "acc1",
            "YES",
            OrderSide::Sell,
            60,
            100,
            100,
            1,
        );
        let ask2 = make_entry(
            &mut ids,
            "a2",
            "acc2",
            "YES",
            OrderSide::Sell,
            50,
            200,
            200,
            2,
        );
        book.insert(ask1);
        book.insert(ask2);

        let view = view_for_match_for_test(&book, &ids, "YES", OrderSide::Buy).unwrap();
        assert_eq!(view.asks.len(), 2);
        // Ascending price: 50 before 60
        assert_eq!(view.asks[0].price_ticks, 50);
        assert_eq!(view.asks[1].price_ticks, 60);
        assert!(view.bids.is_empty());
    }

    #[test]
    fn test_cancel_removes_order_and_returns_entry() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let entry = make_entry(
            &mut ids,
            "o1",
            "acc1",
            "YES",
            OrderSide::Buy,
            55,
            100,
            100,
            1,
        );
        book.insert(entry.clone());
        assert_eq!(book.order_count(), 1);

        let cancelled = book.cancel(ids.order_id("o1"));
        assert!(cancelled.is_some());
        assert_eq!(cancelled.unwrap().order_id, entry.order_id);
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_cancel_nonexistent_returns_none() {
        let mut book = MarketBook::new_multi_outcome();
        assert!(book.cancel(InternalOrderId(99)).is_none());
    }

    #[test]
    fn test_apply_book_transition_removes_filled_orders() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let entry = make_entry(
            &mut ids,
            "m1",
            "acc1",
            "YES",
            OrderSide::Sell,
            60,
            100,
            100,
            1,
        );
        book.insert(entry);
        assert_eq!(book.order_count(), 1);

        let bt = BookTransition {
            maker_remaining_updates: vec![],
            maker_removals: vec![OrderId("m1".to_string())],
            taker_insertion: None,
        };
        book.apply_book_transition(&bt, &mut ids.order_interner, &mut ids.account_interner)
            .unwrap();
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_apply_book_transition_updates_partial_fills() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        let entry = make_entry(
            &mut ids,
            "m1",
            "acc1",
            "YES",
            OrderSide::Sell,
            60,
            100,
            100,
            1,
        );
        book.insert(entry);

        let bt = BookTransition {
            maker_remaining_updates: vec![(OrderId("m1".to_string()), 40, 40)],
            maker_removals: vec![],
            taker_insertion: None,
        };
        book.apply_book_transition(&bt, &mut ids.order_interner, &mut ids.account_interner)
            .unwrap();

        let e = book.get_entry(ids.order_id("m1")).unwrap();
        assert_eq!(e.remaining_minor, 40);
        assert_eq!(e.locked_minor, 40);
    }

    #[test]
    fn test_insert_multiple_price_levels_maintains_sort() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "b1",
            "a1",
            "YES",
            OrderSide::Buy,
            50,
            100,
            100,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "b2",
            "a2",
            "YES",
            OrderSide::Buy,
            60,
            100,
            100,
            2,
        ));
        book.insert(make_entry(
            &mut ids,
            "b3",
            "a3",
            "YES",
            OrderSide::Buy,
            55,
            100,
            100,
            3,
        ));

        let view = view_for_match_for_test(&book, &ids, "YES", OrderSide::Sell).unwrap();
        // Bids sorted descending: 60, 55, 50
        assert_eq!(view.bids[0].price_ticks, 60);
        assert_eq!(view.bids[1].price_ticks, 55);
        assert_eq!(view.bids[2].price_ticks, 50);
    }

    #[test]
    fn test_order_count_tracks_correctly() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        assert_eq!(book.order_count(), 0);

        book.insert(make_entry(
            &mut ids,
            "o1",
            "a1",
            "YES",
            OrderSide::Buy,
            50,
            100,
            100,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "o2",
            "a1",
            "NO",
            OrderSide::Sell,
            60,
            200,
            200,
            2,
        ));
        assert_eq!(book.order_count(), 2);

        book.cancel(ids.order_id("o1"));
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn test_view_for_nonexistent_outcome_returns_empty() {
        let ids = TestIds::default();
        let book = MarketBook::new_multi_outcome();
        let view = view_for_match_for_test(&book, &ids, "NOPE", OrderSide::Buy).unwrap();
        assert!(view.bids.is_empty());
        assert!(view.asks.is_empty());
    }

    #[test]
    fn test_time_priority_within_price_level() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a2",
            "acc",
            "YES",
            OrderSide::Sell,
            60,
            100,
            100,
            200,
        ));
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            60,
            100,
            100,
            100,
        ));

        let view = view_for_match_for_test(&book, &ids, "YES", OrderSide::Buy).unwrap();
        // Same price level, insertion order preserved (FIFO): a2 then a1
        assert_eq!(view.asks[0].order_id.0, "a2");
        assert_eq!(view.asks[1].order_id.0, "a1");
    }

    #[test]
    fn test_match_buy_against_asks_ascending() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a2",
            "acc",
            "YES",
            OrderSide::Sell,
            60,
            4,
            4,
            2,
        ));
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            50,
            4,
            4,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "a3",
            "acc",
            "YES",
            OrderSide::Sell,
            50,
            4,
            4,
            3,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 55,
            quantity_minor: 5,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 7).unwrap();

        assert_eq!(outcome.incoming_remaining_minor, 0);
        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id.0, "a1");
        assert_eq!(outcome.fills[0].price_ticks, 50);
        assert_eq!(outcome.fills[0].canonical_price_ticks, 50);
        assert_eq!(outcome.fills[0].maker_outcome, "YES");
        assert_eq!(outcome.fills[0].maker_side, OrderSide::Sell);
        assert_eq!(outcome.fills[0].maker_price_ticks, 50);
        assert_eq!(outcome.fills[0].taker_outcome, "YES");
        assert_eq!(outcome.fills[0].taker_side, OrderSide::Buy);
        assert_eq!(outcome.fills[0].taker_price_ticks, 50);
        assert_eq!(outcome.fills[0].quantity_minor, 4);
        assert_eq!(outcome.fills[1].maker_order_id.0, "a3");
        assert_eq!(outcome.fills[1].quantity_minor, 1);
        assert_eq!(outcome.maker_updates[1].new_remaining_minor, 3);
        assert_eq!(outcome.fills[0].sequence, 7);
        assert_eq!(outcome.fills[1].sequence, 8);
    }

    #[test]
    fn test_match_sell_against_bids_descending() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "b1",
            "acc",
            "YES",
            OrderSide::Buy,
            50,
            10,
            10,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "b2",
            "acc",
            "YES",
            OrderSide::Buy,
            70,
            2,
            2,
            2,
        ));
        book.insert(make_entry(
            &mut ids,
            "b3",
            "acc",
            "YES",
            OrderSide::Buy,
            60,
            10,
            10,
            3,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "seller".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Sell,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 55,
            quantity_minor: 7,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 3).unwrap();

        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id.0, "b2");
        assert_eq!(outcome.fills[0].price_ticks, 70);
        assert_eq!(outcome.fills[0].quantity_minor, 2);
        assert_eq!(outcome.fills[1].maker_order_id.0, "b3");
        assert_eq!(outcome.fills[1].price_ticks, 60);
        assert_eq!(outcome.fills[1].quantity_minor, 5);
        assert_eq!(outcome.incoming_remaining_minor, 0);
    }

    #[test]
    fn test_match_fifo_within_price_level() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            10,
        ));
        book.insert(make_entry(
            &mut ids,
            "a2",
            "acc",
            "YES",
            OrderSide::Sell,
            50,
            5,
            5,
            20,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 50,
            quantity_minor: 8,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 11).unwrap();

        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id.0, "a1");
        assert_eq!(outcome.fills[1].maker_order_id.0, "a2");
    }

    #[test]
    fn test_match_partial_fill() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            50,
            20,
            20,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "a2",
            "acc2",
            "YES",
            OrderSide::Sell,
            55,
            30,
            30,
            2,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 60,
            quantity_minor: 25,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.incoming_remaining_minor, 0);
        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].quantity_minor, 20);
        assert_eq!(outcome.fills[1].quantity_minor, 5);
        assert_eq!(outcome.maker_updates[0].new_remaining_minor, 0);
        assert_eq!(outcome.maker_updates[1].new_remaining_minor, 25);
    }

    #[test]
    fn test_match_no_crossing() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            80,
            10,
            10,
            1,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 70,
            quantity_minor: 10,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.fills.len(), 0);
        assert_eq!(outcome.incoming_remaining_minor, 10);
        assert_eq!(outcome.maker_updates.len(), 0);
    }

    #[test]
    fn test_match_market_order_crosses_all() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_entry(
            &mut ids,
            "a1",
            "acc",
            "YES",
            OrderSide::Sell,
            100,
            2,
            2,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "a2",
            "acc",
            "YES",
            OrderSide::Sell,
            120,
            5,
            5,
            2,
        ));

        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Market,
            tif: pebble_trading::TimeInForce::Ioc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 0,
            quantity_minor: 10,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 9).unwrap();

        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id.0, "a1");
        assert_eq!(outcome.fills[1].maker_order_id.0, "a2");
        assert_eq!(outcome.incoming_remaining_minor, 3);
    }

    #[test]
    fn test_match_empty_book() {
        let ids = TestIds::default();
        let book = MarketBook::new_multi_outcome();
        let incoming = pebble_trading::IncomingOrder {
            order_id: OrderId("t1".to_string()),
            account_id: "buyer".to_string(),
            market_id: pebble_trading::MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: pebble_trading::OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 10,
            quantity_minor: 10,
            submitted_at_micros: 1,
        };

        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.fills.len(), 0);
        assert_eq!(outcome.incoming_remaining_minor, 10);
        assert_eq!(outcome.maker_updates.len(), 0);
    }

    #[test]
    fn test_cross_outcome_buy_yes_matches_buy_no() {
        let mut ids = TestIds::default();
        let mut book = make_binary_book();
        book.insert(make_entry(
            &mut ids,
            "maker-buy-no",
            "maker",
            "NO",
            OrderSide::Buy,
            4_000,
            5,
            20_000,
            1,
        ));

        let incoming = make_limit_incoming("taker-buy-yes", "YES", OrderSide::Buy, 6_000, 3);
        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.incoming_remaining_minor, 0);
        assert_eq!(outcome.fills.len(), 1);
        assert_eq!(outcome.maker_updates.len(), 1);
        assert_eq!(outcome.fills[0].maker_order_id.0, "maker-buy-no");
        assert_eq!(outcome.fills[0].quantity_minor, 3);
        assert_eq!(outcome.fills[0].canonical_price_ticks, 6_000);
        assert_eq!(outcome.fills[0].maker_outcome, "NO");
        assert_eq!(outcome.fills[0].maker_side, OrderSide::Buy);
        assert_eq!(outcome.fills[0].maker_price_ticks, 4_000);
        assert_eq!(outcome.fills[0].taker_outcome, "YES");
        assert_eq!(outcome.fills[0].taker_side, OrderSide::Buy);
        assert_eq!(outcome.fills[0].taker_price_ticks, 6_000);
        assert_eq!(outcome.fills[0].price_ticks, 6_000);
        assert_eq!(outcome.maker_updates[0].new_remaining_minor, 2);
    }

    #[test]
    fn test_cross_outcome_sell_yes_matches_sell_no() {
        let mut ids = TestIds::default();
        let mut book = make_binary_book();
        book.insert(make_entry(
            &mut ids,
            "maker-sell-no",
            "maker",
            "NO",
            OrderSide::Sell,
            6_000,
            7,
            7,
            1,
        ));

        let incoming = make_limit_incoming("taker-sell-yes", "YES", OrderSide::Sell, 4_000, 4);
        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.incoming_remaining_minor, 0);
        assert_eq!(outcome.fills.len(), 1);
        assert_eq!(outcome.maker_updates.len(), 1);
        assert_eq!(outcome.fills[0].maker_order_id.0, "maker-sell-no");
        assert_eq!(outcome.fills[0].quantity_minor, 4);
        assert_eq!(outcome.fills[0].canonical_price_ticks, 4_000);
        assert_eq!(outcome.fills[0].maker_outcome, "NO");
        assert_eq!(outcome.fills[0].maker_side, OrderSide::Sell);
        assert_eq!(outcome.fills[0].maker_price_ticks, 6_000);
        assert_eq!(outcome.fills[0].taker_outcome, "YES");
        assert_eq!(outcome.fills[0].taker_side, OrderSide::Sell);
        assert_eq!(outcome.fills[0].taker_price_ticks, 4_000);
        assert_eq!(outcome.fills[0].price_ticks, 4_000);
        assert_eq!(outcome.maker_updates[0].new_remaining_minor, 3);
    }

    #[test]
    fn test_cross_outcome_fifo_across_outcomes() {
        let mut ids = TestIds::default();
        let mut book = make_binary_book();
        book.insert(make_entry(
            &mut ids,
            "maker-sell-yes",
            "maker-a",
            "YES",
            OrderSide::Sell,
            5_000,
            1,
            1,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "maker-buy-no",
            "maker-b",
            "NO",
            OrderSide::Buy,
            5_000,
            1,
            5_000,
            2,
        ));

        let incoming = make_limit_incoming("taker", "YES", OrderSide::Buy, 5_000, 2);
        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id.0, "maker-sell-yes");
        assert_eq!(outcome.fills[1].maker_order_id.0, "maker-buy-no");
    }

    #[test]
    fn test_complement_normalization_roundtrip() {
        let mut ids = TestIds::default();
        let mut book = make_binary_book();
        book.insert(make_entry(
            &mut ids,
            "buy-no",
            "maker-a",
            "NO",
            OrderSide::Buy,
            2_500,
            2,
            5_000,
            1,
        ));
        book.insert(make_entry(
            &mut ids,
            "sell-no",
            "maker-b",
            "NO",
            OrderSide::Sell,
            7_600,
            3,
            3,
            2,
        ));

        let buy_no = book.get_entry(ids.order_id("buy-no")).unwrap();
        assert_eq!(buy_no.outcome, "NO");
        assert_eq!(buy_no.side, OrderSide::Buy);
        assert_eq!(buy_no.price_ticks, 2_500);

        let sell_no = book.get_entry(ids.order_id("sell-no")).unwrap();
        assert_eq!(sell_no.outcome, "NO");
        assert_eq!(sell_no.side, OrderSide::Sell);
        assert_eq!(sell_no.price_ticks, 7_600);

        let view = view_for_match_for_test(&book, &ids, "YES", OrderSide::Sell).unwrap();
        assert_eq!(view.bids.len(), 1);
        assert_eq!(view.bids[0].order_id.0, "sell-no");
        assert_eq!(view.bids[0].outcome, "NO");
        assert_eq!(view.bids[0].side, OrderSide::Sell);
        assert_eq!(view.bids[0].price_ticks, 7_600);
    }

    #[test]
    fn test_same_outcome_fills_unchanged_binary() {
        let mut ids = TestIds::default();
        let mut book = make_binary_book();
        book.insert(make_entry(
            &mut ids,
            "maker-sell-yes",
            "maker",
            "YES",
            OrderSide::Sell,
            5_100,
            2,
            2,
            1,
        ));

        let incoming = make_limit_incoming("taker-buy-yes", "YES", OrderSide::Buy, 6_000, 2);
        let outcome = match_order_for_test(&book, &ids, &incoming, "v1", 0).unwrap();

        assert_eq!(outcome.fills.len(), 1);
        assert_eq!(outcome.fills[0].canonical_price_ticks, 5_100);
        assert_eq!(outcome.fills[0].maker_outcome, "YES");
        assert_eq!(outcome.fills[0].taker_outcome, "YES");
        assert_eq!(outcome.fills[0].maker_price_ticks, 5_100);
        assert_eq!(outcome.fills[0].taker_price_ticks, 5_100);
        assert_eq!(outcome.fills[0].price_ticks, 5_100);
    }

    #[test]
    fn test_binary_match_rejects_unknown_outcome_label() {
        let ids = TestIds::default();
        let book = make_binary_book();
        let incoming = make_limit_incoming("taker-buy", "MAYBE", OrderSide::Buy, 5_000, 1);

        let result = match_order_for_test(&book, &ids, &incoming, "v1", 0);
        assert!(matches!(
            result,
            Err(FluidError::MatchFailed(message))
                if message.contains("outcome 'MAYBE' not in binary config")
        ));
    }

    #[test]
    fn test_match_outcome_agrees_with_presorted() {
        let mut state = 1u64;

        for case_idx in 0..128u64 {
            let mut ids = TestIds::default();
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            let outcome = if state & 1 == 0 { "YES" } else { "NO" };
            let incoming_side = if (state >> 1) & 1 == 0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let incoming_order_type = if (state >> 2) & 1 == 0 {
                pebble_trading::OrderType::Limit
            } else {
                pebble_trading::OrderType::Market
            };

            let mut book = MarketBook::new_multi_outcome();
            for idx in 0..16 {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                let side = if (state >> 3) & 1 == 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let price_ticks = 1 + (state & 100) as i64;
                let remaining_minor = 1 + ((state >> 8) & 40) as i64;
                let time = 1 + i64::from(idx);
                book.insert(make_entry(
                    &mut ids,
                    &format!("maker-{outcome}-{case_idx}-{idx}"),
                    &format!("acc-{idx}"),
                    outcome,
                    side,
                    price_ticks,
                    remaining_minor,
                    remaining_minor,
                    time,
                ));
            }

            let incoming = pebble_trading::IncomingOrder {
                order_id: OrderId(format!("taker-{case_idx}")),
                account_id: format!("buyer-{case_idx}"),
                market_id: pebble_trading::MarketId("m1".to_string()),
                outcome: outcome.to_string(),
                side: incoming_side,
                order_type: incoming_order_type,
                tif: match incoming_order_type {
                    pebble_trading::OrderType::Limit => pebble_trading::TimeInForce::Gtc,
                    pebble_trading::OrderType::Market => pebble_trading::TimeInForce::Ioc,
                },
                nonce: pebble_trading::OrderNonce(case_idx as i64),
                price_ticks: if incoming_order_type == pebble_trading::OrderType::Limit {
                    50 + (state >> 12 & 100) as i64
                } else {
                    0
                },
                quantity_minor: 1 + ((state >> 10) & 50) as i64,
                submitted_at_micros: 1,
            };

            let direct_book =
                view_for_match_for_test(&book, &ids, &incoming.outcome, incoming.side).unwrap();
            let direct_result =
                pebble_trading::match_order_presorted(direct_book, &incoming, "stage-a", 200)
                    .unwrap();
            let custom_result =
                match_order_for_test(&book, &ids, &incoming, "stage-a", 200).unwrap();

            assert_eq!(direct_result.fills.len(), custom_result.fills.len());
            assert_eq!(
                direct_result
                    .fills
                    .iter()
                    .map(|fill| {
                        (
                            fill.maker_order_id.clone(),
                            fill.price_ticks,
                            fill.quantity_minor,
                            fill.sequence,
                            fill.fill_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>(),
                custom_result
                    .fills
                    .iter()
                    .map(|fill| {
                        (
                            fill.maker_order_id.clone(),
                            fill.price_ticks,
                            fill.quantity_minor,
                            fill.sequence,
                            fill.fill_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                direct_result.incoming_remaining_minor,
                custom_result.incoming_remaining_minor
            );
            assert_eq!(
                direct_result
                    .maker_updates
                    .iter()
                    .map(|mu| (mu.order_id.clone(), mu.new_remaining_minor))
                    .collect::<Vec<_>>(),
                custom_result
                    .maker_updates
                    .iter()
                    .map(|mu| (mu.order_id.clone(), mu.new_remaining_minor))
                    .collect::<Vec<_>>()
            );
        }
    }
}
