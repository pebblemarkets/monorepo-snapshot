use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pebble_trading::{
    IncomingOrder, InternalAccountId, InternalOrderId, MarketId, OrderId, OrderSide, OrderType,
};

use crate::actor::RiskActorHandle;
use crate::book::{MarketBook, MatchOutcome};
use crate::error::FluidError;
use crate::events::OrderStatus;
use crate::ids::StringInterner;
use crate::risk::RiskEngine;
use crate::transition::{
    AccountRiskDelta, BookTransition, CancelOrderTransition, MakerUpdate, PendingRestingEntry,
    PlaceOrderTransition, RiskTransition,
};
use tokio::sync::{Mutex as AsyncMutex, RwLock};

#[expect(
    clippy::too_many_arguments,
    reason = "transition building requires explicit engine inputs"
)]
pub(crate) fn build_place_order_transition(
    incoming: &IncomingOrder,
    match_result: &MatchOutcome,
    current_book: Option<&MarketBook>,
    order_interner: &StringInterner<InternalOrderId>,
    account_interner: &StringInterner<InternalAccountId>,
    available_minor_before: i64,
    sequences: &SequenceState,
    replay_mode: bool,
) -> Result<PlaceOrderTransition, FluidError> {
    let matcher_remaining = match_result.incoming_remaining_minor;
    let (taker_remaining, taker_locked, taker_status) = match incoming.order_type {
        OrderType::Limit => {
            let locked =
                lock_required_minor(incoming.side, incoming.price_ticks, matcher_remaining)?;
            let status = compute_order_status(incoming.quantity_minor, matcher_remaining);
            (matcher_remaining, locked, status)
        }
        OrderType::Market => {
            let status = if matcher_remaining == 0 {
                OrderStatus::Filled
            } else {
                OrderStatus::Cancelled
            };
            (0, 0, status)
        }
    };

    if !replay_mode && incoming.order_type == OrderType::Market && incoming.side == OrderSide::Buy {
        let total_cost: i64 = match_result
            .fills
            .iter()
            .try_fold(0i64, |acc: i64, fill| {
                fill.taker_price_ticks
                    .checked_mul(fill.quantity_minor)
                    .and_then(|cost| acc.checked_add(cost))
            })
            .ok_or_else(|| FluidError::Overflow("market buy cost overflow".to_string()))?;
        if total_cost > available_minor_before {
            return Err(FluidError::InsufficientBalance {
                available: available_minor_before,
                required: total_cost,
            });
        }
    }

    let mut risk_deltas: HashMap<String, AccountRiskDelta> = HashMap::new();

    if taker_locked != 0 {
        risk_deltas
            .entry(incoming.account_id.clone())
            .or_insert_with(|| AccountRiskDelta {
                account_id: incoming.account_id.clone(),
                delta_pending_minor: 0,
                locked_delta_minor: 0,
            })
            .locked_delta_minor = taker_locked;
    }

    let mut maker_remaining_updates = Vec::new();
    let mut maker_removals = Vec::new();
    let mut maker_updates = Vec::new();

    for mu in &match_result.maker_updates {
        let maker_entry = current_book
            .and_then(|book| book.get_entry_by_external_order_id(&mu.order_id, order_interner))
            .ok_or_else(|| FluidError::OrderNotFound(mu.order_id.0.clone()))?;
        let maker_account_id = account_interner
            .to_external(maker_entry.account_id)
            .ok_or_else(|| {
                FluidError::OrderNotFound(format!(
                    "maker account mapping missing for {}",
                    mu.order_id.0
                ))
            })?;

        let maker_new_locked = lock_required_minor(
            maker_entry.side,
            maker_entry.price_ticks,
            mu.new_remaining_minor,
        )?;
        let locked_delta = maker_new_locked
            .checked_sub(maker_entry.locked_minor)
            .ok_or_else(|| {
                FluidError::Overflow(format!("maker locked delta for {}", mu.order_id.0))
            })?;

        let maker_risk = risk_deltas
            .entry(maker_account_id.to_string())
            .or_insert_with(|| AccountRiskDelta {
                account_id: maker_account_id.to_string(),
                delta_pending_minor: 0,
                locked_delta_minor: 0,
            });
        maker_risk.locked_delta_minor = maker_risk
            .locked_delta_minor
            .checked_add(locked_delta)
            .ok_or_else(|| {
                FluidError::Overflow(format!(
                    "maker locked accumulation for {}",
                    maker_account_id
                ))
            })?;

        if mu.new_remaining_minor == 0 {
            maker_removals.push(mu.order_id.clone());
        } else {
            maker_remaining_updates.push((
                mu.order_id.clone(),
                mu.new_remaining_minor,
                maker_new_locked,
            ));
        }

        maker_updates.push(MakerUpdate {
            order_id: mu.order_id.clone(),
            new_remaining_minor: mu.new_remaining_minor,
        });
    }

    for fill in &match_result.fills {
        let maker_cash =
            cash_delta_minor(fill.maker_side, fill.maker_price_ticks, fill.quantity_minor)?;
        let taker_cash =
            cash_delta_minor(fill.taker_side, fill.taker_price_ticks, fill.quantity_minor)?;

        risk_deltas
            .entry(fill.maker_account_id.clone())
            .or_insert_with(|| AccountRiskDelta {
                account_id: fill.maker_account_id.clone(),
                delta_pending_minor: 0,
                locked_delta_minor: 0,
            })
            .delta_pending_minor = risk_deltas
            .get(&fill.maker_account_id)
            .map_or(0, |d| d.delta_pending_minor)
            .checked_add(maker_cash)
            .ok_or_else(|| {
                FluidError::Overflow(format!("maker pending delta for {}", fill.maker_account_id))
            })?;

        risk_deltas
            .entry(fill.taker_account_id.clone())
            .or_insert_with(|| AccountRiskDelta {
                account_id: fill.taker_account_id.clone(),
                delta_pending_minor: 0,
                locked_delta_minor: 0,
            })
            .delta_pending_minor = risk_deltas
            .get(&fill.taker_account_id)
            .map_or(0, |d| d.delta_pending_minor)
            .checked_add(taker_cash)
            .ok_or_else(|| {
                FluidError::Overflow(format!("taker pending delta for {}", fill.taker_account_id))
            })?;
    }

    let taker_insertion = if taker_remaining > 0 && incoming.order_type == OrderType::Limit {
        Some(PendingRestingEntry {
            order_id: incoming.order_id.clone(),
            account_id: incoming.account_id.clone(),
            outcome: incoming.outcome.clone(),
            side: incoming.side,
            price_ticks: incoming.price_ticks,
            remaining_minor: taker_remaining,
            locked_minor: taker_locked,
            submitted_at_micros: incoming.submitted_at_micros,
        })
    } else {
        None
    };

    let fill_count = match_result.fills.len() as i64;
    let taker_fill_delta = risk_deltas
        .get(&incoming.account_id)
        .map_or(0, |d| d.delta_pending_minor);
    let available_minor_after = if replay_mode {
        available_minor_before
            .saturating_add(taker_fill_delta)
            .saturating_sub(taker_locked)
    } else {
        available_minor_before
            .checked_add(taker_fill_delta)
            .and_then(|v| v.checked_sub(taker_locked))
            .ok_or_else(|| FluidError::Overflow("available_after computation".to_string()))?
    };

    Ok(PlaceOrderTransition {
        market_id: incoming.market_id.clone(),
        book_transition: BookTransition {
            maker_remaining_updates,
            maker_removals,
            taker_insertion,
        },
        risk_transition: RiskTransition {
            deltas: risk_deltas.into_values().collect(),
            nonce_advance: Some((incoming.account_id.clone(), incoming.nonce.0)),
        },
        fills: match_result.fills.clone(),
        taker_status,
        taker_remaining,
        taker_locked,
        available_minor_after,
        maker_updates,
        next_fill_sequence: sequences
            .next_fill_sequence
            .checked_add(fill_count)
            .ok_or_else(|| FluidError::Overflow("fill sequence overflow".to_string()))?,
        next_event_sequence: sequences
            .next_event_sequence
            .checked_add(fill_count)
            .and_then(|v| v.checked_add(1))
            .ok_or_else(|| FluidError::Overflow("event sequence overflow".to_string()))?,
    })
}

/// Write-only health accessor used by panic safety guards.
#[derive(Clone)]
pub struct HealthHandle(Arc<AtomicBool>);

impl HealthHandle {
    pub fn mark_unhealthy(&self, reason: &str) {
        self.0.store(false, Ordering::SeqCst);
        tracing::error!(reason, "fluid engine marked unhealthy");
    }
}

/// RAII guard for single-flight resync ownership.
pub struct ResyncGuard<'a> {
    _resync_mutex: tokio::sync::MutexGuard<'a, ()>,
    _barrier: tokio::sync::RwLockWriteGuard<'a, ()>,
}

/// Per-market engine. Owns one book, sequences, and metadata.
pub struct MarketEngine {
    pub(crate) book: MarketBook,
    pub(crate) sequences: SequenceState,
    pub(crate) meta: MarketMeta,
}

impl MarketEngine {
    pub(crate) fn new(book: MarketBook, sequences: SequenceState, meta: MarketMeta) -> Self {
        Self {
            book,
            sequences,
            meta,
        }
    }

    pub fn compute_cancel_order(
        &self,
        market_id: &MarketId,
        order_id: &OrderId,
        account_id: &str,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<CancelOrderTransition, FluidError> {
        let internal_order_id = order_interner
            .lookup(&order_id.0)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;
        let entry = self
            .book
            .get_entry(internal_order_id)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;
        let entry_account_id = account_interner
            .to_external(entry.account_id)
            .ok_or_else(|| {
                FluidError::OrderNotFound(format!(
                    "account mapping missing for order {}",
                    order_id.0
                ))
            })?;
        if entry_account_id != account_id {
            return Err(FluidError::Unauthorized);
        }

        Ok(CancelOrderTransition {
            market_id: market_id.clone(),
            order_id: order_id.clone(),
            account_id: account_id.to_string(),
            released_locked_minor: entry.locked_minor,
        })
    }

    pub fn compute_place_order(
        &self,
        incoming: &IncomingOrder,
        available_minor_before: i64,
        engine_version: &str,
        order_interner: &StringInterner<InternalOrderId>,
        account_interner: &StringInterner<InternalAccountId>,
    ) -> Result<PlaceOrderTransition, FluidError> {
        if self.meta.status != "Open" {
            return Err(FluidError::MarketNotOpen(incoming.market_id.0.clone()));
        }

        if !self.meta.outcomes.contains(&incoming.outcome) {
            return Err(FluidError::MatchFailed(format!(
                "outcome '{}' not valid for market '{}'",
                incoming.outcome, incoming.market_id.0
            )));
        }
        validate_market_price(incoming, &self.meta)?;

        let match_result = self.book.match_order(
            incoming,
            order_interner,
            account_interner,
            engine_version,
            self.sequences.next_fill_sequence,
        )?;

        build_place_order_transition(
            incoming,
            &match_result,
            Some(&self.book),
            order_interner,
            account_interner,
            available_minor_before,
            &self.sequences,
            false,
        )
    }

    pub fn apply_place_order(
        &mut self,
        transition: &PlaceOrderTransition,
        order_interner: &mut StringInterner<InternalOrderId>,
        account_interner: &mut StringInterner<InternalAccountId>,
    ) -> Result<(), FluidError> {
        self.book.apply_book_transition(
            &transition.book_transition,
            order_interner,
            account_interner,
        )?;
        self.sequences.next_fill_sequence = transition.next_fill_sequence;
        self.sequences.next_event_sequence = transition.next_event_sequence;
        Ok(())
    }

    pub fn apply_cancel_order(
        &mut self,
        transition: &CancelOrderTransition,
        order_interner: &StringInterner<InternalOrderId>,
    ) -> Result<(), FluidError> {
        let internal_order_id = order_interner
            .lookup(&transition.order_id.0)
            .ok_or_else(|| FluidError::OrderNotFound(transition.order_id.0.clone()))?;
        self.book
            .cancel(internal_order_id)
            .ok_or_else(|| FluidError::OrderNotFound(transition.order_id.0.clone()))?;
        Ok(())
    }
}

/// Top-level exchange engine. Not behind a global mutex.
pub struct ExchangeEngine {
    markets: RwLock<HashMap<MarketId, Arc<AsyncMutex<MarketEngine>>>>,
    risk_handle: RiskActorHandle,
    engine_version: Arc<str>,
    order_available_includes_withdrawal_reserves: bool,
    healthy: Arc<AtomicBool>,
    order_index: RwLock<HashMap<InternalOrderId, MarketId>>,
    order_interner: RwLock<StringInterner<InternalOrderId>>,
    account_interner: RwLock<StringInterner<InternalAccountId>>,
    resync_barrier: RwLock<()>,
    resync_mutex: AsyncMutex<()>,
}

impl ExchangeEngine {
    #[expect(
        clippy::too_many_arguments,
        reason = "engine bootstrap wires multiple independent state components"
    )]
    pub fn new(
        books: HashMap<MarketId, MarketBook>,
        risk_handle: RiskActorHandle,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
        engine_version: impl Into<Arc<str>>,
        order_available_includes_withdrawal_reserves: bool,
    ) -> Self {
        let mut markets = HashMap::new();
        let mut order_index = HashMap::new();

        let mut market_ids: HashSet<MarketId> = market_meta.keys().cloned().collect();
        market_ids.extend(books.keys().cloned());
        market_ids.extend(sequences.keys().cloned());

        for market_id in market_ids {
            let Some(meta) = market_meta.get(&market_id).cloned() else {
                continue;
            };

            let book = books
                .get(&market_id)
                .cloned()
                .unwrap_or_else(|| build_book_for_meta(&meta));
            let sequences = sequences.get(&market_id).cloned().unwrap_or_default();

            for order_id in book.order_ids() {
                order_index.insert(order_id, market_id.clone());
            }

            markets.insert(
                market_id,
                Arc::new(AsyncMutex::new(MarketEngine::new(book, sequences, meta))),
            );
        }

        Self {
            markets: RwLock::new(markets),
            risk_handle,
            engine_version: engine_version.into(),
            order_available_includes_withdrawal_reserves,
            healthy: Arc::new(AtomicBool::new(true)),
            order_index: RwLock::new(order_index),
            order_interner: RwLock::new(order_interner),
            account_interner: RwLock::new(account_interner),
            resync_barrier: RwLock::new(()),
            resync_mutex: AsyncMutex::new(()),
        }
    }

    pub async fn get_market(&self, market_id: &MarketId) -> Option<Arc<AsyncMutex<MarketEngine>>> {
        self.markets.read().await.get(market_id).cloned()
    }

    pub fn risk_handle(&self) -> &RiskActorHandle {
        &self.risk_handle
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn health_handle(&self) -> HealthHandle {
        HealthHandle(self.healthy.clone())
    }

    pub fn resync_barrier(&self) -> &RwLock<()> {
        &self.resync_barrier
    }

    pub fn markets(&self) -> &RwLock<HashMap<MarketId, Arc<AsyncMutex<MarketEngine>>>> {
        &self.markets
    }

    pub fn mark_unhealthy(&self, reason: &str) {
        self.healthy.store(false, Ordering::SeqCst);
        tracing::error!(reason, "fluid engine marked unhealthy");
    }

    pub async fn begin_resync(&self) -> Option<ResyncGuard<'_>> {
        if self.is_healthy() {
            return None;
        }

        let resync_mutex = self.resync_mutex.try_lock().ok()?;

        self.mark_unhealthy("resync requested");
        let barrier = self.resync_barrier.write().await;
        Some(ResyncGuard {
            _resync_mutex: resync_mutex,
            _barrier: barrier,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "resync atomically swaps several independent state components"
    )]
    pub async fn complete_resync(
        &self,
        _guard: ResyncGuard<'_>,
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    ) -> Result<(), FluidError> {
        self.risk_handle.resync(risk).await?;

        let mut markets = HashMap::new();
        let mut order_index = HashMap::new();

        let mut all_market_ids: HashSet<MarketId> = market_meta.keys().cloned().collect();
        all_market_ids.extend(books.keys().cloned());
        all_market_ids.extend(sequences.keys().cloned());

        for market_id in all_market_ids {
            let Some(meta) = market_meta.get(&market_id).cloned() else {
                continue;
            };

            let book = books
                .get(&market_id)
                .cloned()
                .unwrap_or_else(|| build_book_for_meta(&meta));
            let sequence = sequences.get(&market_id).cloned().unwrap_or_default();

            for order_id in book.order_ids() {
                order_index.insert(order_id, market_id.clone());
            }

            markets.insert(
                market_id,
                Arc::new(AsyncMutex::new(MarketEngine::new(book, sequence, meta))),
            );
        }

        let mut market_guard = self.markets.write().await;
        *market_guard = markets;

        let mut index_guard = self.order_index.write().await;
        *index_guard = order_index;
        drop(index_guard);

        let mut order_interner_guard = self.order_interner.write().await;
        *order_interner_guard = order_interner;
        drop(order_interner_guard);

        let mut account_interner_guard = self.account_interner.write().await;
        *account_interner_guard = account_interner;

        self.healthy.store(true, Ordering::SeqCst);
        drop(_guard);

        Ok(())
    }

    pub async fn update_market_status(&self, market_id: &MarketId, status: String) {
        if let Some(market) = self.markets.read().await.get(market_id).cloned() {
            let mut market = market.lock().await;
            market.meta.status = status;
        }
    }

    pub fn engine_version(&self) -> &str {
        self.engine_version.as_ref()
    }

    pub fn order_available_includes_withdrawal_reserves(&self) -> bool {
        self.order_available_includes_withdrawal_reserves
    }

    pub async fn market_count(&self) -> usize {
        self.markets.read().await.len()
    }

    pub async fn total_order_count(&self) -> usize {
        let mut total: usize = 0;
        let markets = self.markets.read().await;
        for market in markets.values() {
            let market = market.lock().await;
            total = total.saturating_add(market.book.order_count());
        }
        total
    }

    pub async fn lookup_market_for_order(&self, order_id: &OrderId) -> Option<MarketId> {
        let internal_order_id = {
            let order_interner = self.order_interner.read().await;
            order_interner.lookup(&order_id.0)?
        };
        let order_index = self.order_index.read().await;
        order_index.get(&internal_order_id).cloned()
    }

    pub async fn apply_place_index_updates(
        &self,
        market_id: &MarketId,
        transition: &PlaceOrderTransition,
    ) {
        let mut order_interner = self.order_interner.write().await;
        let mut order_index = self.order_index.write().await;

        for maker_order_id in &transition.book_transition.maker_removals {
            if let Some(internal_order_id) = order_interner.lookup(&maker_order_id.0) {
                let _ = order_index.remove(&internal_order_id);
                order_interner.release(internal_order_id);
            }
        }

        if let Some(taker_insertion) = &transition.book_transition.taker_insertion {
            let internal_order_id = order_interner
                .lookup(&taker_insertion.order_id.0)
                .unwrap_or_else(|| order_interner.intern(&taker_insertion.order_id.0));
            order_index.insert(internal_order_id, market_id.clone());
        }
    }

    pub async fn apply_cancel_index_update(&self, order_id: &OrderId) {
        let mut order_interner = self.order_interner.write().await;
        let mut order_index = self.order_index.write().await;
        if let Some(internal_order_id) = order_interner.lookup(&order_id.0) {
            let _ = order_index.remove(&internal_order_id);
            order_interner.release(internal_order_id);
        }
    }

    pub fn order_interner(&self) -> &RwLock<StringInterner<InternalOrderId>> {
        &self.order_interner
    }

    pub fn account_interner(&self) -> &RwLock<StringInterner<InternalAccountId>> {
        &self.account_interner
    }
}

/// Per-market sequence counters for fills and events.
#[derive(Clone, Debug, Default)]
pub struct SequenceState {
    pub next_fill_sequence: i64,
    pub next_event_sequence: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryConfig {
    pub yes_outcome: String,
    pub no_outcome: String,
    pub max_price_ticks: i64,
}

fn build_book_for_meta(meta: &MarketMeta) -> MarketBook {
    match &meta.binary_config {
        Some(binary_config) => MarketBook::new_binary(binary_config.clone()),
        None => MarketBook::new_multi_outcome(),
    }
}

fn validate_market_price(incoming: &IncomingOrder, meta: &MarketMeta) -> Result<(), FluidError> {
    let Some(binary_config) = &meta.binary_config else {
        return Ok(());
    };
    if binary_config.max_price_ticks <= 1 {
        return Err(FluidError::MatchFailed(format!(
            "invalid max_price_ticks={} for market '{}'",
            binary_config.max_price_ticks, incoming.market_id.0
        )));
    }
    if incoming.order_type == OrderType::Limit
        && (incoming.price_ticks <= 0 || incoming.price_ticks >= binary_config.max_price_ticks)
    {
        return Err(FluidError::MatchFailed(format!(
            "binary limit price_ticks must satisfy 1 <= price_ticks < {}",
            binary_config.max_price_ticks
        )));
    }
    Ok(())
}

/// Metadata about a market.
#[derive(Clone, Debug)]
pub struct MarketMeta {
    pub status: String,
    pub outcomes: Vec<String>,
    pub binary_config: Option<BinaryConfig>,
    pub instrument_admin: String,
    pub instrument_id: String,
}

/// The core in-memory engine state. Pure synchronous struct — no tasks, no channels.
#[derive(Clone)]
pub struct FluidState {
    books: HashMap<MarketId, MarketBook>,
    risk: RiskEngine,
    sequences: HashMap<MarketId, SequenceState>,
    market_meta: HashMap<MarketId, MarketMeta>,
    order_index: HashMap<InternalOrderId, MarketId>,
    order_interner: StringInterner<InternalOrderId>,
    account_interner: StringInterner<InternalAccountId>,
    healthy: bool,
    engine_version: Arc<str>,
    order_available_includes_withdrawal_reserves: bool,
}

impl FluidState {
    /// Create a new `FluidState` from pre-loaded components.
    #[expect(
        clippy::too_many_arguments,
        reason = "state constructor accepts all owned components explicitly"
    )]
    pub fn new(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
        engine_version: impl Into<Arc<str>>,
        order_available_includes_withdrawal_reserves: bool,
    ) -> Self {
        let mut order_index = HashMap::new();
        for (market_id, book) in &books {
            for order_id in book.order_ids() {
                order_index.insert(order_id, market_id.clone());
            }
        }
        Self {
            books,
            risk,
            sequences,
            market_meta,
            order_index,
            order_interner,
            account_interner,
            healthy: true,
            engine_version: engine_version.into(),
            order_available_includes_withdrawal_reserves,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy
    }

    pub fn check_healthy(&self) -> Result<(), FluidError> {
        if self.healthy {
            Ok(())
        } else {
            Err(FluidError::EngineUnhealthy(
                "engine is unhealthy, awaiting resync".to_string(),
            ))
        }
    }

    pub fn risk(&self) -> &RiskEngine {
        &self.risk
    }

    pub fn risk_mut(&mut self) -> &mut RiskEngine {
        &mut self.risk
    }

    pub fn books(&self) -> &HashMap<MarketId, MarketBook> {
        &self.books
    }

    pub fn books_mut(&mut self) -> &mut HashMap<MarketId, MarketBook> {
        &mut self.books
    }

    pub fn market_meta(&self) -> &HashMap<MarketId, MarketMeta> {
        &self.market_meta
    }

    pub fn sequences(&self) -> &HashMap<MarketId, SequenceState> {
        &self.sequences
    }

    pub fn sequences_mut(&mut self) -> &mut HashMap<MarketId, SequenceState> {
        &mut self.sequences
    }

    pub fn engine_version(&self) -> &str {
        self.engine_version.as_ref()
    }

    pub fn order_available_includes_withdrawal_reserves(&self) -> bool {
        self.order_available_includes_withdrawal_reserves
    }

    pub fn order_interner(&self) -> &StringInterner<InternalOrderId> {
        &self.order_interner
    }

    pub fn account_interner(&self) -> &StringInterner<InternalAccountId> {
        &self.account_interner
    }

    /// Compute a `PlaceOrderTransition` without mutating any state.
    pub fn compute_place_order(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<PlaceOrderTransition, FluidError> {
        // 1. Health check
        self.check_healthy()?;

        // 2. Market validation
        let meta = self
            .market_meta
            .get(&incoming.market_id)
            .ok_or_else(|| FluidError::MarketNotFound(incoming.market_id.0.clone()))?;
        if meta.status != "Open" {
            return Err(FluidError::MarketNotOpen(incoming.market_id.0.clone()));
        }
        if !meta.outcomes.contains(&incoming.outcome) {
            return Err(FluidError::MatchFailed(format!(
                "outcome '{}' not valid for market '{}'",
                incoming.outcome, incoming.market_id.0
            )));
        }
        validate_market_price(incoming, meta)?;

        // 3. Account active check
        self.risk.check_account_active(&incoming.account_id)?;

        // 4. Instrument match
        let account = self.risk.get_account(&incoming.account_id)?;
        if account.instrument_admin != meta.instrument_admin
            || account.instrument_id != meta.instrument_id
        {
            return Err(FluidError::InstrumentMismatch);
        }

        // 5. Nonce validation
        self.risk
            .check_nonce(&incoming.account_id, incoming.nonce.0)?;

        // 6. Available balance (raw)
        let raw_available = self.risk.available_minor(&incoming.account_id)?;

        // 7. Subtract withdrawal reserves if toggle is on
        let available = if self.order_available_includes_withdrawal_reserves {
            raw_available
                .checked_sub(account.pending_withdrawals_reserved_minor)
                .ok_or_else(|| FluidError::Overflow("available balance underflow".to_string()))?
        } else {
            raw_available
        };

        // 8. Compute required lock
        let required_lock =
            lock_required_minor(incoming.side, incoming.price_ticks, incoming.quantity_minor)?;

        // 9. Validate sufficiency
        if available < required_lock {
            return Err(FluidError::InsufficientBalance {
                available,
                required: required_lock,
            });
        }

        // 10. Match against the local in-memory book
        let seq = self
            .sequences
            .get(&incoming.market_id)
            .cloned()
            .unwrap_or_default();

        let match_result = match self.books.get(&incoming.market_id) {
            Some(book) => book.match_order(
                incoming,
                &self.order_interner,
                &self.account_interner,
                self.engine_version.as_ref(),
                seq.next_fill_sequence,
            )?,
            None => crate::book::MatchOutcome {
                fills: Vec::new(),
                maker_updates: Vec::new(),
                incoming_remaining_minor: incoming.quantity_minor,
            },
        };

        build_place_order_transition(
            incoming,
            &match_result,
            self.books.get(&incoming.market_id),
            &self.order_interner,
            &self.account_interner,
            available,
            &seq,
            false,
        )
    }

    /// Compute a place-order transition in replay mode.
    /// Skips runtime validations that depend on external state and uses
    /// `replay_mode=true` when building the transition.
    pub fn compute_place_order_replay(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<PlaceOrderTransition, FluidError> {
        let meta = self
            .market_meta
            .get(&incoming.market_id)
            .ok_or_else(|| FluidError::MarketNotFound(incoming.market_id.0.clone()))?;
        validate_market_price(incoming, meta)?;
        let seq = self
            .sequences
            .get(&incoming.market_id)
            .cloned()
            .unwrap_or_default();
        let available = self.risk.available_minor(&incoming.account_id)?;
        let match_result = match self.books.get(&incoming.market_id) {
            Some(book) => book.match_order(
                incoming,
                &self.order_interner,
                &self.account_interner,
                self.engine_version.as_ref(),
                seq.next_fill_sequence,
            )?,
            None => crate::book::MatchOutcome {
                fills: Vec::new(),
                maker_updates: Vec::new(),
                incoming_remaining_minor: incoming.quantity_minor,
            },
        };
        build_place_order_transition(
            incoming,
            &match_result,
            self.books.get(&incoming.market_id),
            &self.order_interner,
            &self.account_interner,
            available,
            &seq,
            true,
        )
    }

    /// Compute a `CancelOrderTransition` without mutating any state.
    pub fn compute_cancel_order(
        &self,
        market_id: &MarketId,
        order_id: &OrderId,
        account_id: &str,
    ) -> Result<CancelOrderTransition, FluidError> {
        self.check_healthy()?;

        let book = self
            .books
            .get(market_id)
            .ok_or_else(|| FluidError::MarketNotFound(market_id.0.clone()))?;

        let internal_order_id = self
            .order_interner
            .lookup(&order_id.0)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;
        let entry = book
            .get_entry(internal_order_id)
            .ok_or_else(|| FluidError::OrderNotFound(order_id.0.clone()))?;

        let entry_account_id = self
            .account_interner
            .to_external(entry.account_id)
            .ok_or_else(|| {
                FluidError::OrderNotFound(format!(
                    "account mapping missing for order {}",
                    order_id.0
                ))
            })?;
        if entry_account_id != account_id {
            return Err(FluidError::Unauthorized);
        }

        Ok(CancelOrderTransition {
            market_id: market_id.clone(),
            order_id: order_id.clone(),
            account_id: account_id.to_string(),
            released_locked_minor: entry.locked_minor,
        })
    }

    /// Apply a computed place-order transition to in-memory state.
    /// Call only after successful DB commit.
    pub fn apply_place_order(
        &mut self,
        transition: PlaceOrderTransition,
    ) -> Result<(), FluidError> {
        if !self.books.contains_key(&transition.market_id) {
            let meta = self
                .market_meta
                .get(&transition.market_id)
                .ok_or_else(|| FluidError::MarketNotFound(transition.market_id.0.clone()))?;
            self.books
                .insert(transition.market_id.clone(), build_book_for_meta(meta));
        }
        let book = self
            .books
            .get_mut(&transition.market_id)
            .ok_or_else(|| FluidError::MarketNotFound(transition.market_id.0.clone()))?;
        book.apply_book_transition(
            &transition.book_transition,
            &mut self.order_interner,
            &mut self.account_interner,
        )?;
        self.risk
            .apply_risk_transition(&transition.risk_transition)?;

        for maker_order_id in &transition.book_transition.maker_removals {
            if let Some(internal_order_id) = self.order_interner.lookup(&maker_order_id.0) {
                let _ = self.order_index.remove(&internal_order_id);
                self.order_interner.release(internal_order_id);
            }
        }
        if let Some(taker_insertion) = &transition.book_transition.taker_insertion {
            let internal_order_id = self
                .order_interner
                .lookup(&taker_insertion.order_id.0)
                .unwrap_or_else(|| self.order_interner.intern(&taker_insertion.order_id.0));
            self.order_index
                .insert(internal_order_id, transition.market_id.clone());
        }

        let seq = self.sequences.entry(transition.market_id).or_default();
        seq.next_fill_sequence = transition.next_fill_sequence;
        seq.next_event_sequence = transition.next_event_sequence;

        Ok(())
    }

    /// Apply a computed cancel-order transition to in-memory state.
    /// Call only after successful DB commit.
    pub fn apply_cancel_order(
        &mut self,
        transition: CancelOrderTransition,
    ) -> Result<(), FluidError> {
        let book = self
            .books
            .get_mut(&transition.market_id)
            .ok_or_else(|| FluidError::MarketNotFound(transition.market_id.0.clone()))?;

        let internal_order_id = self
            .order_interner
            .lookup(&transition.order_id.0)
            .ok_or_else(|| FluidError::OrderNotFound(transition.order_id.0.clone()))?;
        book.cancel(internal_order_id)
            .ok_or_else(|| FluidError::OrderNotFound(transition.order_id.0.clone()))?;
        let _ = self.order_index.remove(&internal_order_id);
        self.order_interner.release(internal_order_id);

        // Release locked capital
        self.risk
            .apply_cancel_risk_delta(&transition.account_id, transition.released_locked_minor)?;

        Ok(())
    }

    /// Mark the engine as unhealthy. All subsequent computes will fail.
    pub fn mark_unhealthy(&mut self, reason: String) {
        tracing::error!(reason = %reason, "fluid engine marked unhealthy");
        self.healthy = false;
    }

    /// Replace all in-memory state from a fresh DB snapshot.
    pub fn resync_from(
        &mut self,
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    ) {
        let mut order_index = HashMap::new();
        for (market_id, book) in &books {
            for order_id in book.order_ids() {
                order_index.insert(order_id, market_id.clone());
            }
        }
        self.books = books;
        self.risk = risk;
        self.sequences = sequences;
        self.market_meta = market_meta;
        self.order_index = order_index;
        self.order_interner = order_interner;
        self.account_interner = account_interner;
        self.healthy = true;
        tracing::info!("fluid engine resynced from DB, marked healthy");
    }

    /// Update market metadata (from sync task).
    pub fn update_market_status(&mut self, market_id: &MarketId, status: String) {
        if let Some(meta) = self.market_meta.get_mut(market_id) {
            meta.status = status;
        }
    }
}

// ── Helpers ──

fn lock_required_minor(
    side: OrderSide,
    price_ticks: i64,
    quantity_minor: i64,
) -> Result<i64, FluidError> {
    match side {
        OrderSide::Buy => price_ticks
            .checked_mul(quantity_minor)
            .ok_or_else(|| FluidError::Overflow("buy lock overflow".to_string())),
        OrderSide::Sell => Ok(quantity_minor),
    }
}

fn cash_delta_minor(
    side: OrderSide,
    price_ticks: i64,
    quantity_minor: i64,
) -> Result<i64, FluidError> {
    let gross = price_ticks
        .checked_mul(quantity_minor)
        .ok_or_else(|| FluidError::Overflow("cash delta overflow".to_string()))?;
    match side {
        OrderSide::Buy => gross
            .checked_neg()
            .ok_or_else(|| FluidError::Overflow("cash delta neg overflow".to_string())),
        OrderSide::Sell => Ok(gross),
    }
}

fn compute_order_status(quantity_minor: i64, remaining_minor: i64) -> OrderStatus {
    if remaining_minor == quantity_minor {
        OrderStatus::Open
    } else if remaining_minor == 0 {
        OrderStatus::Filled
    } else {
        OrderStatus::PartiallyFilled
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_arguments,
    clippy::panic
)]
mod tests {
    use super::*;
    use crate::book::RestingEntry;
    use crate::risk::AccountState;
    use pebble_trading::{OrderNonce, TimeInForce};
    use std::collections::BTreeMap;
    use tokio::sync::oneshot;

    #[derive(Default)]
    struct TestIds {
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    }

    fn test_meta() -> MarketMeta {
        MarketMeta {
            status: "Open".to_string(),
            outcomes: vec!["YES".to_string(), "NO".to_string()],
            binary_config: Some(BinaryConfig {
                yes_outcome: "YES".to_string(),
                no_outcome: "NO".to_string(),
                max_price_ticks: 10_000,
            }),
            instrument_admin: "admin".to_string(),
            instrument_id: "USD".to_string(),
        }
    }

    fn test_account(cleared: i64) -> AccountState {
        AccountState {
            cleared_cash_minor: cleared,
            delta_pending_trades_minor: 0,
            locked_open_orders_minor: 0,
            pending_withdrawals_reserved_minor: 0,
            pending_nonce: None,
            pending_lock_minor: 0,
            last_nonce: None,
            status: "Active".to_string(),
            active: true,
            instrument_admin: "admin".to_string(),
            instrument_id: "USD".to_string(),
        }
    }

    fn make_incoming(
        id: &str,
        account: &str,
        market: &str,
        outcome: &str,
        side: OrderSide,
        price: i64,
        qty: i64,
        nonce: i64,
    ) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId(id.to_string()),
            account_id: account.to_string(),
            market_id: MarketId(market.to_string()),
            outcome: outcome.to_string(),
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            nonce: OrderNonce(nonce),
            price_ticks: price,
            quantity_minor: qty,
            submitted_at_micros: 1000,
        }
    }

    fn build_engine(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        meta: HashMap<MarketId, MarketMeta>,
    ) -> FluidState {
        build_engine_with_interners(
            books,
            risk,
            meta,
            StringInterner::default(),
            StringInterner::default(),
        )
    }

    fn build_engine_with_interners(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    ) -> FluidState {
        FluidState::new(
            books,
            risk,
            HashMap::new(),
            meta,
            order_interner,
            account_interner,
            "test-v1".to_string(),
            false,
        )
    }

    fn build_exchange_engine(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        meta: HashMap<MarketId, MarketMeta>,
    ) -> ExchangeEngine {
        ExchangeEngine::new(
            books,
            RiskActorHandle::spawn(risk, 16),
            HashMap::new(),
            meta,
            StringInterner::default(),
            StringInterner::default(),
            "test-v1".to_string(),
            false,
        )
    }

    fn insert_resting(
        ids: &mut TestIds,
        book: &mut MarketBook,
        order_id: &str,
        account_id: &str,
        outcome: &str,
        side: OrderSide,
        price_ticks: i64,
        remaining_minor: i64,
        locked_minor: i64,
        submitted_at_micros: i64,
    ) {
        book.insert(RestingEntry {
            order_id: ids.order_interner.intern(order_id),
            account_id: ids.account_interner.intern(account_id),
            outcome: outcome.to_string(),
            side,
            price_ticks,
            remaining_minor,
            locked_minor,
            submitted_at_micros,
        });
    }

    fn new_binary_book_from_meta(meta: &MarketMeta) -> MarketBook {
        if let Some(binary_config) = &meta.binary_config {
            MarketBook::new_binary(binary_config.clone())
        } else {
            MarketBook::new_multi_outcome()
        }
    }

    #[test]
    fn test_compute_does_not_mutate_state() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let transition = engine.compute_place_order(&incoming).unwrap();

        // State should not have changed
        assert_eq!(
            engine
                .risk()
                .get_account("buyer")
                .unwrap()
                .locked_open_orders_minor,
            0
        );
        assert!(engine.books().get(&market_id).is_none());

        // But transition should have values
        assert_eq!(transition.taker_remaining, 10);
        assert_eq!(transition.taker_locked, 500); // 50 * 10
        assert_eq!(transition.taker_status, OrderStatus::Open);
    }

    #[test]
    fn test_apply_after_compute_updates_state() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let transition = engine.compute_place_order(&incoming).unwrap();
        engine.apply_place_order(transition).unwrap();

        // Now state should reflect the order
        assert_eq!(
            engine
                .risk()
                .get_account("buyer")
                .unwrap()
                .locked_open_orders_minor,
            500
        );
        let book = engine.books().get(&market_id).unwrap();
        assert_eq!(book.order_count(), 1);
        assert_eq!(
            engine.risk().get_account("buyer").unwrap().last_nonce,
            Some(0)
        );
    }

    #[test]
    fn test_compute_without_apply_leaves_state_clean() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let engine = build_engine(HashMap::new(), risk, meta);

        // Compute but don't apply (simulates commit failure)
        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let _transition = engine.compute_place_order(&incoming).unwrap();

        // State unchanged
        assert_eq!(
            engine
                .risk()
                .get_account("buyer")
                .unwrap()
                .locked_open_orders_minor,
            0
        );
        assert!(engine.books().get(&market_id).is_none());
    }

    #[test]
    fn test_unhealthy_blocks_all_computes() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);
        engine.mark_unhealthy("test".to_string());

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        assert!(matches!(
            engine.compute_place_order(&incoming),
            Err(FluidError::EngineUnhealthy(_))
        ));
    }

    #[test]
    fn test_resync_restores_healthy() {
        let mut engine = build_engine(HashMap::new(), RiskEngine::default(), HashMap::new());
        engine.mark_unhealthy("test".to_string());
        assert!(!engine.is_healthy());

        engine.resync_from(
            HashMap::new(),
            RiskEngine::default(),
            HashMap::new(),
            HashMap::new(),
            StringInterner::default(),
            StringInterner::default(),
        );
        assert!(engine.is_healthy());
    }

    #[test]
    fn test_place_limit_order_no_match_rests_in_book() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let t = engine.compute_place_order(&incoming).unwrap();
        assert!(t.fills.is_empty());
        assert_eq!(t.taker_status, OrderStatus::Open);
        assert!(t.book_transition.taker_insertion.is_some());

        engine.apply_place_order(t).unwrap();
        assert_eq!(engine.books().get(&market_id).unwrap().order_count(), 1);
    }

    #[test]
    fn test_place_order_matches_and_fills() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut seller_acc = test_account(10_000);
        seller_acc.locked_open_orders_minor = 10; // matches resting sell order
        risk.upsert_account("seller", seller_acc);
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut books = HashMap::new();
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        insert_resting(
            &mut ids,
            &mut book,
            "sell-1",
            "seller",
            "YES",
            OrderSide::Sell,
            50,
            10,
            10,
            100,
        );
        books.insert(market_id.clone(), book);

        let mut engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        // Place buy order that crosses the ask
        let incoming = make_incoming("buy-1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let t = engine.compute_place_order(&incoming).unwrap();

        assert_eq!(t.fills.len(), 1);
        assert_eq!(t.taker_status, OrderStatus::Filled);
        assert_eq!(t.taker_remaining, 0);
        assert_eq!(t.taker_locked, 0);
        // Maker should be fully filled
        assert_eq!(t.book_transition.maker_removals.len(), 1);

        engine.apply_place_order(t).unwrap();
        // Book should be empty
        assert_eq!(engine.books().get(&market_id).unwrap().order_count(), 0);

        // Risk deltas applied
        let buyer = engine.risk().get_account("buyer").unwrap();
        assert_eq!(buyer.delta_pending_trades_minor, -500); // bought 10 @ 50
        assert_eq!(buyer.locked_open_orders_minor, 0);

        let seller = engine.risk().get_account("seller").unwrap();
        assert_eq!(seller.delta_pending_trades_minor, 500); // sold 10 @ 50
        assert_eq!(seller.locked_open_orders_minor, 0); // was 10, released 10
    }

    #[test]
    fn test_place_order_insufficient_balance_rejected() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(100)); // Only 100 available

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        let engine = build_engine(HashMap::new(), risk, meta);

        // Need 50 * 10 = 500 locked, but only have 100
        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        assert!(matches!(
            engine.compute_place_order(&incoming),
            Err(FluidError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn test_place_order_on_closed_market_rejected() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        let mut m = test_meta();
        m.status = "Resolved".to_string();
        meta.insert(market_id, m);

        let engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        assert!(matches!(
            engine.compute_place_order(&incoming),
            Err(FluidError::MarketNotOpen(_))
        ));
    }

    #[test]
    fn test_cancel_order_releases_locked() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        // Place then cancel
        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let t = engine.compute_place_order(&incoming).unwrap();
        engine.apply_place_order(t).unwrap();
        assert_eq!(
            engine
                .risk()
                .get_account("buyer")
                .unwrap()
                .locked_open_orders_minor,
            500
        );

        let cancel = engine
            .compute_cancel_order(
                &MarketId("m1".to_string()),
                &OrderId("o1".to_string()),
                "buyer",
            )
            .unwrap();
        assert_eq!(cancel.released_locked_minor, 500);

        engine.apply_cancel_order(cancel).unwrap();
        assert_eq!(
            engine
                .risk()
                .get_account("buyer")
                .unwrap()
                .locked_open_orders_minor,
            0
        );
        assert_eq!(engine.books().get(&market_id).unwrap().order_count(), 0);
    }

    #[test]
    fn test_cancel_wrong_account_unauthorized() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("owner", test_account(10_000));
        risk.upsert_account("other", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "owner", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let t = engine.compute_place_order(&incoming).unwrap();
        engine.apply_place_order(t).unwrap();

        assert!(matches!(
            engine.compute_cancel_order(
                &MarketId("m1".to_string()),
                &OrderId("o1".to_string()),
                "other",
            ),
            Err(FluidError::Unauthorized)
        ));
    }

    #[test]
    fn test_cancel_unknown_order_no_ghost_intern() {
        let market_id = MarketId("m1".to_string());
        let mut books = HashMap::new();
        books.insert(market_id.clone(), MarketBook::new_multi_outcome());

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let engine = build_engine(books, RiskEngine::default(), meta);
        let missing_order_id = OrderId("missing-order".to_string());

        assert_eq!(engine.order_interner().len(), 0);
        assert!(!engine.order_interner().contains(&missing_order_id.0));

        assert!(matches!(
            engine.compute_cancel_order(&market_id, &missing_order_id, "alice"),
            Err(FluidError::OrderNotFound(order_id)) if order_id == "missing-order"
        ));

        assert_eq!(engine.order_interner().len(), 0);
        assert!(!engine.order_interner().contains(&missing_order_id.0));
        assert!(engine
            .order_interner()
            .lookup(&missing_order_id.0)
            .is_none());
    }

    #[test]
    fn test_slot_reuse_no_stale_alias() {
        let market_a = MarketId("m1".to_string());
        let market_b = MarketId("m2".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("trader", test_account(100_000));

        let mut meta = HashMap::new();
        meta.insert(market_a.clone(), test_meta());
        meta.insert(market_b.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        let order_a = make_incoming("order-a", "trader", "m1", "YES", OrderSide::Buy, 50, 1, 0);
        let transition_a = engine.compute_place_order(&order_a).unwrap();
        engine.apply_place_order(transition_a).unwrap();
        let slot_a = engine.order_interner().lookup("order-a").unwrap();
        assert_eq!(engine.order_index.get(&slot_a), Some(&market_a));

        let cancel_a = engine
            .compute_cancel_order(&market_a, &OrderId("order-a".to_string()), "trader")
            .unwrap();
        engine.apply_cancel_order(cancel_a).unwrap();
        assert!(engine.order_interner().lookup("order-a").is_none());
        assert!(!engine.order_index.contains_key(&slot_a));

        let order_b = make_incoming("order-b", "trader", "m2", "YES", OrderSide::Buy, 60, 1, 1);
        let transition_b = engine.compute_place_order(&order_b).unwrap();
        engine.apply_place_order(transition_b).unwrap();

        let slot_b = engine.order_interner().lookup("order-b").unwrap();
        assert_eq!(slot_b, slot_a);
        assert!(engine.order_interner().lookup("order-a").is_none());
        assert_eq!(engine.order_index.get(&slot_b), Some(&market_b));
    }

    #[test]
    fn test_cross_market_risk_shared() {
        let m1 = MarketId("m1".to_string());
        let m2 = MarketId("m2".to_string());

        let mut risk = RiskEngine::default();
        risk.upsert_account("trader", test_account(1000));

        let mut meta = HashMap::new();
        meta.insert(m1.clone(), test_meta());
        meta.insert(m2.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        // Lock 500 in M1
        let o1 = make_incoming("o1", "trader", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        let t1 = engine.compute_place_order(&o1).unwrap();
        engine.apply_place_order(t1).unwrap();

        // Lock 400 in M2
        let o2 = make_incoming("o2", "trader", "m2", "YES", OrderSide::Buy, 40, 10, 1);
        let t2 = engine.compute_place_order(&o2).unwrap();
        engine.apply_place_order(t2).unwrap();

        // Available = 1000 - 900 = 100, try to lock 200 → should fail
        let o3 = make_incoming("o3", "trader", "m1", "NO", OrderSide::Buy, 20, 10, 2);
        assert!(matches!(
            engine.compute_place_order(&o3),
            Err(FluidError::InsufficientBalance {
                available: 100,
                required: 200
            })
        ));
    }

    #[tokio::test]
    async fn test_exchange_markets_lock_independently() {
        let m1 = MarketId("m1".to_string());
        let m2 = MarketId("m2".to_string());

        let mut meta = HashMap::new();
        meta.insert(m1.clone(), test_meta());
        meta.insert(m2.clone(), test_meta());

        let exchange = build_exchange_engine(HashMap::new(), RiskEngine::default(), meta);

        let market1 = exchange.get_market(&m1).await.unwrap();
        let market2 = exchange.get_market(&m2).await.unwrap();

        let hold = std::time::Duration::from_millis(120);
        let start = std::time::Instant::now();
        let (enter1_tx, enter1_rx) = oneshot::channel::<std::time::Instant>();
        let (enter2_tx, enter2_rx) = oneshot::channel::<std::time::Instant>();

        let handle1 = tokio::spawn(async move {
            let _guard = market1.lock().await;
            enter1_tx.send(std::time::Instant::now()).ok();
            tokio::time::sleep(hold).await;
        });
        let handle2 = tokio::spawn(async move {
            let _guard = market2.lock().await;
            enter2_tx.send(std::time::Instant::now()).ok();
            tokio::time::sleep(hold).await;
        });

        let enter1 = enter1_rx.await.unwrap();
        let enter2 = enter2_rx.await.unwrap();

        let (first_join, second_join) = tokio::join!(handle1, handle2);
        assert!(first_join.is_ok(), "market1 lock task panicked");
        assert!(second_join.is_ok(), "market2 lock task panicked");

        let inter_market_gap = if enter1 <= enter2 {
            enter2.duration_since(enter1)
        } else {
            enter1.duration_since(enter2)
        };
        assert!(
            inter_market_gap < hold,
            "locks appear serialized: gap={inter_market_gap:?}, threshold={hold:?}"
        );
        assert!(
            start.elapsed() < hold.saturating_mul(2),
            "unexpectedly long lock path for independent markets: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn test_same_account_nonce_pending_across_markets() {
        let m1 = MarketId("m1".to_string());
        let m2 = MarketId("m2".to_string());

        let mut risk = RiskEngine::default();
        risk.upsert_account("trader", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(m1.clone(), test_meta());
        meta.insert(m2.clone(), test_meta());

        let exchange = build_exchange_engine(HashMap::new(), risk, meta);
        let actor = exchange.risk_handle().clone();
        let actor1 = actor.clone();
        let actor2 = actor.clone();

        let market1 = exchange.get_market(&m1).await.unwrap();
        let market2 = exchange.get_market(&m2).await.unwrap();

        let (reserved_tx, reserved_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let first = tokio::spawn(async move {
            let _market1 = market1.lock().await;
            actor1
                .reserve("trader", 100, 0, "admin", "USD", false)
                .await
                .unwrap();
            reserved_tx.send(()).ok();
            release_rx.await.ok();
            actor1.abort("trader", 100).await
        });

        reserved_rx.await.unwrap();

        let second = tokio::spawn(async move {
            let _market2 = market2.lock().await;
            actor2
                .reserve("trader", 100, 0, "admin", "USD", false)
                .await
                .unwrap_err()
        });

        let second_result = second.await.unwrap();
        release_tx.send(()).ok();
        let first_result = first.await.unwrap();
        assert!(first_result.is_ok(), "first abort should succeed");

        assert!(matches!(
            second_result,
            FluidError::NoncePending {
                account_id: _,
                pending_nonce: 0
            }
        ));
    }

    #[test]
    fn test_instrument_mismatch_rejected() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut acc = test_account(10_000);
        acc.instrument_id = "EUR".to_string(); // Mismatch: market is USD
        risk.upsert_account("buyer", acc);

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        let engine = build_engine(HashMap::new(), risk, meta);

        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        assert!(matches!(
            engine.compute_place_order(&incoming),
            Err(FluidError::InstrumentMismatch)
        ));
    }

    #[test]
    fn test_withdrawal_reserves_toggle() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut acc = test_account(1000);
        acc.pending_withdrawals_reserved_minor = 800;
        risk.upsert_account("buyer", acc);

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        // Toggle OFF: withdrawal reserves not subtracted, available = 1000
        let engine_off = FluidState::new(
            HashMap::new(),
            risk.clone(),
            HashMap::new(),
            meta.clone(),
            StringInterner::default(),
            StringInterner::default(),
            "test-v1".to_string(),
            false,
        );
        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 0);
        // Need 500, have 1000 → ok
        assert!(engine_off.compute_place_order(&incoming).is_ok());

        // Toggle ON: withdrawal reserves subtracted, available = 1000 - 800 = 200
        let engine_on = FluidState::new(
            HashMap::new(),
            risk,
            HashMap::new(),
            meta,
            StringInterner::default(),
            StringInterner::default(),
            "test-v1".to_string(),
            true,
        );
        // Need 500, have 200 → rejected
        assert!(matches!(
            engine_on.compute_place_order(&incoming),
            Err(FluidError::InsufficientBalance {
                available: 200,
                required: 500
            })
        ));
    }

    #[test]
    fn test_nonce_mismatch_rejected() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        let engine = build_engine(HashMap::new(), risk, meta);

        // First nonce must be 0, not 5
        let incoming = make_incoming("o1", "buyer", "m1", "YES", OrderSide::Buy, 50, 10, 5);
        assert!(matches!(
            engine.compute_place_order(&incoming),
            Err(FluidError::NonceMismatch {
                expected: 0,
                got: 5
            })
        ));
    }

    #[test]
    fn test_partial_fill_maker_stays_in_book() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut seller_acc = test_account(10_000);
        seller_acc.locked_open_orders_minor = 20; // matches resting sell order
        risk.upsert_account("seller", seller_acc);
        risk.upsert_account("buyer", test_account(10_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut books = HashMap::new();
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        insert_resting(
            &mut ids,
            &mut book,
            "sell-1",
            "seller",
            "YES",
            OrderSide::Sell,
            50,
            20,
            20,
            100,
        );
        books.insert(market_id.clone(), book);

        let mut engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        // Buy only 5, maker has 20
        let incoming = make_incoming("buy-1", "buyer", "m1", "YES", OrderSide::Buy, 50, 5, 0);
        let t = engine.compute_place_order(&incoming).unwrap();

        assert_eq!(t.fills.len(), 1);
        assert_eq!(t.fills[0].quantity_minor, 5);
        assert_eq!(t.taker_status, OrderStatus::Filled);
        assert_eq!(t.book_transition.maker_remaining_updates.len(), 1);
        assert_eq!(t.book_transition.maker_remaining_updates[0].1, 15); // 20 - 5
        assert!(t.book_transition.maker_removals.is_empty());

        engine.apply_place_order(t).unwrap();
        let sell_order_internal_id = engine.order_interner().lookup("sell-1").unwrap();
        let entry = engine
            .books()
            .get(&market_id)
            .unwrap()
            .get_entry(sell_order_internal_id)
            .unwrap();
        assert_eq!(entry.remaining_minor, 15);
        assert_eq!(entry.locked_minor, 15); // sell locked = remaining
    }

    // ── Property / equivalence tests ──

    /// Build an OrderBookView directly from resting entries (bypassing FluidState)
    /// and match via match_order_presorted. Compare fills with FluidState's compute.
    #[test]
    fn test_equivalence_with_trading_crate() {
        use pebble_trading::{OrderBookView, RestingOrder};

        // Set up a book with 3 asks at different prices
        let mut ids = TestIds::default();
        let asks: Vec<(RestingEntry, RestingOrder)> = vec![
            ("s1", "seller1", 40, 30, 100),
            ("s2", "seller2", 50, 20, 200),
            ("s3", "seller3", 60, 10, 300),
        ]
        .into_iter()
        .map(|(id, acc, price, qty, time)| {
            let entry = RestingEntry {
                order_id: ids.order_interner.intern(id),
                account_id: ids.account_interner.intern(acc),
                outcome: "YES".to_string(),
                side: OrderSide::Sell,
                price_ticks: price,
                remaining_minor: qty,
                locked_minor: qty,
                submitted_at_micros: time,
            };
            let resting = RestingOrder {
                order_id: OrderId(id.to_string()),
                account_id: acc.to_string(),
                side: OrderSide::Sell,
                outcome: "YES".to_string(),
                price_ticks: price,
                remaining_minor: qty,
                submitted_at_micros: time,
            };
            (entry, resting)
        })
        .collect();

        // Build FluidState with the book
        let market_id = MarketId("m1".to_string());
        let mut book = MarketBook::new_multi_outcome();
        let mut total_seller_locked: HashMap<String, i64> = HashMap::new();
        for (entry, _) in &asks {
            let account_id = ids
                .account_interner
                .to_external(entry.account_id)
                .expect("account mapping must exist")
                .to_string();
            *total_seller_locked.entry(account_id).or_default() += entry.locked_minor;
            book.insert(entry.clone());
        }
        let mut books = HashMap::new();
        books.insert(market_id.clone(), book);

        let mut risk = RiskEngine::default();
        for (acc, locked) in &total_seller_locked {
            let mut state = test_account(100_000);
            state.locked_open_orders_minor = *locked;
            risk.upsert_account(acc, state);
        }
        risk.upsert_account("buyer", test_account(100_000));

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());

        let engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        // Incoming buy that should cross the two cheapest asks
        let incoming = make_incoming("b1", "buyer", "m1", "YES", OrderSide::Buy, 55, 45, 0);
        let transition = engine.compute_place_order(&incoming).unwrap();

        // Build equivalent OrderBookView and match directly
        let direct_book = OrderBookView {
            bids: Vec::new(),
            asks: asks.iter().map(|(_, r)| r.clone()).collect(),
        };
        let direct_result =
            pebble_trading::match_order_presorted(direct_book, &incoming, "test-v1", 0).unwrap();

        // Fills must be identical
        assert_eq!(transition.fills.len(), direct_result.fills.len());
        for (fluid_fill, direct_fill) in transition.fills.iter().zip(direct_result.fills.iter()) {
            assert_eq!(fluid_fill.maker_order_id, direct_fill.maker_order_id);
            assert_eq!(fluid_fill.taker_order_id, direct_fill.taker_order_id);
            assert_eq!(fluid_fill.price_ticks, direct_fill.price_ticks);
            assert_eq!(fluid_fill.quantity_minor, direct_fill.quantity_minor);
        }

        // Remaining must be identical
        assert_eq!(
            transition.taker_remaining,
            direct_result.incoming_remaining_minor
        );
    }

    /// Run a sequence of random-ish orders through compute+apply and verify
    /// invariants hold after every step.
    #[test]
    fn test_compute_apply_preserves_invariants() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("alice", test_account(1_000_000));
        risk.upsert_account("bob", test_account(1_000_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        // A deterministic sequence of orders that exercises various paths:
        // - Resting orders (no contra side)
        // - Partial fills
        // - Full fills
        // - Cross-market (would need second market, but we verify single-market invariants)
        let orders = vec![
            // Alice sells 100 @ 60 (rests)
            ("o1", "alice", OrderSide::Sell, 60, 100, 0),
            // Alice sells 50 @ 50 (rests)
            ("o2", "alice", OrderSide::Sell, 50, 50, 1),
            // Bob buys 30 @ 55 — fills 30 of o2 (partial fill)
            ("o3", "bob", OrderSide::Buy, 55, 30, 0),
            // Bob buys 25 @ 65 — fills remaining 20 of o2 + 5 of o1
            ("o4", "bob", OrderSide::Buy, 65, 25, 1),
            // Bob buys 10 @ 40 — no match, rests
            ("o5", "bob", OrderSide::Buy, 40, 10, 2),
            // Alice sells 200 @ 45 — fills 10 of o5, rests 190
            ("o6", "alice", OrderSide::Sell, 45, 200, 2),
        ];

        let mut total_fills = 0i64;

        for (id, account, side, price, qty, nonce) in &orders {
            let incoming = make_incoming(id, account, "m1", "YES", *side, *price, *qty, *nonce);

            // Set seller locked to match their resting orders
            let result = engine.compute_place_order(&incoming);
            match result {
                Ok(transition) => {
                    total_fills += transition.fills.len() as i64;
                    engine.apply_place_order(transition).unwrap();
                }
                Err(e) => {
                    // If the order fails, that's fine — invariants should still hold
                    // (e.g., insufficient balance)
                    panic!("unexpected error for order {id}: {e}");
                }
            }

            // Invariant: locked >= 0 for all accounts
            for acc_id in &["alice", "bob"] {
                let acc = engine.risk().get_account(acc_id).unwrap();
                assert!(
                    acc.locked_open_orders_minor >= 0,
                    "locked negative for {acc_id}: {}",
                    acc.locked_open_orders_minor
                );
            }

            // Invariant: available = cleared + delta - locked, should be >= 0
            // (since we gave each account 1M and orders are small)
            for acc_id in &["alice", "bob"] {
                let avail = engine.risk().available_minor(acc_id).unwrap();
                assert!(avail >= 0, "available negative for {acc_id}: {avail}");
            }
        }

        // Final invariant: total locked across accounts == sum of lock values in book
        let book = engine.books().get(&MarketId("m1".to_string()));
        let mut book_locked_sum = 0i64;
        if let Some(b) = book {
            for acc_id in &["alice", "bob"] {
                // Sum locked for all orders belonging to this account
                // We need to walk the book — use get_entry for known order IDs
                for oid in &["o1", "o2", "o3", "o4", "o5", "o6"] {
                    let order_id = OrderId((*oid).to_string());
                    if let Some(entry) =
                        b.get_entry_by_external_order_id(&order_id, engine.order_interner())
                    {
                        if engine.account_interner().to_external(entry.account_id) == Some(*acc_id)
                        {
                            book_locked_sum += entry.locked_minor;
                        }
                    }
                }
            }
        }

        let risk_locked_sum: i64 = ["alice", "bob"]
            .iter()
            .map(|id| {
                engine
                    .risk()
                    .get_account(id)
                    .unwrap()
                    .locked_open_orders_minor
            })
            .sum();

        assert_eq!(
            risk_locked_sum, book_locked_sum,
            "risk locked sum ({risk_locked_sum}) != book locked sum ({book_locked_sum})"
        );

        // Sequence counters should have advanced
        let seq = engine.sequences().get(&MarketId("m1".to_string())).unwrap();
        assert_eq!(seq.next_fill_sequence, total_fills);
    }

    #[test]
    fn test_random_binary_order_stream_property() {
        let market_id = MarketId("m1".to_string());
        let accounts = ["alice", "bob", "carol", "dave"];

        let mut risk = RiskEngine::default();
        for account_id in accounts {
            risk.upsert_account(account_id, test_account(1_000_000));
        }

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());
        let mut engine = build_engine(HashMap::new(), risk, meta);

        let mut nonces = HashMap::from([
            ("alice".to_string(), 0_i64),
            ("bob".to_string(), 0_i64),
            ("carol".to_string(), 0_i64),
            ("dave".to_string(), 0_i64),
        ]);
        let mut active_orders: Vec<OrderId> = Vec::new();
        let mut positions = BTreeMap::<(String, String), i64>::new();

        fn next_rand(seed: &mut u64) -> u64 {
            *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *seed
        }

        fn signed_qty(side: OrderSide, qty: i64) -> i64 {
            if side == OrderSide::Buy {
                qty
            } else {
                -qty
            }
        }

        fn locked_sum_in_book(engine: &FluidState, market_id: &MarketId) -> i64 {
            let Some(book) = engine.books().get(market_id) else {
                return 0;
            };
            let mut total_locked = 0_i64;
            for order_id in book.order_ids() {
                if let Some(entry) = book.get_entry(order_id) {
                    total_locked = total_locked
                        .checked_add(entry.locked_minor)
                        .expect("book lock sum overflow");
                }
            }
            total_locked
        }

        let mut seed = 0x5EED_u64;

        // Force at least one cross-outcome match so the randomized loop verifies
        // both same-outcome and complement-outcome paths.
        let maker = make_incoming(
            "seed-maker-buy-no",
            "bob",
            "m1",
            "NO",
            OrderSide::Buy,
            4_000,
            5,
            0,
        );
        let maker_transition = engine.compute_place_order(&maker).expect("seed maker");
        active_orders.push(maker.order_id.clone());
        engine
            .apply_place_order(maker_transition)
            .expect("apply seed maker");
        *nonces.get_mut("bob").expect("bob nonce") = 1;

        let taker = make_incoming(
            "seed-taker-buy-yes",
            "alice",
            "m1",
            "YES",
            OrderSide::Buy,
            6_000,
            3,
            0,
        );
        let taker_transition = engine.compute_place_order(&taker).expect("seed taker");
        let mut saw_cross_outcome_fill = taker_transition
            .fills
            .iter()
            .any(|fill| fill.maker_outcome != fill.taker_outcome);
        for fill in &taker_transition.fills {
            *positions
                .entry((fill.maker_account_id.clone(), fill.maker_outcome.clone()))
                .or_default() += signed_qty(fill.maker_side, fill.quantity_minor);
            *positions
                .entry((fill.taker_account_id.clone(), fill.taker_outcome.clone()))
                .or_default() += signed_qty(fill.taker_side, fill.quantity_minor);
        }
        engine
            .apply_place_order(taker_transition)
            .expect("apply seed taker");
        *nonces.get_mut("alice").expect("alice nonce") = 1;

        for step in 0..500_i64 {
            let choose_cancel = next_rand(&mut seed) % 10;
            if choose_cancel >= 7 && !active_orders.is_empty() {
                let idx = usize::try_from(
                    next_rand(&mut seed) % u64::try_from(active_orders.len()).expect("len"),
                )
                .expect("index");
                let order_id = active_orders.remove(idx);
                let maybe_entry = engine.books().get(&market_id).and_then(|book| {
                    book.get_entry_by_external_order_id(&order_id, engine.order_interner())
                });
                let Some(entry) = maybe_entry else {
                    continue;
                };
                let account_id = engine
                    .account_interner()
                    .to_external(entry.account_id)
                    .expect("entry account mapping")
                    .to_string();
                let before_locked = engine
                    .risk()
                    .get_account(&account_id)
                    .expect("account")
                    .locked_open_orders_minor;
                if let Ok(transition) =
                    engine.compute_cancel_order(&market_id, &order_id, &account_id)
                {
                    let released = transition.released_locked_minor;
                    engine.apply_cancel_order(transition).expect("apply cancel");
                    let after_locked = engine
                        .risk()
                        .get_account(&account_id)
                        .expect("account")
                        .locked_open_orders_minor;
                    assert_eq!(before_locked - after_locked, released);
                }
            } else {
                let account_index = usize::try_from(next_rand(&mut seed) % 4).expect("account idx");
                let account_id = accounts[account_index];
                let nonce = *nonces.get(account_id).expect("nonce");
                let side = if next_rand(&mut seed).is_multiple_of(2) {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let outcome = if next_rand(&mut seed).is_multiple_of(2) {
                    "YES"
                } else {
                    "NO"
                };
                let order_type = if next_rand(&mut seed) % 10 < 8 {
                    OrderType::Limit
                } else {
                    OrderType::Market
                };
                let price_ticks = if order_type == OrderType::Limit {
                    1 + i64::try_from(next_rand(&mut seed) % 9_998).expect("price")
                } else {
                    0
                };
                let quantity_minor = 1 + i64::try_from(next_rand(&mut seed) % 20).expect("qty");

                let mut incoming = make_incoming(
                    &format!("prop-{step}-{}", next_rand(&mut seed) % 10_000),
                    account_id,
                    "m1",
                    outcome,
                    side,
                    price_ticks,
                    quantity_minor,
                    nonce,
                );
                incoming.order_type = order_type;
                incoming.tif = if order_type == OrderType::Limit {
                    TimeInForce::Gtc
                } else {
                    TimeInForce::Ioc
                };

                let before_accounts = accounts
                    .iter()
                    .map(|id| {
                        let account = engine.risk().get_account(id).expect("before account");
                        (
                            (*id).to_string(),
                            (
                                account.delta_pending_trades_minor,
                                account.locked_open_orders_minor,
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>();

                if let Ok(transition) = engine.compute_place_order(&incoming) {
                    let fills = transition.fills.clone();
                    let expected_deltas = transition
                        .risk_transition
                        .deltas
                        .iter()
                        .map(|delta| {
                            (
                                delta.account_id.clone(),
                                (delta.delta_pending_minor, delta.locked_delta_minor),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut pending_from_fills = BTreeMap::<String, i64>::new();

                    for fill in &fills {
                        assert_eq!(fill.outcome, fill.taker_outcome);
                        assert_eq!(fill.price_ticks, fill.taker_price_ticks);

                        let maker_pending = cash_delta_minor(
                            fill.maker_side,
                            fill.maker_price_ticks,
                            fill.quantity_minor,
                        )
                        .expect("maker cash delta");
                        let taker_pending = cash_delta_minor(
                            fill.taker_side,
                            fill.taker_price_ticks,
                            fill.quantity_minor,
                        )
                        .expect("taker cash delta");

                        *pending_from_fills
                            .entry(fill.maker_account_id.clone())
                            .or_default() += maker_pending;
                        *pending_from_fills
                            .entry(fill.taker_account_id.clone())
                            .or_default() += taker_pending;

                        if fill.maker_outcome == fill.taker_outcome {
                            let maker_position_delta =
                                signed_qty(fill.maker_side, fill.quantity_minor);
                            let taker_position_delta =
                                signed_qty(fill.taker_side, fill.quantity_minor);
                            assert_eq!(maker_position_delta, -taker_position_delta);
                        } else {
                            saw_cross_outcome_fill = true;
                            assert_eq!(fill.maker_side, fill.taker_side);
                            let complement_sum = fill
                                .maker_price_ticks
                                .checked_add(fill.taker_price_ticks)
                                .expect("complement sum overflow");
                            assert_eq!(complement_sum, 10_000);
                        }

                        *positions
                            .entry((fill.maker_account_id.clone(), fill.maker_outcome.clone()))
                            .or_default() += signed_qty(fill.maker_side, fill.quantity_minor);
                        *positions
                            .entry((fill.taker_account_id.clone(), fill.taker_outcome.clone()))
                            .or_default() += signed_qty(fill.taker_side, fill.quantity_minor);
                    }

                    for (account_id, pending_delta) in &pending_from_fills {
                        let expected_pending =
                            expected_deltas.get(account_id).map_or(0, |delta| delta.0);
                        assert_eq!(*pending_delta, expected_pending);
                    }

                    let should_rest =
                        incoming.order_type == OrderType::Limit && transition.taker_remaining > 0;
                    let taker_order_id = incoming.order_id.clone();

                    engine
                        .apply_place_order(transition)
                        .expect("apply transition");
                    *nonces.get_mut(account_id).expect("nonce entry") = nonce + 1;
                    if should_rest {
                        active_orders.push(taker_order_id);
                    }

                    for id in accounts {
                        let before = before_accounts.get(id).expect("before");
                        let after = engine.risk().get_account(id).expect("after");
                        let expected = expected_deltas.get(id).copied().unwrap_or((0, 0));
                        assert_eq!(after.delta_pending_trades_minor - before.0, expected.0);
                        assert_eq!(after.locked_open_orders_minor - before.1, expected.1);
                        assert!(after.locked_open_orders_minor >= 0);
                        assert!(engine.risk().available_minor(id).expect("available") >= 0);
                    }
                }
            }

            let risk_locked_sum: i64 = accounts
                .iter()
                .map(|id| {
                    engine
                        .risk()
                        .get_account(id)
                        .expect("risk account")
                        .locked_open_orders_minor
                })
                .sum();
            let book_locked_sum = locked_sum_in_book(&engine, &market_id);
            assert_eq!(risk_locked_sum, book_locked_sum);
        }

        assert!(saw_cross_outcome_fill);
        assert!(!positions.is_empty());
    }

    /// Compute multiple transitions but only apply some — verify that
    /// unapplied transitions leave state consistent and that the applied
    /// transitions produce correct state.
    #[test]
    fn test_partial_apply_leaves_consistent_state() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("alice", test_account(100_000));
        risk.upsert_account("bob", test_account(100_000));

        let mut meta = HashMap::new();
        meta.insert(market_id.clone(), test_meta());

        let mut engine = build_engine(HashMap::new(), risk, meta);

        // Compute transition 1: Alice sells 50 @ 60 (rests)
        let o1 = make_incoming("o1", "alice", "m1", "YES", OrderSide::Sell, 60, 50, 0);
        let t1 = engine.compute_place_order(&o1).unwrap();

        // Snapshot state before apply
        let alice_locked_before = engine
            .risk()
            .get_account("alice")
            .unwrap()
            .locked_open_orders_minor;

        // Apply t1
        engine.apply_place_order(t1).unwrap();
        assert_eq!(
            engine
                .risk()
                .get_account("alice")
                .unwrap()
                .locked_open_orders_minor,
            alice_locked_before + 50
        );

        // Compute transition 2: Bob buys 30 @ 65 (crosses Alice's ask)
        let o2 = make_incoming("o2", "bob", "m1", "YES", OrderSide::Buy, 65, 30, 0);
        let t2 = engine.compute_place_order(&o2).unwrap();
        assert_eq!(t2.fills.len(), 1);

        // DO NOT apply t2 — simulate commit failure
        // State should reflect only t1

        // Alice still has her full resting order
        let book = engine.books().get(&market_id).unwrap();
        assert_eq!(book.order_count(), 1);
        let entry = book
            .get_entry_by_external_order_id(&OrderId("o1".to_string()), engine.order_interner())
            .unwrap();
        assert_eq!(entry.remaining_minor, 50); // unchanged

        // Bob has no risk changes
        let bob = engine.risk().get_account("bob").unwrap();
        assert_eq!(bob.locked_open_orders_minor, 0);
        assert_eq!(bob.delta_pending_trades_minor, 0);

        // Now compute and apply transition 3: Bob buys 20 @ 65
        // This should work correctly against the unmodified book state
        let o3 = make_incoming("o3", "bob", "m1", "YES", OrderSide::Buy, 65, 20, 0);
        let t3 = engine.compute_place_order(&o3).unwrap();
        assert_eq!(t3.fills.len(), 1);
        assert_eq!(t3.fills[0].quantity_minor, 20);

        engine.apply_place_order(t3).unwrap();

        // Alice's order partially filled: 50 - 20 = 30 remaining
        let entry = engine
            .books()
            .get(&market_id)
            .unwrap()
            .get_entry_by_external_order_id(&OrderId("o1".to_string()), engine.order_interner())
            .unwrap();
        assert_eq!(entry.remaining_minor, 30);

        // Risk invariants hold
        let alice = engine.risk().get_account("alice").unwrap();
        assert!(alice.locked_open_orders_minor >= 0);
        assert_eq!(alice.locked_open_orders_minor, 30); // sell locked = remaining

        let bob = engine.risk().get_account("bob").unwrap();
        assert!(bob.locked_open_orders_minor >= 0);
        assert_eq!(bob.delta_pending_trades_minor, -(60 * 20)); // paid 60 * 20
    }

    #[test]
    fn test_price_validation_binary_limit() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(100_000));

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());
        let engine = build_engine(HashMap::new(), risk, meta);

        let zero_price = make_incoming("o-zero", "buyer", "m1", "YES", OrderSide::Buy, 0, 1, 0);
        assert!(matches!(
            engine.compute_place_order(&zero_price),
            Err(FluidError::MatchFailed(message))
                if message.contains("1 <= price_ticks < 10000")
        ));

        let max_price = make_incoming("o-max", "buyer", "m1", "YES", OrderSide::Buy, 10_000, 1, 1);
        assert!(matches!(
            engine.compute_place_order(&max_price),
            Err(FluidError::MatchFailed(message))
                if message.contains("1 <= price_ticks < 10000")
        ));
    }

    #[test]
    fn test_price_validation_market_unchanged() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        risk.upsert_account("buyer", test_account(100_000));

        let mut meta = HashMap::new();
        meta.insert(market_id, test_meta());
        let engine = build_engine(HashMap::new(), risk, meta);

        let incoming = IncomingOrder {
            order_id: OrderId("market-buy".to_string()),
            account_id: "buyer".to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: "NO".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            tif: TimeInForce::Ioc,
            nonce: OrderNonce(0),
            price_ticks: 0,
            quantity_minor: 10,
            submitted_at_micros: 1,
        };

        let transition = engine.compute_place_order(&incoming).unwrap();
        assert_eq!(transition.taker_locked, 0);
        assert_eq!(transition.taker_remaining, 0);
        assert_eq!(transition.taker_status, OrderStatus::Cancelled);
    }

    #[test]
    fn test_cross_outcome_cash_delta_correctness() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut maker = test_account(100_000);
        maker.locked_open_orders_minor = 8_000;
        risk.upsert_account("maker", maker);
        risk.upsert_account("taker", test_account(100_000));

        let mut meta = HashMap::new();
        let market_meta = test_meta();
        meta.insert(market_id.clone(), market_meta.clone());

        let mut ids = TestIds::default();
        let mut book = new_binary_book_from_meta(&market_meta);
        insert_resting(
            &mut ids,
            &mut book,
            "maker-buy-no",
            "maker",
            "NO",
            OrderSide::Buy,
            4_000,
            2,
            8_000,
            1,
        );

        let mut books = HashMap::new();
        books.insert(market_id.clone(), book);

        let mut engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        let incoming = make_incoming(
            "taker-buy-yes",
            "taker",
            "m1",
            "YES",
            OrderSide::Buy,
            6_000,
            2,
            0,
        );
        let transition = engine.compute_place_order(&incoming).unwrap();
        assert_eq!(transition.fills.len(), 1);
        assert_eq!(transition.fills[0].maker_price_ticks, 4_000);
        assert_eq!(transition.fills[0].taker_price_ticks, 6_000);

        engine.apply_place_order(transition).unwrap();

        let maker_after = engine.risk().get_account("maker").unwrap();
        assert_eq!(maker_after.delta_pending_trades_minor, -8_000);
        assert_eq!(maker_after.locked_open_orders_minor, 0);

        let taker_after = engine.risk().get_account("taker").unwrap();
        assert_eq!(taker_after.delta_pending_trades_minor, -12_000);
        assert_eq!(taker_after.locked_open_orders_minor, 0);
    }

    #[test]
    fn test_cross_outcome_lock_correctness() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut maker = test_account(100_000);
        maker.locked_open_orders_minor = 40_000;
        risk.upsert_account("maker", maker);
        risk.upsert_account("taker", test_account(100_000));

        let mut meta = HashMap::new();
        let market_meta = test_meta();
        meta.insert(market_id.clone(), market_meta.clone());

        let mut ids = TestIds::default();
        let mut book = new_binary_book_from_meta(&market_meta);
        insert_resting(
            &mut ids,
            &mut book,
            "maker-buy-no",
            "maker",
            "NO",
            OrderSide::Buy,
            4_000,
            10,
            40_000,
            1,
        );

        let mut books = HashMap::new();
        books.insert(market_id.clone(), book);
        let mut engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        let incoming = make_incoming(
            "taker-buy-yes",
            "taker",
            "m1",
            "YES",
            OrderSide::Buy,
            6_000,
            4,
            0,
        );
        let transition = engine.compute_place_order(&incoming).unwrap();
        assert_eq!(transition.book_transition.maker_remaining_updates.len(), 1);
        assert_eq!(transition.book_transition.maker_remaining_updates[0].1, 6);
        assert_eq!(
            transition.book_transition.maker_remaining_updates[0].2,
            24_000
        );

        engine.apply_place_order(transition).unwrap();
        let maker_after = engine.risk().get_account("maker").unwrap();
        assert_eq!(maker_after.locked_open_orders_minor, 24_000);
        assert_eq!(maker_after.delta_pending_trades_minor, -16_000);

        let entry = engine
            .books()
            .get(&market_id)
            .unwrap()
            .get_entry_by_external_order_id(
                &OrderId("maker-buy-no".to_string()),
                engine.order_interner(),
            )
            .unwrap();
        assert_eq!(entry.remaining_minor, 6);
        assert_eq!(entry.locked_minor, 24_000);
    }

    #[test]
    fn test_cross_outcome_market_buy_affordability() {
        let market_id = MarketId("m1".to_string());
        let mut risk = RiskEngine::default();
        let mut maker = test_account(100_000);
        maker.locked_open_orders_minor = 7_000;
        risk.upsert_account("maker", maker);
        risk.upsert_account("buyer", test_account(3_000));

        let mut meta = HashMap::new();
        let market_meta = test_meta();
        meta.insert(market_id.clone(), market_meta.clone());

        let mut ids = TestIds::default();
        let mut book = new_binary_book_from_meta(&market_meta);
        insert_resting(
            &mut ids,
            &mut book,
            "maker-buy-yes",
            "maker",
            "YES",
            OrderSide::Buy,
            7_000,
            1,
            7_000,
            1,
        );

        let mut books = HashMap::new();
        books.insert(market_id, book);
        let mut engine = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );

        let incoming = IncomingOrder {
            order_id: OrderId("buyer-buy-no-market".to_string()),
            account_id: "buyer".to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: "NO".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            tif: TimeInForce::Ioc,
            nonce: OrderNonce(0),
            price_ticks: 0,
            quantity_minor: 1,
            submitted_at_micros: 1,
        };

        let transition = engine.compute_place_order(&incoming).unwrap();
        assert_eq!(transition.fills.len(), 1);
        assert_eq!(transition.fills[0].canonical_price_ticks, 7_000);
        assert_eq!(transition.fills[0].taker_price_ticks, 3_000);
        assert_eq!(transition.fills[0].maker_price_ticks, 7_000);

        engine.apply_place_order(transition).unwrap();

        let buyer = engine.risk().get_account("buyer").unwrap();
        assert_eq!(buyer.delta_pending_trades_minor, -3_000);
    }

    #[test]
    fn test_compute_place_order_replay_skips_market_buy_affordability_check() {
        let mut risk = RiskEngine::default();
        let mut maker = test_account(100_000);
        maker.locked_open_orders_minor = 1;
        risk.upsert_account("maker", maker);
        risk.upsert_account("buyer", test_account(5));

        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        insert_resting(
            &mut ids,
            &mut book,
            "ask-1",
            "maker",
            "YES",
            OrderSide::Sell,
            100,
            1,
            1,
            1,
        );

        let mut books = HashMap::new();
        books.insert(MarketId("m1".to_string()), book);

        let mut meta = HashMap::new();
        meta.insert(MarketId("m1".to_string()), test_meta());

        let state = build_engine_with_interners(
            books,
            risk,
            meta,
            ids.order_interner,
            ids.account_interner,
        );
        let incoming = IncomingOrder {
            order_id: OrderId("buy-1".to_string()),
            account_id: "buyer".to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            tif: TimeInForce::Ioc,
            nonce: pebble_trading::OrderNonce(0),
            price_ticks: 0,
            quantity_minor: 1,
            submitted_at_micros: 1,
        };

        assert!(matches!(
            state.compute_place_order(&incoming),
            Err(FluidError::InsufficientBalance { .. })
        ));
        assert!(state.compute_place_order_replay(&incoming).is_ok());
    }
}
