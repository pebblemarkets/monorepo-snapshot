use std::collections::HashMap;

use crate::error::FluidError;
use crate::transition::AccountRiskDelta;
use crate::transition::RiskTransition;

/// Per-account risk and balance state held in memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountState {
    pub cleared_cash_minor: i64,
    pub delta_pending_trades_minor: i64,
    pub locked_open_orders_minor: i64,
    pub pending_withdrawals_reserved_minor: i64,
    /// Nonce of the account currently reserved by an in-flight operation.
    pub pending_nonce: Option<i64>,
    /// Capital reserved by the in-flight operation.
    pub pending_lock_minor: i64,
    pub last_nonce: Option<i64>,
    pub status: String,
    pub active: bool,
    pub instrument_admin: String,
    pub instrument_id: String,
}

/// Grant from a successful reserve call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveGrant {
    pub available_before: i64,
    pub instrument_admin: String,
    pub instrument_id: String,
}

/// Single shared risk engine for all markets and accounts.
#[derive(Clone, Debug, Default)]
pub struct RiskEngine {
    accounts: HashMap<String, AccountState>,
}

impl RiskEngine {
    fn get_account_mut(&mut self, id: &str) -> Result<&mut AccountState, FluidError> {
        self.accounts
            .get_mut(id)
            .ok_or_else(|| FluidError::AccountNotFound(id.to_string()))
    }

    /// Raw available balance: `cleared + delta - locked - pending_lock`.
    /// Caller is responsible for subtracting withdrawal reserves if the toggle is on.
    pub fn available_minor(&self, id: &str) -> Result<i64, FluidError> {
        let acc = self.get_account(id)?;
        acc.cleared_cash_minor
            .checked_add(acc.delta_pending_trades_minor)
            .and_then(|v| v.checked_sub(acc.locked_open_orders_minor))
            .and_then(|v| v.checked_sub(acc.pending_lock_minor))
            .ok_or_else(|| FluidError::Overflow(format!("available_minor for {id}")))
    }

    /// Validate that the given nonce is the next expected value.
    /// For accounts that have never traded (`last_nonce = None`), the first nonce must be 0.
    pub fn check_nonce(&self, id: &str, nonce: i64) -> Result<(), FluidError> {
        let acc = self.get_account(id)?;
        let expected = match acc.last_nonce {
            Some(last) => last
                .checked_add(1)
                .ok_or_else(|| FluidError::Overflow(format!("nonce overflow for {id}")))?,
            None => 0,
        };
        if nonce != expected {
            return Err(FluidError::NonceMismatch {
                expected,
                got: nonce,
            });
        }
        Ok(())
    }

    /// Check that the account exists, is active, and has status `"Active"`.
    pub fn check_account_active(&self, id: &str) -> Result<(), FluidError> {
        let acc = self.get_account(id)?;
        if !acc.active || acc.status != "Active" {
            return Err(FluidError::AccountNotActive(id.to_string()));
        }
        Ok(())
    }

    /// Phase 1: reserve capital. Validates account/nonce/balance atomically.
    pub fn reserve(
        &mut self,
        account_id: &str,
        required_lock: i64,
        nonce: i64,
        instrument_admin: &str,
        instrument_id: &str,
        withdrawal_toggle: bool,
    ) -> Result<ReserveGrant, FluidError> {
        if required_lock < 0 {
            return Err(FluidError::MatchFailed(
                "required lock cannot be negative".to_string(),
            ));
        }

        {
            let account = self.get_account(account_id)?;
            if !account.active || account.status != "Active" {
                return Err(FluidError::AccountNotActive(account_id.to_string()));
            }

            if account.instrument_admin != instrument_admin
                || account.instrument_id != instrument_id
            {
                return Err(FluidError::InstrumentMismatch);
            }

            if let Some(pending_nonce) = account.pending_nonce {
                return Err(FluidError::NoncePending {
                    account_id: account_id.to_string(),
                    pending_nonce,
                });
            }

            self.check_nonce(account_id, nonce)?;
        }

        let mut available = self.available_minor(account_id)?;
        if withdrawal_toggle {
            let pending_withdrawal_reserves = self
                .get_account(account_id)?
                .pending_withdrawals_reserved_minor;
            available = available
                .checked_sub(pending_withdrawal_reserves)
                .ok_or_else(|| FluidError::Overflow("available balance underflow".to_string()))?;
        }
        if available < required_lock {
            return Err(FluidError::InsufficientBalance {
                available,
                required: required_lock,
            });
        }

        let available_before = available;
        let account = self.get_account_mut(account_id)?;
        account.pending_nonce = Some(nonce);
        account.pending_lock_minor = required_lock;

        Ok(ReserveGrant {
            available_before,
            instrument_admin: account.instrument_admin.clone(),
            instrument_id: account.instrument_id.clone(),
        })
    }

    /// Phase 2a: commit a reservation and apply a risk transition.
    pub fn commit(
        &mut self,
        account_id: &str,
        transition: &RiskTransition,
    ) -> Result<(), FluidError> {
        let (transition_account, expected_nonce) =
            transition.nonce_advance.clone().ok_or_else(|| {
                FluidError::MatchFailed("missing nonce_advance in transition".to_string())
            })?;

        let account = self.get_account_mut(account_id)?;
        let pending_nonce = account
            .pending_nonce
            .ok_or_else(|| FluidError::MatchFailed("no pending reserve".to_string()))?;
        if transition_account != account_id {
            return Err(FluidError::MatchFailed(format!(
                "transition account {transition_account} does not match reserve account {account_id}"
            )));
        }
        if expected_nonce != pending_nonce {
            return Err(FluidError::NonceMismatch {
                expected: pending_nonce,
                got: expected_nonce,
            });
        }

        account.pending_nonce = None;
        account.pending_lock_minor = 0;
        let _ = account;

        self.apply_risk_transition(transition)
    }

    /// Phase 2b: abort a reservation.
    pub fn abort(&mut self, account_id: &str, release_amount: i64) -> Result<(), FluidError> {
        let account = self.get_account_mut(account_id)?;
        if account.pending_nonce.is_none() {
            return Err(FluidError::MatchFailed("no pending reserve".to_string()));
        }
        if release_amount != account.pending_lock_minor {
            return Err(FluidError::MatchFailed(format!(
                "abort amount mismatch: pending={} release={}",
                account.pending_lock_minor, release_amount
            )));
        }
        account.pending_nonce = None;
        account.pending_lock_minor = 0;
        Ok(())
    }

    /// Clear all pending nonces/locks.
    pub fn clear_all_pending(&mut self) {
        for account in self.accounts.values_mut() {
            account.pending_nonce = None;
            account.pending_lock_minor = 0;
        }
    }

    /// Apply risk mutations after a successful DB persist.
    pub fn apply_risk_transition(&mut self, rt: &RiskTransition) -> Result<(), FluidError> {
        for delta in &rt.deltas {
            let acc = self
                .accounts
                .get_mut(&delta.account_id)
                .ok_or_else(|| FluidError::AccountNotFound(delta.account_id.clone()))?;

            acc.delta_pending_trades_minor = acc
                .delta_pending_trades_minor
                .checked_add(delta.delta_pending_minor)
                .ok_or_else(|| {
                    FluidError::Overflow(format!("delta_pending for {}", delta.account_id))
                })?;

            let new_locked = acc
                .locked_open_orders_minor
                .checked_add(delta.locked_delta_minor)
                .ok_or_else(|| {
                    FluidError::Overflow(format!("locked_open_orders for {}", delta.account_id))
                })?;
            if new_locked < 0 {
                return Err(FluidError::Overflow(format!(
                    "locked_open_orders negative ({new_locked}) for {}",
                    delta.account_id
                )));
            }
            acc.locked_open_orders_minor = new_locked;
        }

        if let Some((ref account_id, new_nonce)) = rt.nonce_advance {
            let acc = self
                .accounts
                .get_mut(account_id)
                .ok_or_else(|| FluidError::AccountNotFound(account_id.clone()))?;
            acc.last_nonce = Some(new_nonce);
        }

        Ok(())
    }

    /// Apply the locked capital release from cancelling an order.
    pub fn apply_cancel_risk_delta(
        &mut self,
        account_id: &str,
        released_locked_minor: i64,
    ) -> Result<(), FluidError> {
        if released_locked_minor < 0 {
            return Err(FluidError::Overflow(format!(
                "released locked must be non-negative: {released_locked_minor}"
            )));
        }

        let rt = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: account_id.to_string(),
                delta_pending_minor: 0,
                locked_delta_minor: released_locked_minor.checked_neg().ok_or_else(|| {
                    FluidError::Overflow("cancel locked release overflow".to_string())
                })?,
            }],
            nonce_advance: None,
        };

        self.apply_risk_transition(&rt)
    }

    /// Overwrite cleared cash for an account (from sync task).
    pub fn update_cleared_cash(&mut self, id: &str, val: i64) {
        if let Some(acc) = self.accounts.get_mut(id) {
            acc.cleared_cash_minor = val;
        }
    }

    /// Overwrite account status and active flag (from sync task).
    pub fn update_account_status(&mut self, id: &str, status: String, active: bool) {
        if let Some(acc) = self.accounts.get_mut(id) {
            acc.status = status;
            acc.active = active;
        }
    }

    /// Overwrite withdrawal reserves (from sync task).
    pub fn update_withdrawal_reserves(&mut self, id: &str, val: i64) {
        if let Some(acc) = self.accounts.get_mut(id) {
            acc.pending_withdrawals_reserved_minor = val;
        }
    }

    /// Get a reference to an account's state.
    pub fn get_account(&self, id: &str) -> Result<&AccountState, FluidError> {
        self.accounts
            .get(id)
            .ok_or_else(|| FluidError::AccountNotFound(id.to_string()))
    }

    /// Insert or overwrite an account's full state (bootstrap + sync).
    pub fn upsert_account(&mut self, id: &str, state: AccountState) {
        self.accounts.insert(id.to_string(), state);
    }

    /// Number of tracked accounts.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::transition::AccountRiskDelta;

    fn test_account(cleared: i64, delta: i64, locked: i64) -> AccountState {
        AccountState {
            cleared_cash_minor: cleared,
            delta_pending_trades_minor: delta,
            locked_open_orders_minor: locked,
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

    #[test]
    fn test_available_minor_formula() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 200, 300));
        // available = 1000 + 200 - 300 = 900
        assert_eq!(risk.available_minor("a1").unwrap(), 900);
    }

    #[test]
    fn test_available_minor_includes_pending_lock() {
        let mut risk = RiskEngine::default();
        let mut acc = test_account(1000, 0, 300);
        acc.pending_lock_minor = 150;
        risk.upsert_account("a1", acc);

        // 1000 - 300 - 150 = 550
        assert_eq!(risk.available_minor("a1").unwrap(), 550);
    }

    #[test]
    fn test_available_minor_with_negative_delta() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, -200, 300));
        // available = 1000 + (-200) - 300 = 500
        assert_eq!(risk.available_minor("a1").unwrap(), 500);
    }

    #[test]
    fn test_nonce_check_validates_progression() {
        let mut risk = RiskEngine::default();
        let mut acc = test_account(1000, 0, 0);
        acc.last_nonce = Some(5);
        risk.upsert_account("a1", acc);

        // Next expected is 6
        assert!(risk.check_nonce("a1", 6).is_ok());
        assert!(risk.check_nonce("a1", 5).is_err());
        assert!(risk.check_nonce("a1", 7).is_err());
    }

    #[test]
    fn test_nonce_first_order_must_be_zero() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 0, 0));
        // last_nonce = None → first order must be 0
        assert!(risk.check_nonce("a1", 0).is_ok());
        assert!(risk.check_nonce("a1", 1).is_err());
    }

    #[test]
    fn test_check_account_active_rejects_inactive() {
        let mut risk = RiskEngine::default();
        let mut acc = test_account(1000, 0, 0);
        acc.status = "Suspended".to_string();
        risk.upsert_account("a1", acc);

        assert!(risk.check_account_active("a1").is_err());
    }

    #[test]
    fn test_check_account_active_accepts_active() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 0, 0));
        assert!(risk.check_account_active("a1").is_ok());
    }

    #[test]
    fn test_reserve_basic() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(10_000, 0, 1_000));

        let grant = risk.reserve("a1", 200, 0, "admin", "USD", false).unwrap();
        assert_eq!(grant.available_before, 9_000);
        assert_eq!(grant.instrument_admin, "admin");
        assert_eq!(grant.instrument_id, "USD");

        let account = risk.get_account("a1").unwrap();
        assert_eq!(account.pending_nonce, Some(0));
        assert_eq!(account.pending_lock_minor, 200);
    }

    #[test]
    fn test_reserve_then_commit() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(10_000, 0, 0));

        risk.reserve("a1", 200, 0, "admin", "USD", false).unwrap();

        let transition = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: "a1".to_string(),
                delta_pending_minor: -200,
                locked_delta_minor: 200,
            }],
            nonce_advance: Some(("a1".to_string(), 0)),
        };
        risk.commit("a1", &transition).unwrap();

        let account = risk.get_account("a1").unwrap();
        assert_eq!(account.pending_nonce, None);
        assert_eq!(account.pending_lock_minor, 0);
        assert_eq!(account.delta_pending_trades_minor, -200);
        assert_eq!(account.locked_open_orders_minor, 200);
        assert_eq!(account.last_nonce, Some(0));
    }

    #[test]
    fn test_reserve_then_abort() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(10_000, 0, 1_000));

        risk.reserve("a1", 200, 0, "admin", "USD", false).unwrap();
        risk.abort("a1", 200).unwrap();

        let account = risk.get_account("a1").unwrap();
        assert_eq!(account.pending_nonce, None);
        assert_eq!(account.pending_lock_minor, 0);
        assert_eq!(account.locked_open_orders_minor, 1_000);
    }

    #[test]
    fn test_reserve_double_rejected() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(10_000, 0, 0));

        risk.reserve("a1", 100, 0, "admin", "USD", false).unwrap();
        let err = risk
            .reserve("a1", 50, 1, "admin", "USD", false)
            .unwrap_err();
        assert!(matches!(
            err,
            FluidError::NoncePending {
                account_id: _,
                pending_nonce: 0
            }
        ));
    }

    #[test]
    fn test_reserve_wrong_nonce() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(10_000, 0, 0));

        let err = risk
            .reserve("a1", 100, 2, "admin", "USD", false)
            .unwrap_err();
        assert!(matches!(
            err,
            FluidError::NonceMismatch {
                expected: 0,
                got: 2
            }
        ));
    }

    #[test]
    fn test_reserve_insufficient_balance() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(100, 0, 0));

        let err = risk
            .reserve("a1", 101, 0, "admin", "USD", false)
            .unwrap_err();
        assert!(matches!(
            err,
            FluidError::InsufficientBalance {
                available: 100,
                required: 101
            }
        ));
    }

    #[test]
    fn test_reserve_inactive_rejected() {
        let mut risk = RiskEngine::default();
        let mut account = test_account(10_000, 0, 0);
        account.active = false;
        risk.upsert_account("a1", account);

        let err = risk.reserve("a1", 1, 0, "admin", "USD", false).unwrap_err();
        assert!(matches!(err, FluidError::AccountNotActive(_)));
    }

    #[test]
    fn test_reserve_instrument_mismatch() {
        let mut risk = RiskEngine::default();
        let mut account = test_account(10_000, 0, 0);
        account.instrument_id = "EUR".to_string();
        risk.upsert_account("a1", account);

        let err = risk.reserve("a1", 1, 0, "admin", "USD", false).unwrap_err();
        assert!(matches!(err, FluidError::InstrumentMismatch));
    }

    #[test]
    fn test_clear_all_pending() {
        let mut risk = RiskEngine::default();
        let mut account = test_account(10_000, 0, 0);
        account.pending_nonce = Some(5);
        account.pending_lock_minor = 123;
        risk.upsert_account("a1", account);

        risk.clear_all_pending();
        let account = risk.get_account("a1").unwrap();
        assert_eq!(account.pending_nonce, None);
        assert_eq!(account.pending_lock_minor, 0);
    }

    #[test]
    fn test_apply_risk_transition_updates_both_parties() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("taker", test_account(1000, 0, 0));
        risk.upsert_account("maker", test_account(1000, 0, 500));

        let rt = RiskTransition {
            deltas: vec![
                AccountRiskDelta {
                    account_id: "taker".to_string(),
                    delta_pending_minor: -200,
                    locked_delta_minor: 200,
                },
                AccountRiskDelta {
                    account_id: "maker".to_string(),
                    delta_pending_minor: 200,
                    locked_delta_minor: -200,
                },
            ],
            nonce_advance: Some(("taker".to_string(), 0)),
        };

        risk.apply_risk_transition(&rt).unwrap();

        let taker = risk.get_account("taker").unwrap();
        assert_eq!(taker.delta_pending_trades_minor, -200);
        assert_eq!(taker.locked_open_orders_minor, 200);
        assert_eq!(taker.last_nonce, Some(0));

        let maker = risk.get_account("maker").unwrap();
        assert_eq!(maker.delta_pending_trades_minor, 200);
        assert_eq!(maker.locked_open_orders_minor, 300);
    }

    #[test]
    fn test_release_locked_decrements() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 0, 500));

        let rt = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: "a1".to_string(),
                delta_pending_minor: 0,
                locked_delta_minor: -200,
            }],
            nonce_advance: None,
        };

        risk.apply_risk_transition(&rt).unwrap();
        assert_eq!(
            risk.get_account("a1").unwrap().locked_open_orders_minor,
            300
        );
    }

    #[test]
    fn test_locked_negative_rejected() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 0, 5));

        let rt = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: "a1".to_string(),
                delta_pending_minor: 0,
                locked_delta_minor: -10, // would push locked to -5
            }],
            nonce_advance: None,
        };

        assert!(matches!(
            risk.apply_risk_transition(&rt),
            Err(FluidError::Overflow(_))
        ));
        // State should be unchanged after error
        assert_eq!(risk.get_account("a1").unwrap().locked_open_orders_minor, 5);
    }

    #[test]
    fn test_overflow_returns_error() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(i64::MAX, 1, 0));

        // cleared + delta overflows
        assert!(matches!(
            risk.available_minor("a1"),
            Err(FluidError::Overflow(_))
        ));
    }

    #[test]
    fn test_account_not_found() {
        let risk = RiskEngine::default();
        assert!(matches!(
            risk.available_minor("nope"),
            Err(FluidError::AccountNotFound(_))
        ));
    }

    #[test]
    fn test_sync_updates_are_idempotent() {
        let mut risk = RiskEngine::default();
        risk.upsert_account("a1", test_account(1000, 0, 0));

        risk.update_cleared_cash("a1", 2000);
        risk.update_cleared_cash("a1", 2000);
        assert_eq!(risk.get_account("a1").unwrap().cleared_cash_minor, 2000);

        risk.update_account_status("a1", "Suspended".to_string(), false);
        assert_eq!(risk.get_account("a1").unwrap().status, "Suspended");
        assert!(!risk.get_account("a1").unwrap().active);

        risk.update_withdrawal_reserves("a1", 500);
        assert_eq!(
            risk.get_account("a1")
                .unwrap()
                .pending_withdrawals_reserved_minor,
            500
        );
    }
}
