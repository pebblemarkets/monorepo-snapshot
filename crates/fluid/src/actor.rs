use tokio::sync::{mpsc, oneshot};

use crate::error::FluidError;
use crate::risk::{AccountState, ReserveGrant, RiskEngine};
use crate::transition::RiskTransition;

#[derive(Debug)]
pub enum RiskCommand {
    Reserve {
        account_id: String,
        required_lock: i64,
        nonce: i64,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_toggle: bool,
        reply: oneshot::Sender<Result<ReserveGrant, FluidError>>,
    },
    Commit {
        account_id: String,
        transition: RiskTransition,
        reply: oneshot::Sender<Result<(), FluidError>>,
    },
    Abort {
        account_id: String,
        release_amount: i64,
        reply: oneshot::Sender<Result<(), FluidError>>,
    },
    ApplyDelta {
        transition: RiskTransition,
        reply: oneshot::Sender<Result<(), FluidError>>,
    },
    SyncClearedCash {
        account_id: String,
        cleared_cash_minor: i64,
        epoch: u64,
    },
    SyncAccountStatus {
        account_id: String,
        status: String,
        active: bool,
        epoch: u64,
    },
    SyncWithdrawalReserves {
        account_id: String,
        reserves: i64,
        epoch: u64,
    },
    Resync {
        risk: RiskEngine,
        reply: oneshot::Sender<()>,
    },
    GetEpoch {
        reply: oneshot::Sender<u64>,
    },
    GetBalance {
        account_id: String,
        reply: oneshot::Sender<Result<i64, FluidError>>,
    },
    GetAccount {
        account_id: String,
        reply: oneshot::Sender<Result<AccountState, FluidError>>,
    },
    AccountCount {
        reply: oneshot::Sender<usize>,
    },
}

#[derive(Clone, Debug)]
pub struct RiskActorHandle {
    tx: mpsc::Sender<RiskCommand>,
}

impl RiskActorHandle {
    pub fn spawn(risk: RiskEngine, channel_capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity);
        tokio::spawn(run_risk_actor(risk, rx));
        Self { tx }
    }

    pub async fn reserve(
        &self,
        account_id: &str,
        required_lock: i64,
        nonce: i64,
        instrument_admin: &str,
        instrument_id: &str,
        withdrawal_toggle: bool,
    ) -> Result<ReserveGrant, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::Reserve {
            account_id: account_id.to_string(),
            required_lock,
            nonce,
            instrument_admin: instrument_admin.to_string(),
            instrument_id: instrument_id.to_string(),
            withdrawal_toggle,
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn commit(
        &self,
        account_id: &str,
        transition: RiskTransition,
    ) -> Result<(), FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::Commit {
            account_id: account_id.to_string(),
            transition,
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn abort(&self, account_id: &str, release_amount: i64) -> Result<(), FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::Abort {
            account_id: account_id.to_string(),
            release_amount,
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn apply_delta(&self, transition: RiskTransition) -> Result<(), FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::ApplyDelta {
            transition,
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn sync_cleared_cash(
        &self,
        account_id: &str,
        cleared_cash_minor: i64,
    ) -> Result<(), FluidError> {
        let epoch = self.current_epoch().await?;
        let cmd = RiskCommand::SyncClearedCash {
            account_id: account_id.to_string(),
            cleared_cash_minor,
            epoch,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        Ok(())
    }

    pub async fn sync_account_status(
        &self,
        account_id: &str,
        status: &str,
        active: bool,
    ) -> Result<(), FluidError> {
        let epoch = self.current_epoch().await?;
        let cmd = RiskCommand::SyncAccountStatus {
            account_id: account_id.to_string(),
            status: status.to_string(),
            active,
            epoch,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        Ok(())
    }

    pub async fn sync_withdrawal_reserves(
        &self,
        account_id: &str,
        reserves: i64,
    ) -> Result<(), FluidError> {
        let epoch = self.current_epoch().await?;
        let cmd = RiskCommand::SyncWithdrawalReserves {
            account_id: account_id.to_string(),
            reserves,
            epoch,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        Ok(())
    }

    pub async fn resync(&self, risk: RiskEngine) -> Result<(), FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::Resync {
            risk,
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?;
        Ok(())
    }

    pub async fn current_epoch(&self) -> Result<u64, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::GetEpoch { reply: reply_tx };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())
    }

    pub async fn get_balance(&self, account_id: &str) -> Result<i64, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::GetBalance {
            account_id: account_id.to_string(),
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn get_account(&self, account_id: &str) -> Result<AccountState, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::GetAccount {
            account_id: account_id.to_string(),
            reply: reply_tx,
        };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())?
    }

    pub async fn account_count(&self) -> Result<usize, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = RiskCommand::AccountCount { reply: reply_tx };
        self.tx.send(cmd).await.map_err(|_| actor_unavailable())?;
        reply_rx.await.map_err(|_| actor_unavailable())
    }
}

async fn run_risk_actor(mut risk: RiskEngine, mut rx: mpsc::Receiver<RiskCommand>) {
    let mut epoch: u64 = 0;

    while let Some(cmd) = rx.recv().await {
        match cmd {
            RiskCommand::Reserve {
                account_id,
                required_lock,
                nonce,
                instrument_admin,
                instrument_id,
                withdrawal_toggle,
                reply,
            } => {
                let result = risk.reserve(
                    &account_id,
                    required_lock,
                    nonce,
                    &instrument_admin,
                    &instrument_id,
                    withdrawal_toggle,
                );
                let _ = reply.send(result);
            }
            RiskCommand::Commit {
                account_id,
                transition,
                reply,
            } => {
                let result = risk.commit(&account_id, &transition);
                let _ = reply.send(result);
            }
            RiskCommand::Abort {
                account_id,
                release_amount,
                reply,
            } => {
                let result = risk.abort(&account_id, release_amount);
                let _ = reply.send(result);
            }
            RiskCommand::ApplyDelta { transition, reply } => {
                let result = risk.apply_risk_transition(&transition);
                let _ = reply.send(result);
            }
            RiskCommand::SyncClearedCash {
                account_id,
                cleared_cash_minor,
                epoch: cmd_epoch,
            } => {
                if cmd_epoch == epoch {
                    risk.update_cleared_cash(&account_id, cleared_cash_minor);
                }
            }
            RiskCommand::SyncAccountStatus {
                account_id,
                status,
                active,
                epoch: cmd_epoch,
            } => {
                if cmd_epoch == epoch {
                    risk.update_account_status(&account_id, status, active);
                }
            }
            RiskCommand::SyncWithdrawalReserves {
                account_id,
                reserves,
                epoch: cmd_epoch,
            } => {
                if cmd_epoch == epoch {
                    risk.update_withdrawal_reserves(&account_id, reserves);
                }
            }
            RiskCommand::Resync {
                risk: next_risk,
                reply,
            } => {
                risk = next_risk;
                epoch = epoch.saturating_add(1);
                let _ = reply.send(());
            }
            RiskCommand::GetEpoch { reply } => {
                let _ = reply.send(epoch);
            }
            RiskCommand::GetBalance { account_id, reply } => {
                let result = risk.available_minor(&account_id);
                let _ = reply.send(result);
            }
            RiskCommand::GetAccount { account_id, reply } => {
                let result = risk.get_account(&account_id).cloned();
                let _ = reply.send(result);
            }
            RiskCommand::AccountCount { reply } => {
                let _ = reply.send(risk.account_count());
            }
        }
    }
}

fn actor_unavailable() -> FluidError {
    FluidError::EngineUnhealthy("risk actor unavailable".to_string())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::risk::{AccountState, RiskEngine};
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

    #[tokio::test]
    async fn test_actor_reserve_commit_flow() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        let grant = actor
            .reserve("buyer", 200, 0, "admin", "USD", false)
            .await
            .unwrap();
        assert_eq!(grant.available_before, 10_000);

        let transition = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: "buyer".to_string(),
                delta_pending_minor: -200,
                locked_delta_minor: 200,
            }],
            nonce_advance: Some(("buyer".to_string(), 0)),
        };
        actor.commit("buyer", transition).await.unwrap();

        let account = actor.get_account("buyer").await.unwrap();
        assert_eq!(account.pending_nonce, None);
        assert_eq!(account.pending_lock_minor, 0);
        assert_eq!(account.locked_open_orders_minor, 200);
        assert_eq!(account.delta_pending_trades_minor, -200);
        assert_eq!(account.last_nonce, Some(0));
    }

    #[tokio::test]
    async fn test_actor_reserve_abort_flow() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        actor
            .reserve("buyer", 200, 0, "admin", "USD", false)
            .await
            .unwrap();
        actor.abort("buyer", 200).await.unwrap();

        let account = actor.get_account("buyer").await.unwrap();
        assert_eq!(account.pending_nonce, None);
        assert_eq!(account.pending_lock_minor, 0);
    }

    #[tokio::test]
    async fn test_actor_apply_delta_without_reservation() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("seller", test_account(10_000, 0, 500));
        let actor = RiskActorHandle::spawn(initial, 16);

        let transition = RiskTransition {
            deltas: vec![AccountRiskDelta {
                account_id: "seller".to_string(),
                delta_pending_minor: 100,
                locked_delta_minor: -100,
            }],
            nonce_advance: None,
        };
        actor.apply_delta(transition).await.unwrap();

        let account = actor.get_account("seller").await.unwrap();
        assert_eq!(account.delta_pending_trades_minor, 100);
        assert_eq!(account.locked_open_orders_minor, 400);
    }

    #[tokio::test]
    async fn test_actor_concurrent_different_accounts() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        initial.upsert_account("seller", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        let actor1 = actor.clone();
        let actor2 = actor.clone();
        let (buyer_res, seller_res) = tokio::join!(
            actor1.reserve("buyer", 100, 0, "admin", "USD", false),
            actor2.reserve("seller", 200, 0, "admin", "USD", false)
        );
        assert!(buyer_res.is_ok());
        assert!(seller_res.is_ok());
        actor.abort("buyer", 100).await.unwrap();
        actor.abort("seller", 200).await.unwrap();
    }

    #[tokio::test]
    async fn test_actor_concurrent_same_account_serialized() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        let actor1 = actor.clone();
        let actor2 = actor.clone();
        let first =
            tokio::spawn(
                async move { actor1.reserve("buyer", 100, 0, "admin", "USD", false).await },
            );
        let second =
            tokio::spawn(
                async move { actor2.reserve("buyer", 100, 0, "admin", "USD", false).await },
            );

        let (first_res, second_res) = tokio::join!(first, second);
        assert!(first_res.is_ok());
        assert!(second_res.is_ok());
        let first_res = first_res.unwrap();
        let second_res = second_res.unwrap();
        assert!(first_res.is_ok() != second_res.is_ok());

        if first_res.is_ok() {
            assert!(matches!(
                second_res,
                Err(FluidError::NoncePending {
                    account_id: _,
                    pending_nonce: 0
                })
            ));
        } else {
            assert!(matches!(
                first_res,
                Err(FluidError::NoncePending {
                    account_id: _,
                    pending_nonce: 0
                })
            ));
        }

        actor.abort("buyer", 100).await.unwrap();
    }

    #[tokio::test]
    async fn test_actor_sync_updates() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        actor.sync_cleared_cash("buyer", 2000).await.unwrap();
        actor
            .sync_account_status("buyer", "Suspended", false)
            .await
            .unwrap();
        actor.sync_withdrawal_reserves("buyer", 100).await.unwrap();

        let account = actor.get_account("buyer").await.unwrap();
        assert_eq!(account.cleared_cash_minor, 2000);
        assert_eq!(account.status, "Suspended");
        assert!(!account.active);
        assert_eq!(account.pending_withdrawals_reserved_minor, 100);
    }

    #[tokio::test]
    async fn test_actor_resync() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);

        let epoch_before = actor.current_epoch().await.unwrap();
        let mut replacement = RiskEngine::default();
        replacement.upsert_account("buyer", test_account(3_000, 0, 0));

        actor.resync(replacement).await.unwrap();
        let epoch_after = actor.current_epoch().await.unwrap();
        assert_eq!(epoch_after, epoch_before.saturating_add(1));

        let account = actor.get_account("buyer").await.unwrap();
        assert_eq!(account.cleared_cash_minor, 3_000);
    }

    #[tokio::test]
    async fn test_actor_stale_sync_dropped_after_resync() {
        let mut initial = RiskEngine::default();
        initial.upsert_account("buyer", test_account(10_000, 0, 0));
        let actor = RiskActorHandle::spawn(initial, 16);
        let stale_epoch = actor.current_epoch().await.unwrap();

        let mut replacement = RiskEngine::default();
        replacement.upsert_account("buyer", test_account(3_000, 0, 0));
        actor.resync(replacement).await.unwrap();

        actor
            .tx
            .send(RiskCommand::SyncClearedCash {
                account_id: "buyer".to_string(),
                cleared_cash_minor: 999,
                epoch: stale_epoch,
            })
            .await
            .unwrap();
        let fresh_epoch = actor.current_epoch().await.unwrap();
        actor
            .tx
            .send(RiskCommand::SyncClearedCash {
                account_id: "buyer".to_string(),
                cleared_cash_minor: 4000,
                epoch: fresh_epoch,
            })
            .await
            .unwrap();

        let account = actor.get_account("buyer").await.unwrap();
        assert_eq!(account.cleared_cash_minor, 4000);
    }
}
