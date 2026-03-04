use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use anyhow::Context as _;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Default)]
struct AuthKeySnapshot {
    user_api_keys: HashMap<String, String>,
    admin_api_keys: HashSet<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AuthKeyStore {
    snapshot: Arc<RwLock<AuthKeySnapshot>>,
}

#[derive(Debug, sqlx::FromRow)]
struct UserApiKeyRow {
    api_key: String,
    account_id: String,
}

#[derive(Debug, sqlx::FromRow)]
struct AdminApiKeyRow {
    admin_key: String,
}

impl AuthKeyStore {
    pub(crate) fn api_key_count(&self) -> usize {
        self.with_read(|snapshot| snapshot.user_api_keys.len())
    }

    pub(crate) fn admin_key_count(&self) -> usize {
        self.with_read(|snapshot| snapshot.admin_api_keys.len())
    }

    pub(crate) fn account_id_for_api_key(&self, api_key: &str) -> Option<String> {
        self.with_read(|snapshot| snapshot.user_api_keys.get(api_key).cloned())
    }

    pub(crate) fn is_admin_key(&self, api_key: &str) -> bool {
        self.with_read(|snapshot| snapshot.admin_api_keys.contains(api_key))
    }

    pub(crate) async fn reload_from_db(&self, db: &PgPool) -> Result<(), anyhow::Error> {
        let user_rows: Vec<UserApiKeyRow> = sqlx::query_as(
            r#"
            SELECT api_key, account_id
            FROM user_api_keys
            WHERE revoked_at IS NULL
            "#,
        )
        .fetch_all(db)
        .await
        .context("query user API keys")?;

        let admin_rows: Vec<AdminApiKeyRow> = sqlx::query_as(
            r#"
            SELECT admin_key
            FROM admin_api_keys
            "#,
        )
        .fetch_all(db)
        .await
        .context("query admin API keys")?;

        let mut user_api_keys = HashMap::with_capacity(user_rows.len());
        for row in user_rows {
            user_api_keys.insert(row.api_key, row.account_id);
        }

        let mut admin_api_keys = HashSet::with_capacity(admin_rows.len());
        for row in admin_rows {
            admin_api_keys.insert(row.admin_key);
        }

        self.with_write(move |snapshot| {
            snapshot.user_api_keys = user_api_keys;
            snapshot.admin_api_keys = admin_api_keys;
        });

        Ok(())
    }

    pub(crate) async fn upsert_user_api_key(
        &self,
        db: &PgPool,
        api_key: &str,
        account_id: &str,
    ) -> Result<(), anyhow::Error> {
        let key_id = format!("uak_{}", Uuid::new_v4().simple());
        sqlx::query(
            r#"
            INSERT INTO user_api_keys (
              api_key,
              account_id,
              key_id,
              created_at,
              updated_at
            )
            VALUES ($1, $2, $3, now(), now())
            ON CONFLICT (api_key) DO UPDATE
            SET
              account_id = EXCLUDED.account_id,
              revoked_at = NULL,
              updated_at = now()
            "#,
        )
        .bind(api_key)
        .bind(account_id)
        .bind(key_id)
        .execute(db)
        .await
        .context("upsert user API key")?;

        self.with_write(|snapshot| {
            snapshot
                .user_api_keys
                .insert(api_key.to_string(), account_id.to_string());
        });

        Ok(())
    }

    pub(crate) async fn upsert_admin_api_key(
        &self,
        db: &PgPool,
        admin_key: &str,
    ) -> Result<(), anyhow::Error> {
        sqlx::query(
            r#"
            INSERT INTO admin_api_keys (
              admin_key,
              created_at,
              updated_at
            )
            VALUES ($1, now(), now())
            ON CONFLICT (admin_key) DO UPDATE
            SET
              updated_at = now()
            "#,
        )
        .bind(admin_key)
        .execute(db)
        .await
        .context("upsert admin API key")?;

        self.with_write(|snapshot| {
            snapshot.admin_api_keys.insert(admin_key.to_string());
        });

        Ok(())
    }

    fn with_read<T>(&self, op: impl FnOnce(&AuthKeySnapshot) -> T) -> T {
        match self.snapshot.read() {
            Ok(snapshot) => op(&snapshot),
            Err(poisoned) => {
                tracing::warn!("auth key snapshot read lock poisoned; continuing with inner state");
                let snapshot = poisoned.into_inner();
                op(&snapshot)
            }
        }
    }

    fn with_write(&self, op: impl FnOnce(&mut AuthKeySnapshot)) {
        match self.snapshot.write() {
            Ok(mut snapshot) => op(&mut snapshot),
            Err(poisoned) => {
                tracing::warn!(
                    "auth key snapshot write lock poisoned; continuing with inner state"
                );
                let mut snapshot = poisoned.into_inner();
                op(&mut snapshot);
            }
        }
    }
}
