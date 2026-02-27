use std::time::Duration;

use pebble_daml_grpc::com::daml::ledger::api::v2 as lapi;
use thiserror::Error;
use tonic::{
    metadata::{errors::InvalidMetadataValue, MetadataValue},
    transport::Endpoint,
    Code, Request,
};

pub type LedgerResult<T> = std::result::Result<T, LedgerClientError>;

#[derive(Debug, Error)]
pub enum LedgerClientError {
    #[error("invalid configuration: {field} {reason}")]
    InvalidConfiguration {
        field: &'static str,
        reason: &'static str,
    },
    #[error("invalid argument: {field} {reason}")]
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    #[error("build ledger endpoint failed: {0}")]
    BuildEndpoint(#[source] tonic::transport::Error),
    #[error("connect to ledger failed: {0}")]
    Connect(#[source] tonic::transport::Error),
    #[error("parse auth token as gRPC metadata value failed: {0}")]
    InvalidAuthHeader(#[source] InvalidMetadataValue),
    #[error("ledger RPC failed ({code}): {message}")]
    RpcStatus { code: Code, message: String },
    #[error("ledger RPC retry exhausted after {attempts} attempts ({code}): {message}")]
    RetryExhausted {
        attempts: u32,
        code: Code,
        message: String,
    },
    #[error("missing transaction in response")]
    MissingTransaction,
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub retryable_codes: Vec<Code>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
            retryable_codes: vec![
                Code::Unavailable,
                Code::Aborted,
                Code::DeadlineExceeded,
                Code::ResourceExhausted,
            ],
        }
    }
}

impl RetryPolicy {
    pub fn validate(&self) -> LedgerResult<()> {
        if self.max_attempts == 0 {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "max_attempts",
                reason: "must be > 0",
            });
        }

        if self.initial_backoff.is_zero() {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "initial_backoff",
                reason: "must be > 0",
            });
        }

        if self.max_backoff.is_zero() {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "max_backoff",
                reason: "must be > 0",
            });
        }

        if self.max_backoff < self.initial_backoff {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "max_backoff",
                reason: "must be >= initial_backoff",
            });
        }

        if self.retryable_codes.is_empty() {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "retryable_codes",
                reason: "must be non-empty",
            });
        }

        Ok(())
    }

    fn should_retry(&self, code: Code) -> bool {
        self.retryable_codes.contains(&code)
    }

    fn next_backoff(&self, current_backoff: Duration) -> Duration {
        std::cmp::min(current_backoff.saturating_mul(2), self.max_backoff)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SubmitOptions {
    pub act_as: Option<Vec<String>>,
    pub read_as: Vec<String>,
    pub workflow_id: Option<String>,
    pub submission_id: Option<String>,
    pub disclosed_contracts: Vec<lapi::DisclosedContract>,
}

impl SubmitOptions {
    fn resolve_act_as(&self, fallback: &[String]) -> LedgerResult<Vec<String>> {
        match self.act_as.as_ref() {
            Some(act_as) => {
                if act_as.is_empty() {
                    return Err(LedgerClientError::InvalidArgument {
                        field: "act_as",
                        reason: "must be non-empty",
                    });
                }
                if act_as.iter().any(|party| party.trim().is_empty()) {
                    return Err(LedgerClientError::InvalidArgument {
                        field: "act_as",
                        reason: "must not contain empty parties",
                    });
                }

                Ok(act_as.clone())
            }
            None => Ok(fallback.to_vec()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LedgerClient {
    channel: tonic::transport::Channel,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    user_id: String,
    act_as: Vec<String>,
    retry_policy: RetryPolicy,
}

impl LedgerClient {
    pub async fn connect(
        endpoint_url: &str,
        user_id: impl Into<String>,
        act_as: Vec<String>,
        auth_token: Option<&str>,
    ) -> LedgerResult<Self> {
        let endpoint = Endpoint::from_shared(endpoint_url.to_string())
            .map_err(LedgerClientError::BuildEndpoint)?;
        let channel = endpoint
            .connect()
            .await
            .map_err(LedgerClientError::Connect)?;

        let auth_header = match auth_token {
            Some(token) if !token.trim().is_empty() => Some(parse_auth_header(token)?),
            _ => None,
        };

        Self::from_channel(channel, user_id, act_as, auth_header)
    }

    pub fn from_channel(
        channel: tonic::transport::Channel,
        user_id: impl Into<String>,
        act_as: Vec<String>,
        auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    ) -> LedgerResult<Self> {
        let user_id = user_id.into();
        if user_id.trim().is_empty() {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "user_id",
                reason: "must be non-empty",
            });
        }
        if act_as.is_empty() {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "act_as",
                reason: "must be non-empty",
            });
        }
        if act_as.iter().any(|p| p.trim().is_empty()) {
            return Err(LedgerClientError::InvalidConfiguration {
                field: "act_as",
                reason: "must not contain empty parties",
            });
        }

        let retry_policy = RetryPolicy::default();
        retry_policy.validate()?;

        Ok(Self {
            channel,
            auth_header,
            user_id,
            act_as,
            retry_policy,
        })
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> LedgerResult<Self> {
        retry_policy.validate()?;
        self.retry_policy = retry_policy;
        Ok(self)
    }

    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    pub async fn submit_and_wait_for_transaction(
        &self,
        command_id: String,
        commands: Vec<lapi::Command>,
    ) -> LedgerResult<lapi::Transaction> {
        self.submit_and_wait_for_transaction_with_options(
            command_id,
            commands,
            SubmitOptions::default(),
        )
        .await
    }

    pub async fn submit_and_wait_for_transaction_as(
        &self,
        command_id: String,
        act_as: Vec<String>,
        commands: Vec<lapi::Command>,
    ) -> LedgerResult<lapi::Transaction> {
        let options = SubmitOptions {
            act_as: Some(act_as),
            ..SubmitOptions::default()
        };

        self.submit_and_wait_for_transaction_with_options(command_id, commands, options)
            .await
    }

    pub async fn submit_and_wait_for_transaction_with_options(
        &self,
        command_id: String,
        commands: Vec<lapi::Command>,
        options: SubmitOptions,
    ) -> LedgerResult<lapi::Transaction> {
        let commands =
            build_commands_payload(&self.user_id, &self.act_as, command_id, commands, options)?;
        self.submit_with_retry(commands).await
    }

    async fn submit_with_retry(&self, commands: lapi::Commands) -> LedgerResult<lapi::Transaction> {
        let mut attempt = 1_u32;
        let mut backoff = self.retry_policy.initial_backoff;

        loop {
            let mut client =
                lapi::command_service_client::CommandServiceClient::new(self.channel.clone());
            let mut req = Request::new(lapi::SubmitAndWaitForTransactionRequest {
                commands: Some(commands.clone()),
                transaction_format: None,
            });
            if let Some(auth_header) = self.auth_header.clone() {
                req.metadata_mut().insert("authorization", auth_header);
            }

            match client.submit_and_wait_for_transaction(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    return resp
                        .transaction
                        .ok_or(LedgerClientError::MissingTransaction);
                }
                Err(status) => {
                    let code = status.code();
                    let message = status.message().to_string();
                    let retryable = self.retry_policy.should_retry(code);

                    if retryable && attempt < self.retry_policy.max_attempts {
                        tokio::time::sleep(backoff).await;
                        backoff = self.retry_policy.next_backoff(backoff);
                        attempt = attempt.saturating_add(1);
                        continue;
                    }

                    if retryable {
                        return Err(LedgerClientError::RetryExhausted {
                            attempts: attempt,
                            code,
                            message,
                        });
                    }

                    return Err(LedgerClientError::RpcStatus { code, message });
                }
            }
        }
    }
}

fn build_commands_payload(
    user_id: &str,
    default_act_as: &[String],
    command_id: String,
    commands: Vec<lapi::Command>,
    options: SubmitOptions,
) -> LedgerResult<lapi::Commands> {
    if command_id.trim().is_empty() {
        return Err(LedgerClientError::InvalidArgument {
            field: "command_id",
            reason: "must be non-empty",
        });
    }

    if commands.is_empty() {
        return Err(LedgerClientError::InvalidArgument {
            field: "commands",
            reason: "must be non-empty",
        });
    }

    let SubmitOptions {
        act_as,
        read_as,
        workflow_id,
        submission_id,
        disclosed_contracts,
    } = options;

    let act_as = SubmitOptions {
        act_as,
        read_as: vec![],
        workflow_id: None,
        submission_id: None,
        disclosed_contracts: vec![],
    }
    .resolve_act_as(default_act_as)?;

    Ok(lapi::Commands {
        workflow_id: workflow_id.unwrap_or_default(),
        user_id: user_id.to_string(),
        command_id,
        commands,
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as,
        read_as,
        submission_id: submission_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        disclosed_contracts,
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

pub fn parse_auth_header(token: &str) -> LedgerResult<MetadataValue<tonic::metadata::Ascii>> {
    let token = token.trim();
    if token.is_empty() {
        return Err(LedgerClientError::InvalidArgument {
            field: "auth_token",
            reason: "must be non-empty",
        });
    }

    let header_value = if token.to_ascii_lowercase().starts_with("bearer ") {
        token.to_string()
    } else {
        format!("Bearer {token}")
    };

    header_value
        .parse()
        .map_err(LedgerClientError::InvalidAuthHeader)
}

pub fn identifier(package_id: &str, module_name: &str, entity_name: &str) -> lapi::Identifier {
    lapi::Identifier {
        package_id: package_id.to_string(),
        module_name: module_name.to_string(),
        entity_name: entity_name.to_string(),
    }
}

pub fn record_field(label: &str, value: lapi::Value) -> lapi::RecordField {
    lapi::RecordField {
        label: label.to_string(),
        value: Some(value),
    }
}

pub fn value_record(fields: Vec<lapi::RecordField>) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Record(lapi::Record {
            record_id: None,
            fields,
        })),
    }
}

pub fn value_record_empty() -> lapi::Value {
    value_record(vec![])
}

pub fn value_text(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Text(value.to_string())),
    }
}

pub fn value_int64(value: i64) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Int64(value)),
    }
}

pub fn value_contract_id(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::ContractId(value.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_header_adds_bearer_prefix() {
        let got = parse_auth_header("token123").unwrap();
        assert_eq!(got.as_encoded_bytes(), b"Bearer token123");
    }

    #[test]
    fn parse_auth_header_preserves_existing_prefix() {
        let got = parse_auth_header("Bearer token123").unwrap();
        assert_eq!(got.as_encoded_bytes(), b"Bearer token123");
    }

    #[test]
    fn parse_auth_header_rejects_empty_token() {
        let err = parse_auth_header(" ").unwrap_err();
        assert!(matches!(
            err,
            LedgerClientError::InvalidArgument {
                field: "auth_token",
                reason: "must be non-empty"
            }
        ));
    }

    #[test]
    fn retry_policy_rejects_invalid_configuration() {
        let policy = RetryPolicy {
            max_attempts: 0,
            ..RetryPolicy::default()
        };

        assert!(matches!(
            policy.validate(),
            Err(LedgerClientError::InvalidConfiguration {
                field: "max_attempts",
                reason: "must be > 0"
            })
        ));
    }

    #[test]
    fn build_commands_payload_applies_submit_options() {
        let commands = vec![lapi::Command { command: None }];
        let options = SubmitOptions {
            act_as: Some(vec!["SubmitterParty".to_string()]),
            read_as: vec!["ReadParty".to_string()],
            workflow_id: Some("wf-1".to_string()),
            submission_id: Some("sub-1".to_string()),
            disclosed_contracts: vec![lapi::DisclosedContract::default()],
        };

        let payload = build_commands_payload(
            "user-1",
            &["FallbackParty".to_string()],
            "cmd-1".to_string(),
            commands,
            options,
        )
        .unwrap();

        assert_eq!(payload.user_id, "user-1");
        assert_eq!(payload.command_id, "cmd-1");
        assert_eq!(payload.act_as, vec!["SubmitterParty".to_string()]);
        assert_eq!(payload.read_as, vec!["ReadParty".to_string()]);
        assert_eq!(payload.workflow_id, "wf-1");
        assert_eq!(payload.submission_id, "sub-1");
        assert_eq!(payload.disclosed_contracts.len(), 1);
    }
}
