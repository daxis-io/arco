//! Arco Flow orchestration dispatcher service.

use std::net::SocketAddr;
use std::sync::Arc;

use arco_core::observability::{LogFormat, init_logging};
use arco_core::{
    DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TOKEN_TTL_SECONDS, ScopedStorage,
    TaskTokenConfig,
};
use arco_flow::dispatch::HttpTaskEnqueuer;
#[cfg(feature = "http-client")]
use arco_flow::dispatch::OperatorHttpEnqueuer;
use arco_flow::dispatch::cloud_tasks::{
    CloudTasksConfig, CloudTasksDispatcher, resolve_target_audience,
};
use arco_flow::dispatch::worker_auth::worker_dispatch_headers;
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::dispatcher_service::{
    DispatcherServiceConfig, DispatcherServiceState, router,
};
use arco_storage::from_bucket;

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    std::env::var(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
}

fn parse_u64_env(key: &str, default: u64) -> Result<u64> {
    parse_u64_value(optional_env(key).as_deref(), key, default)
}

fn parse_u64_value(raw: Option<&str>, key: &str, default: u64) -> Result<u64> {
    let Some(raw) = raw else {
        return Ok(default);
    };

    raw.parse::<u64>()
        .map_err(|_| Error::configuration(format!("invalid {key}")))
}

fn task_token_config_from_env(task_timeout_secs: u64) -> Result<TaskTokenConfig> {
    task_token_config_from_parts(
        required_env("ARCO_FLOW_TASK_TOKEN_SECRET")?,
        optional_env("ARCO_FLOW_TASK_TOKEN_ISSUER"),
        optional_env("ARCO_FLOW_TASK_TOKEN_AUDIENCE"),
        parse_u64_env(
            "ARCO_FLOW_TASK_TOKEN_TTL_SECS",
            DEFAULT_TASK_TOKEN_TTL_SECONDS,
        )?,
        task_timeout_secs,
    )
}

fn task_token_config_from_parts(
    hs256_secret: String,
    issuer: Option<String>,
    audience: Option<String>,
    ttl_seconds: u64,
    task_timeout_secs: u64,
) -> Result<TaskTokenConfig> {
    let config = TaskTokenConfig {
        hs256_secret,
        issuer,
        audience,
        ttl_seconds,
    };
    config
        .validate_for_dispatch(task_timeout_secs, true)
        .map_err(|e| Error::configuration(e.to_string()))?;
    Ok(config)
}

fn task_timeout_seconds_from_env() -> Result<u64> {
    let timeout = parse_u64_env(
        "ARCO_FLOW_TASK_TIMEOUT_SECS",
        DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
    )?;
    validate_task_timeout_seconds(timeout)
}

fn validate_task_timeout_seconds(timeout: u64) -> Result<u64> {
    if timeout == 0 {
        return Err(Error::configuration(
            "ARCO_FLOW_TASK_TIMEOUT_SECS must be greater than zero",
        ));
    }
    Ok(timeout)
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_FLOW_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_FLOW_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

#[allow(clippy::unused_async)]
async fn build_cloud_tasks(config: CloudTasksConfig) -> Result<CloudTasksDispatcher> {
    #[cfg(feature = "gcp")]
    {
        CloudTasksDispatcher::new(config).await
    }

    #[cfg(not(feature = "gcp"))]
    {
        CloudTasksDispatcher::new(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let dispatch_target_url = required_env("ARCO_FLOW_DISPATCH_TARGET_URL")?;
    let dispatch_target_audience_env = optional_env("ARCO_FLOW_DISPATCH_TARGET_AUDIENCE");
    let callback_base_url = required_env("ARCO_FLOW_CALLBACK_BASE_URL")?;
    let orch_compactor_url = optional_env("ARCO_FLOW_COMPACTOR_URL");
    let transport =
        optional_env("ARCO_FLOW_WORKER_TRANSPORT").unwrap_or_else(|| "cloud_tasks".to_string());
    let timer_target_url = if transport == "http" {
        None
    } else {
        optional_env("ARCO_FLOW_TIMER_TARGET_URL")
    };
    let timer_target_audience_env = optional_env("ARCO_FLOW_TIMER_TARGET_AUDIENCE");
    let timer_queue = optional_env("ARCO_FLOW_TIMER_QUEUE");
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let task_timeout_secs = task_timeout_seconds_from_env()?;
    let task_token_config = task_token_config_from_env(task_timeout_secs)?;
    let worker_dispatch_headers =
        worker_dispatch_headers(&required_env("ARCO_FLOW_WORKER_DISPATCH_SECRET")?)?;
    let port = resolve_port()?;
    let dispatch_target_audience = resolve_target_audience(
        dispatch_target_audience_env.as_deref(),
        &dispatch_target_url,
    );
    let timer_target_audience = timer_target_url.as_deref().map(|target_url| {
        resolve_target_audience(timer_target_audience_env.as_deref(), target_url)
    });

    let task_queue: Arc<dyn HttpTaskEnqueuer> = match transport.as_str() {
        "http" => {
            #[cfg(feature = "http-client")]
            {
                Arc::new(OperatorHttpEnqueuer::new(
                    required_env("ARCO_FLOW_HTTP_INGRESS_URL")?,
                    required_env("ARCO_FLOW_HTTP_INGRESS_TOKEN")?,
                )?)
            }
            #[cfg(not(feature = "http-client"))]
            {
                return Err(Error::configuration(
                    "HTTP transport requires http-client feature",
                ));
            }
        }
        "cloud_tasks" => {
            let mut config = CloudTasksConfig::new(
                required_env("ARCO_GCP_PROJECT_ID")?,
                required_env("ARCO_GCP_LOCATION")?,
                optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string()),
                dispatch_target_url.clone(),
            );
            if let Some(email) = service_account_email {
                config = config.with_service_account(email);
            }
            if !parse_bool_env("ARCO_FLOW_APPLY_QUEUE_RETRY_CONFIG", false) {
                config = config.with_queue_retry_updates(false);
            }
            config = config.with_task_timeout(std::time::Duration::from_secs(task_timeout_secs));
            Arc::new(build_cloud_tasks(config).await?)
        }
        _ => {
            return Err(Error::configuration(
                "ARCO_FLOW_WORKER_TRANSPORT must be cloud_tasks or http",
            ));
        }
    };

    let backend = from_bucket(&bucket)?;
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = DispatcherServiceState::new(
        storage,
        task_queue,
        DispatcherServiceConfig {
            orch_compactor_url,
            worker_dispatch_headers,
            dispatch_target_url,
            dispatch_target_audience,
            callback_base_url,
            task_token_config,
            task_timeout_secs,
            timer_target_url,
            timer_target_audience,
            timer_queue,
        },
    )?;

    let app = router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_token_config_from_parts_rejects_missing_issuer() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            None,
            Some("audience".to_string()),
            3_600,
            1_800,
        )
        .expect_err("missing issuer must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn task_token_config_from_parts_rejects_missing_audience() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            Some("issuer".to_string()),
            None,
            3_600,
            1_800,
        )
        .expect_err("missing audience must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn parse_u64_value_rejects_invalid_timeout_env() {
        let err = parse_u64_value(
            Some("not-a-number"),
            "ARCO_FLOW_TASK_TIMEOUT_SECS",
            DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
        )
        .expect_err("invalid timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn validate_task_timeout_seconds_rejects_zero() {
        let err = validate_task_timeout_seconds(0).expect_err("zero timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }
}
