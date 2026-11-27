//! Credential-free error observations for the one-shot hosted qualification.

use arco_core::{Error, Result};
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt};

const TARGET: &str = "worker_pull_hosted_fixture";

pub fn install() -> Result<()> {
    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .with_filter(filter_fn(|metadata| {
            metadata.target() == TARGET && *metadata.level() == tracing::Level::WARN
        }));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .map_err(|_| Error::storage("configure hosted fixture diagnostics"))
}

// Values from the error chain are used only to recognize status and vetted codes.
#[derive(Debug, Default, PartialEq, Eq)]
struct ProviderCause {
    status: Option<u16>,
    code: Option<&'static str>,
}

fn provider_cause(error: &Error) -> ProviderCause {
    const CODES: &[&str] = &[
        "AccessDenied",
        "AuthorizationHeaderMalformed",
        "ConditionalRequestConflict",
        "ExpiredToken",
        "InternalError",
        "InvalidAccessKeyId",
        "InvalidRequest",
        "InvalidToken",
        "NoSuchBucket",
        "NoSuchKey",
        "OperationAborted",
        "PermanentRedirect",
        "PreconditionFailed",
        "RequestTimeTooSkewed",
        "RequestTimeout",
        "ServiceUnavailable",
        "SignatureDoesNotMatch",
        "SlowDown",
        "Throttling",
    ];
    let mut cause = ProviderCause::default();
    let mut next: Option<&dyn std::error::Error> = Some(error);
    for _ in 0..8 {
        let Some(source) = next else { break };
        let message = source.to_string();
        if cause.status.is_none() {
            cause.status = message
                .split_once("with status ")
                .and_then(|(_, tail)| tail.get(..3))
                .and_then(|status| status.parse::<u16>().ok())
                .filter(|status| (400..600).contains(status));
        }
        if cause.code.is_none() {
            cause.code = message
                .split_once("<Code>")
                .and_then(|(_, tail)| tail.split_once("</Code>"))
                .and_then(|(code, _)| CODES.iter().copied().find(|allowed| *allowed == code));
        }
        next = source.source();
    }
    cause
}

pub fn storage_failure(operation: &str, object: &str, condition: &str, error: &Error) {
    let cause = provider_cause(error);
    let class = match error {
        Error::Storage { .. } => "storage",
        Error::NotFound(_) => "not_found",
        Error::PreconditionFailed { .. } => "precondition",
        _ => "other",
    };
    tracing::warn!(
        target: TARGET,
        operation,
        object,
        condition,
        class,
        http_status = cause.status,
        provider_code = cause.code,
        "hosted S3 operation failed"
    );
}

pub fn retry_response(reason: Option<&str>) {
    let reason = match reason {
        Some("worker-pull authority needs retry") => "authority",
        Some("worker-pull invocation timed out") => "timeout",
        _ => "retryable",
    };
    tracing::warn!(target: TARGET, reason, http_status = 503, "hosted public route deferred");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warning_keeps_status_and_known_code_without_echoing_provider_body() {
        let error = Error::storage_with_source(
            "object store failure",
            std::io::Error::other(
                "Client error with status 409 Conflict: <Error><Code>ConditionalRequestConflict</Code>                 <Message>signed provider body must never be logged</Message></Error>",
            ),
        );
        assert_eq!(
            provider_cause(&error),
            ProviderCause {
                status: Some(409),
                code: Some("ConditionalRequestConflict"),
            }
        );
        let injected = Error::storage(
            "Client error with status 403 Forbidden: <Code>untrusted-token-value</Code>",
        );
        assert_eq!(
            provider_cause(&injected),
            ProviderCause {
                status: Some(403),
                code: None
            }
        );
    }
}
