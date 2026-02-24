//! Log redaction helpers.
//!
//! Signed URLs contain credentials in query parameters and MUST NOT be logged in full.

/// Returns a copy of `url` with sensitive query parameter values replaced.
///
/// This is intended for log output only.
#[allow(dead_code)]
pub fn redact_url(url: &str) -> String {
    if let Ok(mut parsed) = reqwest::Url::parse(url) {
        let mut changed = false;
        let mut pairs = Vec::new();

        for (key, value) in parsed.query_pairs() {
            let key = key.into_owned();
            if is_sensitive_query_key(&key) {
                pairs.push((key, "REDACTED".to_string()));
                changed = true;
            } else {
                pairs.push((key, value.into_owned()));
            }
        }

        if !changed {
            return url.to_string();
        }

        {
            let mut query = parsed.query_pairs_mut();
            query.clear();
            for (key, value) in pairs {
                query.append_pair(&key, &value);
            }
        }

        return parsed.to_string();
    }

    redact_query_fallback(url)
}

fn redact_query_fallback(value: &str) -> String {
    let Some(query_start) = value.find('?') else {
        return value.to_string();
    };

    let prefix = &value[..=query_start];
    let query_and_fragment = &value[query_start + 1..];
    let (query, fragment) = match query_and_fragment.split_once('#') {
        Some((query, fragment)) => (query, Some(fragment)),
        None => (query_and_fragment, None),
    };

    let mut redacted_query = String::new();
    for (index, pair) in query.split('&').enumerate() {
        if index > 0 {
            redacted_query.push('&');
        }

        let Some((key, raw_value)) = pair.split_once('=') else {
            redacted_query.push_str(pair);
            continue;
        };

        if is_sensitive_query_key(key) {
            redacted_query.push_str(key);
            redacted_query.push_str("=REDACTED");
        } else {
            redacted_query.push_str(key);
            redacted_query.push('=');
            redacted_query.push_str(raw_value);
        }
    }

    match fragment {
        Some(fragment) => format!("{prefix}{redacted_query}#{fragment}"),
        None => format!("{prefix}{redacted_query}"),
    }
}

fn is_sensitive_query_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "sig"
            | "signature"
            | "token"
            | "x-id-token"
            | "x-amz-signature"
            | "x-amz-security-token"
            | "x-amz-credential"
            | "x-goog-signature"
            | "x-goog-credential"
            | "x-ms-signature"
            | "x-ms-credential"
            | "se"
            | "sp"
            | "sr"
            | "sv"
            | "skoid"
            | "sktid"
            | "skt"
            | "ske"
            | "sks"
            | "skv"
    ) || normalized.starts_with("x-amz-")
        || normalized.starts_with("x-goog-")
        || normalized.starts_with("x-ms-")
}

/// A URL wrapper that redacts sensitive query parameters in `Display`/`Debug`.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct RedactedUrl<'a>(pub &'a str);

impl std::fmt::Display for RedactedUrl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", redact_url(self.0))
    }
}

impl std::fmt::Debug for RedactedUrl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", redact_url(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_redacts_sig_token_signature() {
        let url = "http://localhost/objects/path?expires=123&sig=abc&token=def&signature=ghi";
        let redacted = redact_url(url);
        assert!(redacted.contains("expires=123"));
        assert!(redacted.contains("sig=REDACTED"));
        assert!(redacted.contains("token=REDACTED"));
        assert!(redacted.contains("signature=REDACTED"));
        assert!(!redacted.contains("sig=abc"));
        assert!(!redacted.contains("token=def"));
        assert!(!redacted.contains("signature=ghi"));
    }

    #[test]
    fn redact_url_no_query_is_unchanged() {
        let url = "http://localhost/objects/path";
        assert_eq!(redact_url(url), url);
    }

    #[test]
    fn redacted_url_display_is_redacted() {
        let url = "http://localhost/objects/path?sig=abc";
        let s = RedactedUrl(url).to_string();
        assert_eq!(s, "http://localhost/objects/path?sig=REDACTED");
    }

    #[test]
    fn redact_url_redacts_aws_presign_params() {
        let url = "https://storage.example/object.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA/20260223/us-east-1/s3/aws4_request&X-Amz-Date=20260223T120000Z&X-Amz-Expires=900&X-Amz-Signature=abc123";
        let redacted = redact_url(url);
        assert!(redacted.contains("X-Amz-Algorithm=REDACTED"));
        assert!(redacted.contains("X-Amz-Credential=REDACTED"));
        assert!(redacted.contains("X-Amz-Date=REDACTED"));
        assert!(redacted.contains("X-Amz-Signature=REDACTED"));
        assert!(!redacted.contains("X-Amz-Signature=abc123"));
    }

    #[test]
    fn redact_url_redacts_google_signed_url_params() {
        let url = "https://storage.googleapis.com/bucket/file.parquet?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=test%40example.iam.gserviceaccount.com%2F20260223%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260223T120000Z&X-Goog-Expires=900&X-Goog-Signature=deadbeef";
        let redacted = redact_url(url);
        assert!(redacted.contains("X-Goog-Algorithm=REDACTED"));
        assert!(redacted.contains("X-Goog-Credential=REDACTED"));
        assert!(redacted.contains("X-Goog-Date=REDACTED"));
        assert!(redacted.contains("X-Goog-Signature=REDACTED"));
        assert!(!redacted.contains("X-Goog-Signature=deadbeef"));
    }

    #[test]
    fn redact_url_is_case_insensitive_for_sensitive_keys() {
        let url = "http://localhost/objects/path?Token=abc&Signature=def&SIG=ghi";
        let redacted = redact_url(url);
        assert!(redacted.contains("Token=REDACTED"));
        assert!(redacted.contains("Signature=REDACTED"));
        assert!(redacted.contains("SIG=REDACTED"));
        assert!(!redacted.contains("Token=abc"));
        assert!(!redacted.contains("Signature=def"));
        assert!(!redacted.contains("SIG=ghi"));
    }
}
