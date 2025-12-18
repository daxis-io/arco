//! Log redaction helpers.
//!
//! Signed URLs contain credentials in query parameters and MUST NOT be logged in full.

/// Returns a copy of `url` with sensitive query parameter values replaced.
///
/// This is intended for log output only.
#[allow(dead_code)]
pub fn redact_url(url: &str) -> String {
    // Keep it simple and allocation-friendly: scan for known patterns and
    // replace `key=value` with `key=REDACTED`.
    const REDACT_PATTERNS: [&str; 3] = ["sig=", "signature=", "token="];

    let mut redacted = url.to_string();
    for pattern in REDACT_PATTERNS {
        if let Some(start) = redacted.find(pattern) {
            let end = redacted[start..]
                .find('&')
                .map_or_else(|| redacted.len(), |i| start + i);

            let key = pattern.trim_end_matches('=');
            redacted.replace_range(start..end, &format!("{key}=REDACTED"));
        }
    }

    redacted
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
}
