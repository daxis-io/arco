# Threat Model Notes - Signed URLs and Multi-Tenant Object Storage

Date: 2025-12-30
Scope: Signed URL minting, browser direct read path, multi-tenant isolation on object storage.
Method: STRIDE summary with controls mapped to Arco architecture/ADRs.

## Spoofing
- Threat: Stolen or replayed signed URLs enable unauthorized access.
- Controls: Short TTLs, allowlist enforcement, tenant/workspace scope in path, correlation metadata on mint.
- Evidence: docs/adr/adr-019-existence-privacy.md; crates/arco-api/tests/browser_e2e.rs; infra/terraform/iam_conditions.tf

## Tampering
- Threat: Path manipulation to access non-allowlisted objects or other tenants.
- Controls: Path traversal rejection and domain allowlist; manifest-driven allowlist.
- Evidence: crates/arco-api/tests/browser_e2e.rs; crates/arco-catalog/src/reader.rs

## Repudiation
- Threat: Users deny access to data via signed URL.
- Controls: Audit correlation metadata at mint time; log redaction policy for signed URLs.
- Evidence: docs/adr/adr-019-existence-privacy.md; docs/runbooks/metrics-catalog.md

## Information Disclosure
- Threat: Cross-tenant object exposure or URL leakage via logs.
- Controls: IAM conditions on object prefixes; signed URL TTL bounds; no query param logging.
- Evidence: infra/terraform/iam_conditions.tf; crates/arco-api/tests/api_integration.rs; docs/adr/adr-019-existence-privacy.md

## Denial of Service
- Threat: Abuse of URL minting endpoint or excessive signed URL generation.
- Controls: API rate limiting; TTL bounds.
- Evidence: crates/arco-api/tests/api_integration.rs (rate limiting behavior); docs/plans/2025-12-17-part5-catalog-productization.md (rate limit plan)

## Elevation of Privilege
- Threat: Minting URLs for unauthorized domains or paths.
- Controls: Domain allowlist + manifest-driven path allowlist; tenant/workspace headers required.
- Evidence: crates/arco-api/tests/browser_e2e.rs; crates/arco-api/tests/api_integration.rs

## References (external)
- https://docs.aws.amazon.com/prescriptive-guidance/latest/presigned-url-best-practices/overview.html
- https://cheatsheetseries.owasp.org/cheatsheets/Threat_Modeling_Cheat_Sheet.html
- https://github.com/trustoncloud/threatmodel-for-aws-s3
