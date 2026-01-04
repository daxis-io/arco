# Runbook: Metrics Access

## Scope

This runbook documents the metrics access strategy for:

- The Arco API service (`crates/arco-api/`)
- The Arco compactor service (`crates/arco-compactor/`)

It focuses on how `/metrics` is exposed (or intentionally not exposed) in different deployment postures, and what operators should do to collect metrics safely.

## Decision Summary

- API `/metrics` is intentionally unavailable in `Posture::Public` (HTTP 404) to prevent exposing attacker-amplifiable, high-cardinality metrics surfaces on a public endpoint.
- API `/metrics` remains available (unauthenticated) in `Posture::Dev` and `Posture::Private` where the service is not publicly reachable.
- Compactor `/metrics` remains mounted without auth in code, and is protected via infrastructure controls:
  - Internal-only ingress
  - Cloud Run IAM (restrict which principals can invoke the service)

Implementation references:

- API `/metrics` gating: `crates/arco-api/src/server.rs`
- API metrics exporter and `/metrics` handler: `crates/arco-api/src/metrics.rs`
- Compactor `/metrics` route: `crates/arco-compactor/src/main.rs`
- Cloud Run ingress and deployment wiring: `infra/terraform/cloud_run.tf`

## API: `/metrics` Behavior By Posture

Source: `crates/arco-api/src/server.rs` (handler and router wiring).

| Posture | Reachability intent | GET /metrics |
|---|---|---|
| Dev | local/CI, not internet-facing | 200 OK (Prometheus text) |
| Private | internal deployment, not internet-facing | 200 OK (Prometheus text) |
| Public | internet-facing | 404 Not Found |

Notes:

- The API currently exports Prometheus-formatted metrics via the `metrics_exporter_prometheus` recorder (`crates/arco-api/src/metrics.rs`).
- In `Posture::Public`, the API does not provide an alternate authenticated metrics endpoint; it is simply not available via HTTP.

## Compactor: `/metrics` Access Model

Source: `crates/arco-compactor/src/main.rs` (router wiring).

- The compactor serves `/metrics` without an auth gate in code.
- The intended protection is infrastructure-level:
  - On GCP Cloud Run, the compactor service is configured with internal-only ingress (`infra/terraform/cloud_run.tf`).
  - Cloud Run IAM is used to restrict who can invoke the service (for example, scheduled triggers use OIDC tokens; see `infra/terraform/cloud_run.tf`).

Operational implication:

- Treat the compactor metrics endpoint as safe only when the network path is internal and IAM does not allow broad invocation.

## Residual Risk: Non-Cloud-Run Deployments

If the compactor is deployed outside Cloud Run (or without equivalent controls), `/metrics` may become reachable without authentication.

Residual risks include:

- Unauthenticated metrics exposure to untrusted networks
- Increased attack surface for resource exhaustion (scrape amplification)
- Information disclosure via metric names/labels

Required mitigations in non-Cloud-Run environments:

- Ensure the compactor listener is only reachable from a trusted network segment.
- Prefer an internal load balancer / ingress that enforces authentication and rate limits.
- Do not expose `/metrics` directly on a public interface.

## Operational Guidance: Public Deployments

### What changes in Public posture

- API `/metrics` returns 404 in `Posture::Public` and cannot be scraped, even from internal networks.
- Compactor `/metrics` is intended to remain private (internal-only) and can still be scraped internally if the network path exists.

### Recommended collection strategy

For deployments that must run the API in `Posture::Public`, treat metrics collection as a push pipeline (OTLP) rather than direct Prometheus scraping of the public API.

Collector reference:

- Baseline collector config: `infra/monitoring/otel-collector.yaml`

Important reality check:

- The checked-in `infra/monitoring/otel-collector.yaml` configures both a Prometheus receiver and an OTLP receiver; the metrics pipeline accepts either.
- The API currently exports Prometheus metrics only (`crates/arco-api/src/metrics.rs`) and does not provide an OTLP metrics exporter.

As a result, collecting API metrics in `Posture::Public` requires one of the following operational approaches:

1. Preferred (future-facing): Add OTLP metric export from services and configure the collector to receive OTLP.
2. Alternative (private-only): Keep API in `Posture::Private` (not internet-facing) so an internal collector can scrape `/metrics`.

### OTLP push pipeline (operator steps)

This is the intended model for public deployments:

1. Run an OpenTelemetry Collector reachable only from trusted networks.
2. Configure services to export metrics via OTLP to that collector.
3. Export from the collector to the destination backend (for example, Google Cloud Monitoring).

A minimal collector adjustment (example only) looks like:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [googlecloud]
```

Use `infra/monitoring/otel-collector.yaml` as the baseline and extend it with an OTLP receiver and an OTLP-based metrics pipeline appropriate for your environment.

### Internal scraping for compactor

Even in public API deployments, you can typically keep scraping compactor metrics internally if:

- The compactor remains internal-only (`infra/terraform/cloud_run.tf`), and
- Only trusted principals can invoke it (Cloud Run IAM), and
- Your collector runs in the same private network context.

## Troubleshooting

- API in Public posture returns 404 for `/metrics`:
  - This is expected. Do not treat it as an outage; use a public-safe metrics strategy instead.
- Compactor metrics unexpectedly reachable from the internet:
  - Treat as a security incident. Verify ingress settings and IAM restrictions in `infra/terraform/cloud_run.tf` and any external load balancers.
