# Runbook: Pen Test Scope

## Status
Deferred (requires staging environment)

## Overview
Define the scope and rules of engagement for an external penetration test.

## Preconditions
- Approved staging environment
- Data classification and redaction plan
- Contact list for incident response

## Scope
- In-scope APIs: `/api/v1/*`, `/iceberg/v1/*`
- In-scope storage paths: tenant/workspace-scoped buckets
- Out-of-scope: production systems, customer data, third-party integrations

## Procedure
1. Draft scope statement and testing window.
2. Validate access credentials and logging.
3. Conduct test with agreed rules of engagement.
4. Review findings and remediation plan.

## Evidence to capture
- Signed scope document
- Final report and remediation tracker
