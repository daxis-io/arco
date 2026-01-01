# Runbook: Rollback Drill

## Status
Deferred (requires staging environment and release process)

## Overview
Verify rollback safety for API, compactor, and flow services.

## Preconditions
- Tagged release artifact and previous known-good tag
- Deployment pipeline with rollback capability
- Staging environment

## Procedure
1. Deploy candidate release to staging.
2. Run smoke tests for catalog and query endpoints.
3. Roll back to the previous release tag.
4. Re-run smoke tests and validate manifests unchanged.

## Evidence to capture
- Deployment logs for forward and rollback
- Smoke test results
- Manifest hash comparison before and after rollback
