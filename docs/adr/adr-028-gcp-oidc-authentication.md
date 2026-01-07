# ADR-028: GCP Workload Identity Federation for CI Authentication

**Status**: Proposed

## Context

GitHub Actions currently authenticates to GCP using a long-lived JSON service account key stored as `GCP_SA_KEY_API` in repository secrets. This approach has security concerns:

1. **Key rotation burden**: Manual key rotation required
2. **Blast radius**: Compromised key grants persistent access until revoked
3. **Secret sprawl**: Keys must be stored in GitHub Secrets
4. **Audit complexity**: Harder to trace access back to specific workflows

## Decision

Migrate GitHub Actions GCP authentication from static service account keys to Workload Identity Federation (WIF) using OpenID Connect (OIDC).

### Implementation

#### 1. GCP Infrastructure Setup

Create Workload Identity Pool and OIDC Provider:

```bash
PROJECT_ID="your-project-id"
GITHUB_ORG="daxis-io"

gcloud iam workload-identity-pools create "github" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools providers create-oidc "arco-repo" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github" \
  --display-name="Arco Repository Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository_owner == '${GITHUB_ORG}'" \
  --issuer-uri="https://token.actions.githubusercontent.com"
```

#### 2. Service Account IAM Binding

Allow the Workload Identity Pool to impersonate the service account:

```bash
SERVICE_ACCOUNT="github-actions@${PROJECT_ID}.iam.gserviceaccount.com"
WORKLOAD_IDENTITY_POOL_ID=$(gcloud iam workload-identity-pools describe "github" \
  --project="${PROJECT_ID}" --location="global" --format="value(name)")

gcloud iam service-accounts add-iam-policy-binding "${SERVICE_ACCOUNT}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${GITHUB_ORG}/arco"
```

#### 3. GitHub Actions Workflow Update

Update the `iam-smoke` job in `.github/workflows/ci.yml`:

```yaml
iam-smoke:
  name: IAM Smoke Tests
  runs-on: ubuntu-latest
  if: github.event_name == 'push' && github.ref == 'refs/heads/main'
  environment: dev
  needs: [check, test]
  permissions:
    contents: read
    id-token: write
  steps:
    - uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683
    - uses: dtolnay/rust-toolchain@4be9e76fd7c4901c61fb841f559994984270fce7
      with:
        toolchain: 1.85
    - uses: Swatinem/rust-cache@ad397744b0d591a723ab90405b7247fac0e6b8db

    - name: Authenticate to GCP
      uses: google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093
      with:
        workload_identity_provider: 'projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github/providers/arco-repo'
        service_account: 'github-actions@PROJECT_ID.iam.gserviceaccount.com'

    - name: Run API IAM smoke tests
      env:
        ARCO_TEST_BUCKET: ${{ vars.ARCO_BUCKET_DEV }}
      run: |
        cargo test --package arco-integration-tests \
          --features iam-smoke \
          -- --ignored --test-threads=1
```

#### 4. Environment Variables

Store these in GitHub repository variables (not secrets):
- `GCP_WORKLOAD_IDENTITY_PROVIDER`: The full provider resource name
- `GCP_SERVICE_ACCOUNT`: The service account email

#### 5. Cleanup

After migration is verified:
1. Remove `GCP_SA_KEY_API` from GitHub Secrets
2. Rotate and eventually delete the JSON key in GCP

## Consequences

### Benefits

- **No static credentials**: Authentication happens via short-lived OIDC tokens
- **Automatic rotation**: Tokens expire in 5 minutes
- **Fine-grained access**: Attribute conditions restrict which repos/branches can authenticate
- **Better audit trail**: GCP logs show the specific workflow run that authenticated

### Trade-offs

- **GCP setup required**: One-time Workload Identity Pool configuration
- **Propagation delay**: New pools/providers need ~5 minutes before first use
- **Token lifetime**: Maximum 10 minutes for ID tokens

### Security Considerations

- Use `repository_owner_id` instead of `repository_owner` for stronger security (prevents org name squatting)
- For production deployments, add branch restrictions: `assertion.ref == 'refs/heads/main'`
- Use GitHub Environments with protection rules for additional security gates

## References

- [GitHub: Configuring OIDC in GCP](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-google-cloud-platform)
- [GCP: Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation-with-deployment-pipelines)
- [google-github-actions/auth](https://github.com/google-github-actions/auth)
