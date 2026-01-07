#!/usr/bin/env bash
set -euo pipefail

REPO="daxis-io/arco"

echo "=== Phase 1.1: Enable Security Features ==="

echo "Enabling Dependabot vulnerability alerts..."
gh api "repos/${REPO}/vulnerability-alerts" -X PUT

echo "Enabling Dependabot security updates..."
gh api "repos/${REPO}/automated-security-fixes" -X PUT

echo "Enabling secret scanning and push protection..."
gh api "repos/${REPO}" -X PATCH \
  -F security_and_analysis[secret_scanning][status]=enabled \
  -F security_and_analysis[secret_scanning_push_protection][status]=enabled

echo "Enabling private vulnerability reporting..."
gh api "repos/${REPO}/private-vulnerability-reporting" -X PUT

echo "Enabling CodeQL default setup..."
gh api "repos/${REPO}/code-scanning/default-setup" -X PATCH \
  -F state=configured \
  -F languages='["python","actions"]' \
  -F query_suite=default

echo ""
echo "=== Phase 1.2: Set Merge Policy ==="

echo "Setting squash-only merge, auto-delete branches..."
gh api "repos/${REPO}" -X PATCH \
  -F allow_merge_commit=false \
  -F allow_rebase_merge=false \
  -F allow_squash_merge=true \
  -F delete_branch_on_merge=true

echo ""
echo "=== Phase 1.3: Restrict Actions ==="

echo "Restricting to GitHub and verified creators only..."
gh api "repos/${REPO}/actions/permissions" -X PUT \
  -F enabled=true \
  -F allowed_actions=selected

gh api "repos/${REPO}/actions/permissions/selected-actions" -X PUT \
  -F github_owned_allowed=true \
  -F verified_allowed=true \
  -F patterns='[]'

echo ""
echo "=== Phase 2: Create Branch Ruleset ==="

echo "Creating protect-main ruleset..."
gh api "repos/${REPO}/rulesets" -X POST \
  -F name="protect-main" \
  -F target=branch \
  -F enforcement=active \
  --input - << 'EOF'
{
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/main"],
      "exclude": []
    }
  },
  "rules": [
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 1,
        "dismiss_stale_reviews_on_push": true,
        "require_code_owner_review": true,
        "require_last_push_approval": false,
        "required_review_thread_resolution": true
      }
    },
    {
      "type": "required_signatures"
    },
    {
      "type": "deletion"
    },
    {
      "type": "non_fast_forward"
    }
  ]
}
EOF

echo ""
echo "=== Phase 3.1a: Create dev Environment ==="

gh api "repos/${REPO}/environments/dev" -X PUT \
  --input - << 'EOF'
{
  "deployment_branch_policy": {
    "protected_branches": false,
    "custom_branch_policies": true
  }
}
EOF

gh api "repos/${REPO}/environments/dev/deployment-branch-policies" -X POST \
  -F name="main" \
  -F type="branch"

echo ""
echo "=== Phase 3.1b: Create prod Environment ==="

gh api "repos/${REPO}/environments/prod" -X PUT \
  --input - << 'EOF'
{
  "wait_timer": 5,
  "reviewers": [
    {"type": "User", "id": 0}
  ],
  "deployment_branch_policy": {
    "protected_branches": false,
    "custom_branch_policies": true
  }
}
EOF

gh api "repos/${REPO}/environments/prod/deployment-branch-policies" -X POST \
  -F name="release/*" \
  -F type="branch"

echo ""
echo "=== Verification ==="

echo "Checking security settings..."
gh api "repos/${REPO}" | jq '{
  security_and_analysis,
  allow_squash_merge,
  allow_merge_commit,
  allow_rebase_merge,
  delete_branch_on_merge
}'

echo ""
echo "Checking Actions permissions..."
gh api "repos/${REPO}/actions/permissions"

echo ""
echo "Checking rulesets..."
gh api "repos/${REPO}/rulesets" | jq 'length'

echo ""
echo "Checking environments..."
gh api "repos/${REPO}/environments" | jq '.total_count'

echo ""
echo "=== Setup Complete ==="
echo "NOTE: After CI passes on main, update the ruleset to add required status checks:"
echo "  Gates, Check, Format, Clippy, Test, Docs, Doc Tests, Python Tests,"
echo "  Cargo Deny, Cargo Deny (Advisories), Proto Compatibility, Compile-Negative Tests"
