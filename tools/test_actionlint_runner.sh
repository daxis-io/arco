#!/usr/bin/env bash
set -euo pipefail

help_output="$(scripts/run_actionlint.sh --help)"
[[ "$help_output" == *"ACTIONLINT_BIN"* ]]

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fake_actionlint="$tmpdir/actionlint"
log_file="$tmpdir/actionlint.args"
cat >"$fake_actionlint" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$ACTIONLINT_ARG_LOG"
SH
chmod +x "$fake_actionlint"

ACTIONLINT_BIN="$fake_actionlint" ACTIONLINT_ARG_LOG="$log_file" scripts/run_actionlint.sh
grep -q ".github/workflows/ci.yml" "$log_file"

if ACTIONLINT_BIN="$tmpdir/missing-actionlint" scripts/run_actionlint.sh >/tmp/actionlint-missing.out 2>/tmp/actionlint-missing.err; then
  echo "expected missing actionlint to fail" >&2
  exit 1
fi
grep -q "actionlint was not found" /tmp/actionlint-missing.err

echo "actionlint runner smoke passed"
