#!/usr/bin/env bash
# Daily-maintenance scan for the Weft engine repo.
#
# This is the DETERMINISTIC half of the daily automation: it runs the cheap-core
# quality + security gates and writes machine-readable reports under
# target/daily-maintenance/. The Cursor agent (.cursor/rules/daily-maintenance.mdc)
# then reads those reports and does the triage / PR / advisory work.
#
# Deliberately does NOT run the heavy bench/parity gates (tpch, clickbench x2,
# correctness, parity) — those live in scripts/ci-local.sh and are too slow for a
# daily pass. Run those in a separate weekly agent if you want deeper coverage.
#
# Each step is wrapped so one failure does not abort the rest: we want the full
# picture in one run, not a stop at the first red gate. Individual exit codes are
# recorded in target/daily-maintenance/summary.txt.

set -uo pipefail
cd "$(dirname "$0")/.."

OUT="target/daily-maintenance"
mkdir -p "$OUT"
: > "$OUT/summary.txt"

# run <label> <report-file> <cmd...> — run a scan step, tee to report, record status.
run() {
  local label="$1"; shift
  local report="$1"; shift
  echo "==> $label"
  local status=0
  "$@" > "$OUT/$report" 2>&1 || status=$?
  if [ "$status" -eq 0 ]; then
    echo "  OK    $label -> $report"
    printf '%-24s OK    (%s)\n' "$label" "$report" >> "$OUT/summary.txt"
  else
    echo "  FLAG  $label (exit $status) -> $report"
    printf '%-24s FLAG  exit=%s (%s)\n' "$label" "$status" "$report" >> "$OUT/summary.txt"
  fi
  return 0
}

echo "# daily-maintenance scan — reports in $OUT/"

# --- formatting: non-zero exit means the tree is unformatted (a finding) ---
run "rustfmt-check"   "fmt.diff"        cargo fmt --all -- --check

# --- weft-cli MUST be built before tests/clippy (binary-only crate; AGENTS.md) ---
run "build-weft-cli"  "build-cli.log"   cargo build -p weft-cli

# --- clippy: JSON so the agent can parse individual lints; -D warnings = gate ---
run "clippy"          "clippy.json"     cargo clippy --workspace --all-targets --message-format=json -- -D warnings

# --- test suite: failures are bugs to triage ---
run "test"            "test.log"        cargo test --workspace

# --- dependency CVEs: RUSTSEC advisories (first real security signal for this repo) ---
if command -v cargo-audit >/dev/null 2>&1; then
  run "cargo-audit"   "audit.json"      cargo audit --json
else
  echo "  MISSING cargo-audit — install with: cargo install cargo-audit --locked"
  printf '%-24s MISSING (install cargo-audit)\n' "cargo-audit" >> "$OUT/summary.txt"
fi

# --- advisories + licenses + bans + yanked, per deny.toml ---
if command -v cargo-deny >/dev/null 2>&1; then
  run "cargo-deny"    "deny.log"        cargo deny check
else
  echo "  MISSING cargo-deny — install with: cargo install cargo-deny --locked"
  printf '%-24s MISSING (install cargo-deny)\n' "cargo-deny" >> "$OUT/summary.txt"
fi

# --- available dependency bumps (informational; drives chore(deps) PRs) ---
run "dep-updates"     "updates.log"     cargo update --dry-run --verbose

echo
echo "==> summary"
cat "$OUT/summary.txt"
echo
echo "Reports written to $OUT/. Hand these to the daily-maintenance playbook."
