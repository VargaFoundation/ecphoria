#!/usr/bin/env bash
# Versions that must move together, do.
#
# Some versions in this repository are one thing wearing several hats — the server binary, the image
# the chart deploys, the operator that manages it — and some are genuinely independent, because they
# are separate packages with their own compatibility story (the SDKs, the chart's own revision).
#
# The failure this catches is quiet: a release where the chart still points at the previous
# appVersion deploys the old image with the new chart, and everything looks fine until someone asks
# why a new endpoint is missing.
#
# Run: scripts/check-versions.sh
set -euo pipefail

cd "$(dirname "$0")/.."

fail=0
note() { printf '  %-42s %s\n' "$1" "$2"; }
bad() { printf '\033[31m  %-42s %s\033[0m\n' "$1" "$2"; fail=1; }

# The workspace version is the source of truth: it is what the binary reports and what the image is
# tagged with.
workspace=$(grep -m1 '^version = ' Cargo.toml | cut -d'"' -f2)
echo "Workspace (ecphoria-server, all crates): $workspace"
echo
echo "Must match the workspace:"

chart_app=$(grep -m1 '^appVersion:' deploy/helm/ecphoria/Chart.yaml | tr -d '"' | awk '{print $2}')
if [ "$chart_app" = "$workspace" ]; then
  note "deploy/helm/ecphoria appVersion" "$chart_app"
else
  bad "deploy/helm/ecphoria appVersion" "$chart_app  ← expected $workspace"
fi

operator=$(grep -m1 '^version = ' ops/operator/Cargo.toml | cut -d'"' -f2)
if [ "$operator" = "$workspace" ]; then
  note "ops/operator" "$operator"
else
  bad "ops/operator" "$operator  ← expected $workspace"
fi

# Independent on purpose — printed so a release can see them, never enforced.
echo
echo "Independent (their own semver, shown for information):"
note "deploy/helm/ecphoria chart version" "$(grep -m1 '^version:' deploy/helm/ecphoria/Chart.yaml | awk '{print $2}')"
note "sdk/python" "$(grep -m1 '^version = ' sdk/python/pyproject.toml | cut -d'"' -f2)"
note "sdk/typescript" "$(grep -m1 '"version"' sdk/typescript/package.json | cut -d'"' -f4)"
note "bindings/python" "$(grep -m1 '^version = ' bindings/python/pyproject.toml | cut -d'"' -f2)"

echo
if [ "$fail" -ne 0 ]; then
  echo "Versions are out of step — see above. \`cargo release\` keeps them aligned (release.toml)."
  exit 1
fi
echo "Versions are consistent."
