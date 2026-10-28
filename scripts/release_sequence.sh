#!/usr/bin/env bash
# Run release workflow targets one after another and check each registry afterwards.
#
#   scripts/release_sequence.sh nuget=0.3.4 npm=0.2.0 pypi=0.1.2 maven=0.1.2 docker-ui=0.1.9
#
# Each argument is target=version. A target is one of: rust-client, nuget, npm, pypi, maven,
# docker-ui, ui-host. The workflow runs from main (a dev run would only publish previews), one
# at a time, because every run pushes a version commit to main. Stops at the first failure.
# Needs the GitHub CLI signed in with write access: gh auth status.
set -euo pipefail

repo=salihcantekin/ruvio
workflow=release.yml

[ "$#" -gt 0 ] || { echo "usage: $0 target=version [target=version ...]" >&2; exit 2; }

input_for() {
  case "$1" in
    rust-client) echo rust_client_version ;;
    nuget) echo csharp_version ;;
    npm) echo node_version ;;
    pypi) echo python_version ;;
    maven) echo java_version ;;
    docker-ui | ui-host) echo ui_version ;;
    *) echo "unknown target: $1" >&2; return 1 ;;
  esac
}

latest_run() { gh run list --repo "$repo" --workflow "$workflow" --limit 1 --json databaseId -q '.[0].databaseId'; }

wait_idle() {
  while [ -n "$(gh run list --repo "$repo" --workflow "$workflow" --status in_progress --json databaseId -q '.[].databaseId')" ] \
     || [ -n "$(gh run list --repo "$repo" --workflow "$workflow" --status queued --json databaseId -q '.[].databaseId')" ]; do
    echo "another release run is active, waiting"
    sleep 20
  done
}

verify() {
  local target=$1 version=$2 found=""
  for _ in 1 2 3 4 5 6; do
    case "$target" in
      rust-client)
        found=$(curl -fsS -H "User-Agent: ruvio-release-check" "https://crates.io/api/v1/crates/ruvio-client" | grep -o "\"num\":\"$version\"" | head -1 || true) ;;
      nuget)
        found=$(curl -fsS "https://api.nuget.org/v3-flatcontainer/ruvio.client/index.json" | grep -o "\"$version\"" | head -1 || true) ;;
      npm)
        found=$(curl -fsS "https://registry.npmjs.org/ruvio-client/$version" | grep -o "\"version\":\"$version\"" | head -1 || true) ;;
      pypi)
        found=$(curl -fsS "https://pypi.org/pypi/ruvio-client/$version/json" | grep -o "\"version\": *\"$version\"" | head -1 || true) ;;
      maven)
        found=$(curl -fsS "https://repo1.maven.org/maven2/io/github/salihcantekin/ruvio-client/$version/" 2>/dev/null | head -1 || true) ;;
      docker-ui)
        found=$(curl -fsS "https://hub.docker.com/v2/repositories/salihcantekin/ruvio-ui/tags/$version" | grep -o "\"name\": *\"$version\"" | head -1 || true) ;;
      *) echo "  $target: no registry check"; return 0 ;;
    esac
    [ -n "$found" ] && { echo "  verified: $target $version is listed"; return 0; }
    sleep 20
  done
  echo "  NOT VERIFIED: $target $version is not listed yet (Maven Central can take longer, and Maven waits for Publish in the Portal unless auto publish is on)"
}

for pair in "$@"; do
  target=${pair%%=*}
  version=${pair#*=}
  input=$(input_for "$target")
  echo "== $target $version"
  wait_idle
  before=$(latest_run)
  gh workflow run "$workflow" --repo "$repo" --ref main -f "target=$target" -f "$input=$version"
  run=""
  for _ in $(seq 1 30); do
    sleep 4
    now=$(latest_run)
    [ "$now" != "$before" ] && { run=$now; break; }
  done
  [ -n "$run" ] || { echo "the run did not start" >&2; exit 1; }
  gh run watch "$run" --repo "$repo" --exit-status >/dev/null || { echo "run $run failed: gh run view $run --repo $repo --log-failed" >&2; exit 1; }
  echo "  workflow succeeded (run $run)"
  verify "$target" "$version"
done
echo "done"
