#!/usr/bin/env bash
# Local gate before a push: the tests that tie hand-kept lists to their one source
# (command table, format versions, settings template, value kinds, hot parser, INFO fields), the
# compatibility matrix check, and a build of every target. About two minutes on a warm
# debug build; no release build, no full test suite.
#
#   scripts/check.sh                 run the checks
#   scripts/check.sh --install-hook  run them before every `git push` from this clone
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ "${1:-}" == "--install-hook" ]]; then
	hook="$(git rev-parse --git-path hooks)/pre-push"
	printf '#!/usr/bin/env bash\nexec "%s/scripts/check.sh"\n' "$root" >"$hook"
	chmod +x "$hook"
	echo "pre-push hook installed: $hook"
	exit 0
fi

limit="${RUVIO_TEST_TIMEOUT:-300}"
# Stock macOS has no `timeout`; coreutils installs it as `gtimeout`. Without either the
# checks still run, only without the time limit.
timeout_cmd=()
if command -v timeout >/dev/null; then
	timeout_cmd=(timeout "$limit")
elif command -v gtimeout >/dev/null; then
	timeout_cmd=(gtimeout "$limit")
fi
run() {
	echo "+ $*" >&2
	${timeout_cmd[@]+"${timeout_cmd[@]}"} "$@"
}

run python3 scripts/check_compatibility_matrix.py
run python3 scripts/gen_client_keys.py --check
# C# layout rules (blank lines, bodies on their own line); skipped where .NET is not installed.
if command -v dotnet >/dev/null; then
	run dotnet run -c Release --project scripts/csharp-format -- --check
fi
# The model checker's mutation test applies these; one that no longer applies tests nothing.
for patch in scripts/model_check_mutants/*.patch; do
	git apply --check "$patch" || { echo "$patch no longer applies; redo it on the current code" >&2; exit 1; }
done
# Workflows install the toolchain rust-toolchain.toml pins, so CI downloads one toolchain.
pinned="$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)"
# Actions are pinned to a commit SHA; the trailing comment names the toolchain tag.
if grep -n 'dtolnay/rust-toolchain@' .github/workflows/*.yml | grep -v '# nightly$' | grep -v "# $pinned\$"; then
	echo "workflows above install a toolchain other than $pinned (rust-toolchain.toml)" >&2
	exit 1
fi
run cargo check -q --all-targets
run cargo clippy -q --all-targets -- -D warnings

# Each filter names the test that holds one standard; a renamed test must be renamed here.
standards=(
	hand_kept_command_lists_agree_with_the_table
	table_arity_and_key_positions_match_what_the_parser_accepts
	every_keyless_command_is_decided_for_scripts
	every_keyless_command_is_decided_for_fan_out
	hot_parser_agrees_with_the_general_parser
	every_record_type_the_decoder_knows_has_a_format_minor
	every_value_kind_the_decoder_knows_is_counted
	every_template_setting_answers_config_get
	every_file_setting_is_documented
	every_environment_variable_is_documented
	scan_type_finds_every_kind_by_the_name_type_reports
	info_fields_and_the_observability_table_agree
	every_value_kind_survives_log_replay_checkpoint_and_dump
	version_one_wal
	client_command_spec_matches_the_server
)
# A filter that matches nothing passes silently, so first make sure every filter still
# matches at least one test name (a filter may name a prefix shared by several tests).
listed="$(run cargo test -q --lib -- --list)"
for name in "${standards[@]}"; do
	if ! grep -q "::[^:]*${name}[^:]*: test$" <<<"$listed"; then
		echo "check.sh: standards test '${name}' not found; it was renamed or removed" >&2
		exit 1
	fi
done
run cargo test -q --lib -- "${standards[@]}"
run cargo test -q --bin ruvio -- every_command_line_flag_is_documented
run cargo test -q --test zero_alloc --test layout
echo "check.sh: ok"
