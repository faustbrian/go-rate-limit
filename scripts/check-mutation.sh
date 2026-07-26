#!/bin/sh
set -eu

version=${GREMLINS_VERSION:-v0.6.0}
gremlins="github.com/go-gremlins/gremlins/cmd/gremlins@$version"
common='--workers 2 --timeout-coefficient 10 --output-statuses l'
exclude='^(memory|postgres|ratelimithttp|ratelimitlog|ratelimitprincipal|ratelimitqueue|ratelimitrpc|ratelimittelemetry|ratelimittest|valkey)/'

run_package() {
	path=$1
	minimum_efficacy=$2
	minimum_coverage=$3
	shift 3
	output=$(mktemp)
	trap 'rm -f "$output"' EXIT HUP INT TERM

	# shellcheck disable=SC2086 # Gremlins flags are intentionally split.
	go run "$gremlins" unleash "$path" $common "$@" >"$output" 2>&1
	cat "$output"
	efficacy=$(awk '/Test efficacy:/ {gsub(/%/, "", $3); print $3}' "$output")
	coverage=$(awk '/Mutator coverage:/ {gsub(/%/, "", $3); print $3}' "$output")
	if [ -z "$efficacy" ] || [ -z "$coverage" ]; then
		printf 'mutation report missing for %s\n' "$path" >&2
		exit 1
	fi
	if ! awk -v actual="$efficacy" -v minimum="$minimum_efficacy" \
		'BEGIN { exit !(actual >= minimum) }'; then
		printf '%s mutation efficacy is %s%%, want at least %s%%\n' \
			"$path" "$efficacy" "$minimum_efficacy" >&2
		exit 1
	fi
	if ! awk -v actual="$coverage" -v minimum="$minimum_coverage" \
		'BEGIN { exit !(actual >= minimum) }'; then
		printf '%s mutant coverage is %s%%, want at least %s%%\n' \
			"$path" "$coverage" "$minimum_coverage" >&2
		exit 1
	fi

	rm -f "$output"
	trap - EXIT HUP INT TERM
}

run_package . 85 100 --exclude-files "$exclude"
run_package memory 70 100
run_package postgres 65 84
run_package ratelimithttp 75 100
run_package ratelimitlog 100 50
run_package ratelimitprincipal 100 100
run_package ratelimitqueue 100 100
run_package ratelimitrpc 90 75
run_package ratelimittelemetry 100 100
run_package valkey 80 83
