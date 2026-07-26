#!/bin/sh
set -eu

raw=$(mktemp)
temporary=$(mktemp)
trap 'rm -f "$raw" "$temporary"' EXIT HUP INT TERM

go list ./... | while IFS= read -r package; do
	case "$package" in
		*/ratelimittest) continue ;;
	esac
	printf 'PACKAGE %s\n\n' "$package"
	go doc -all "$package"
done >"$raw"

awk '
	NF { for (n = 0; n < pending; n++) print ""; print; pending = 0; next }
	{ pending++ }
' "$raw" >"$temporary"

if [ "${UPDATE_API:-0}" = "1" ]; then
	cp "$temporary" api/baseline.txt
	exit 0
fi

if ! diff -u api/baseline.txt "$temporary"; then
	printf '%s\n' 'public API changed; review and regenerate api/baseline.txt' >&2
	exit 1
fi
