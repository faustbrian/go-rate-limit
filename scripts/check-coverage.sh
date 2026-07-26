#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

cd "$root"
go list ./... | while IFS= read -r package; do
	case "$package" in
		*/ratelimittest) continue ;;
	esac
	name=$(printf '%s' "$package" | tr '/.' '__')
	profile="$temporary/$name.out"
	go test -count=1 -covermode=atomic -coverprofile="$profile" "$package"
	total=$(go tool cover -func="$profile" | awk '/^total:/ {print $3}')
	if [ "$total" != "100.0%" ]; then
		printf '%s coverage is %s, want 100.0%%\n' "$package" "$total" >&2
		exit 1
	fi
done
