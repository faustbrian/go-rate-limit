#!/bin/sh
set -eu

: "${VALKEY_ADDRESS:?set VALKEY_ADDRESS to a disposable Valkey 9 instance}"
: "${POSTGRES_URL:?set POSTGRES_URL to a disposable PostgreSQL database}"

go test -race -count=1 ./valkey ./postgres
