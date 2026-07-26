#!/bin/sh
set -eu

fuzz_time="${FUZZ_TIME:-10000x}"

go test -run '^$$' -fuzz '^FuzzNewKeyNeverLeaksHashedSubject$$' -fuzztime="${fuzz_time}" .
go test -run '^$$' -fuzz '^FuzzDecodeStateNeverPanics$$' -fuzztime="${fuzz_time}" ./postgres
go test -run '^$$' -fuzz '^FuzzTrustedProxyChainNeverPanics$$' -fuzztime="${fuzz_time}" ./ratelimithttp
go test -run '^$$' -fuzz '^FuzzDecodeDecisionNeverPanics$$' -fuzztime="${fuzz_time}" ./valkey
