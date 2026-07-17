#!/bin/sh
set -eu

go test -run '^$$' -fuzz '^FuzzNewKeyNeverLeaksHashedSubject$$' -fuzztime=2s .
go test -run '^$$' -fuzz '^FuzzDecodeStateNeverPanics$$' -fuzztime=2s ./postgres
go test -run '^$$' -fuzz '^FuzzTrustedProxyChainNeverPanics$$' -fuzztime=2s ./ratelimithttp
go test -run '^$$' -fuzz '^FuzzDecodeDecisionNeverPanics$$' -fuzztime=2s ./valkey
