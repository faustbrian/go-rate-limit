#!/bin/sh
set -eu

output=$(mktemp)
trap 'rm -f "$output"' EXIT HUP INT TERM

go test -run '^$' \
	-bench '^(BenchmarkBatchSizes|BenchmarkHotKey|BenchmarkHighCardinality)$' \
	-benchmem -benchtime=10000x -count=3 ./... | tee "$output"

awk '
function check(name, latency, bytes, allocations, max_latency, max_bytes, max_allocations) {
	seen[name]++
	if (latency > max_latency || bytes > max_bytes || allocations > max_allocations) {
		printf "%s exceeded budget: %s ns/op, %s B/op, %s allocs/op\n", \
			name, latency, bytes, allocations > "/dev/stderr"
		failed = 1
	}
}
/^BenchmarkBatchSizes\/size_1-/ {
	check("batch-1", $(NF-5), $(NF-3), $(NF-1), 5000, 512, 8)
}
/^BenchmarkBatchSizes\/size_16-/ {
	check("batch-16", $(NF-5), $(NF-3), $(NF-1), 50000, 8192, 64)
}
/^BenchmarkBatchSizes\/size_64-/ {
	check("batch-64", $(NF-5), $(NF-3), $(NF-1), 200000, 32768, 256)
}
/^BenchmarkBatchSizes\/size_256-/ {
	check("batch-256", $(NF-5), $(NF-3), $(NF-1), 800000, 131072, 1024)
}
/^BenchmarkHotKey-/ {
	check("hot-key", $(NF-5), $(NF-3), $(NF-1), 5000, 256, 2)
}
/^BenchmarkHighCardinality-/ {
	check("high-cardinality", $(NF-5), $(NF-3), $(NF-1), 200000, 8192, 16)
}
END {
	split("batch-1 batch-16 batch-64 batch-256 hot-key high-cardinality", required)
	for (item in required) {
		name = required[item]
		if (seen[name] < 3) {
			printf "%s benchmark evidence missing\n", name > "/dev/stderr"
			failed = 1
		}
	}
	exit failed
}
' "$output"
