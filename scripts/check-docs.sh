#!/bin/sh
set -eu

required='README.md
CHANGELOG.md
SECURITY.md
docs/README.md
docs/api.md
docs/algorithms.md
docs/audit-report.md
docs/backends.md
docs/deployment.md
docs/faq.md
docs/hardening.md
docs/http.md
docs/json-rpc.md
docs/laravel-migration.md
docs/operations.md
docs/outages.md
docs/performance.md
docs/principals-and-tenants.md
docs/queues.md
docs/security.md
docs/troubleshooting.md
docs/trusted-proxies.md
docs/weighted-costs.md'

printf '%s\n' "$required" | while IFS= read -r file; do
	test -s "$file" || { printf 'missing documentation: %s\n' "$file" >&2; exit 1; }
done

go list ./... | while IFS= read -r package; do
	go doc "$package" >/dev/null
done
