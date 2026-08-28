.PHONY: benchmark docs integration

benchmark:
	./scripts/check-benchmarks.sh

docs:
	./scripts/check-docs.sh

integration:
	./scripts/check-integration.sh
