SHELL := /bin/sh

GOLANGCI_LINT_VERSION := v2.12.2
STATICCHECK_VERSION := v0.7.0
NILAWAY_VERSION := v0.0.0-20260710181136-2378218750e4
GOVULNCHECK_VERSION := v1.6.0
GREMLINS_VERSION := v0.6.0
ACTIONLINT_VERSION := v1.7.12

.DEFAULT_GOAL := check

.PHONY: api benchmark check coverage docs fmt fmt-check fuzz integration \
	lint mutation nilaway race staticcheck test tidy unit vet vuln workflow

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"

tidy:
	go mod tidy -diff

vet:
	go vet ./...

unit:
	go test ./... -count=1

test: unit

coverage:
	./scripts/check-coverage.sh

race:
	go test -race ./... -count=1

fuzz:
	./scripts/check-fuzz.sh

benchmark:
	./scripts/check-benchmarks.sh

integration:
	./scripts/check-integration.sh

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

nilaway:
	@status=0; \
	go run go.uber.org/nilaway/cmd/nilaway@$(NILAWAY_VERSION) \
		-include-pkgs='github.com/faustbrian/golib/pkg/rate-limit' ./... || status=$$?; \
	if [ "$$status" -ne 0 ]; then \
		printf '%s\n' 'NilAway advisory reported findings; continuing'; \
	fi; \
	exit 0

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

workflow:
	go run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)

mutation:
	GREMLINS_VERSION=$(GREMLINS_VERSION) ./scripts/check-mutation.sh

docs:
	./scripts/check-docs.sh

api:
	./scripts/check-api.sh

check: fmt-check tidy vet lint staticcheck nilaway unit coverage race fuzz \
	docs api vuln workflow integration benchmark mutation
