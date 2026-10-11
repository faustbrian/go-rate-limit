GOLIB ?= golib

.PHONY: check check-full ci cohesion inventory repository-check

check:
	$(GOLIB) check --local --all

check-full:
	$(GOLIB) check --all

ci:
	$(GOLIB) repository check
	$(GOLIB) cohesion check
	$(GOLIB) check --local --all

cohesion:
	$(GOLIB) cohesion check

inventory repository-check:
	$(GOLIB) repository check
