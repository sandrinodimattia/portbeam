GO ?= go
BINARY ?= bin/portbeam
VERSION ?= dev

.PHONY: all fmt fmt-check vet test race coverage bench build clean

all: fmt-check vet coverage build

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)"

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

coverage:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	@total="$$( $(GO) tool cover -func=coverage.out | awk '/^total:/ { print $$3 }' )"; \
	if [ "$$total" != "100.0%" ]; then \
		echo "coverage $$total, want 100.0%"; \
		exit 1; \
	fi

bench:
	$(GO) test -bench=. -benchmem ./...

build:
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BINARY) ./cmd/portbeam

clean:
	rm -rf bin dist coverage.out
