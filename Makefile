GO ?= go
BINARY ?= bin/portbeam
VERSION ?= dev

.PHONY: all fmt fmt-check vet test race bench build clean

all: fmt-check vet test build

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

bench:
	$(GO) test -bench=. -benchmem ./...

build:
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BINARY) ./cmd/portbeam

clean:
	rm -rf bin dist coverage.out
