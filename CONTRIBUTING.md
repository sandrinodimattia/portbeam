# Contributing

Thanks for helping improve Portbeam.

## Local Setup

Install Go 1.26.4 or newer in the same release line, then run:

```bash
go version
make all
```

## Development Checks

Use the same checks as CI:

```bash
make fmt-check
make vet
make test
make race
make build
```

For performance-sensitive changes, also run:

```bash
go test -bench=BenchmarkRunForwardsTCPThroughput -benchmem -benchtime=5s -run '^$' .
```

## Pull Requests

- Keep changes focused and explain behavioral impact.
- Add or update tests for forwarding, shutdown, parsing, and TCP edge cases.
- Avoid adding dependencies unless they clearly reduce complexity or risk.
- Include benchmark results when changing the hot path.

## Commit Messages

Use a short prefix so history is easy to scan:

```text
feat: add new forwarding option
fix: preserve target half-close handling
docs: clarify launchd setup
test: cover concurrent clients
```
