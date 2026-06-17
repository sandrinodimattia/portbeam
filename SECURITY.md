# Security Policy

## Supported Versions

Security fixes target the latest released version of Portbeam.

## Reporting A Vulnerability

Please report suspected vulnerabilities privately before opening a public issue.
If GitHub private vulnerability reporting is enabled for the repository, use
that flow. Otherwise, contact the maintainers through the security contact
listed on the repository profile.

Include:

- Affected version or commit.
- Platform and Go version.
- Minimal reproduction steps.
- Expected and observed behavior.
- Any known exposure or mitigation.

## Scope

Portbeam forwards raw TCP streams. Reports are especially useful for issues
around unintended listener exposure, shutdown behavior, resource exhaustion, or
incorrect connection handling.
