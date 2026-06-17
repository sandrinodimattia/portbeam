# Portbeam

Portbeam is a small, fast TCP port forwarder for exposing selected local or
private services to another network interface.

It was built for cases where a raw TCP forwarder is enough: forwarding SSH
endpoints, database listeners, local development services, and other TCP
services without inspecting or modifying traffic.

## Features

- Repeats `--forward listen=target` for as many TCP mappings as needed.
- Preserves TCP half-closes so protocols can finish cleanly.
- Resolves target hostnames once at startup to keep per-connection setup lean.
- Uses Go's `io.Copy` TCP fast paths where the runtime and OS support them.
- Provides both a CLI and an importable Go package.
- Has no third-party runtime dependencies.

## Install

```bash
go install github.com/sandrino/portbeam/cmd/portbeam@latest
```

Or build from a checkout:

```bash
make build
bin/portbeam -version
```

Portbeam currently targets Go 1.26.4.

## Release Binaries

Tagged releases are built by GitHub Actions with GoReleaser. Pushing a tag such
as `v0.1.0` creates downloadable archives for macOS, Linux, and Windows on
`amd64` and `arm64`.

Local binaries are build artifacts, not source files. `make build` writes to
`bin/portbeam`, and `bin/` is ignored by git.

## CLI Usage

Forward one port:

```bash
portbeam \
  --forward 127.0.0.1:18443=service.example.com:443
```

Forward several ports:

```bash
portbeam \
  --forward 127.0.0.1:15432=db.example.com:5432 \
  --forward 127.0.0.1:16379=cache.example.com:6379 \
  --forward 0.0.0.0:12222=bastion.example.com:22
```

Options:

```text
-forward value
  TCP forward in listen=target form; may be repeated
-shutdown-timeout duration
  maximum time to drain active connections after shutdown before closing them
-dial-timeout duration
  maximum time to establish each target connection
-keepalive duration
  TCP keepalive period; set to a negative duration to disable
-version
  print version and exit
```

## Use As A Library

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/sandrino/portbeam"
)

func main() {
	specs, err := portbeam.ParseSpecs([]string{
		"127.0.0.1:18080=10.0.0.5:8080",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = portbeam.Run(ctx, specs, portbeam.Options{
		ShutdownTimeout: 30 * time.Second,
		Logger:          log.New(os.Stderr, "", log.LstdFlags),
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Run At Startup On macOS

Install the binary:

```bash
make build
sudo install -d -m 0755 /usr/local/bin
sudo install -m 0755 bin/portbeam /usr/local/bin/portbeam
```

Create a LaunchDaemon. This example binds a network interface address on the
Mac and forwards traffic to a private service address:

```bash
PORTBEAM_USER="$(id -un)"
PORTBEAM_LOG_DIR="$HOME/Library/Logs/portbeam"
install -d -m 0755 "$PORTBEAM_LOG_DIR"

sudo tee /Library/LaunchDaemons/com.portbeam.forward.plist >/dev/null <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.portbeam.forward</string>
  <key>UserName</key>
  <string>${PORTBEAM_USER}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/portbeam</string>
    <string>--shutdown-timeout</string>
    <string>30s</string>
    <string>--forward</string>
    <string>10.0.0.25:8443=10.0.1.20:443</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${PORTBEAM_LOG_DIR}/portbeam.log</string>
  <key>StandardErrorPath</key>
  <string>${PORTBEAM_LOG_DIR}/portbeam.err.log</string>
</dict>
</plist>
PLIST

sudo chown root:wheel /Library/LaunchDaemons/com.portbeam.forward.plist
sudo chmod 644 /Library/LaunchDaemons/com.portbeam.forward.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.portbeam.forward.plist
sudo launchctl enable system/com.portbeam.forward
sudo launchctl kickstart -k system/com.portbeam.forward
```

Check it:

```bash
sudo launchctl print system/com.portbeam.forward
lsof -nP -iTCP:8443 -sTCP:LISTEN
tail -f "$HOME/Library/Logs/portbeam/portbeam.err.log"
```

Restart the daemon after changing mappings or when a target hostname resolves to
a different address.

## Performance Notes

Portbeam stays intentionally simple: one accepted TCP connection maps to one
outbound TCP connection and two `io.Copy` loops. It does not terminate TLS,
parse HTTP, buffer complete requests, or route by content.

Targets are resolved once at startup. This avoids DNS work for every incoming
connection and also makes behavior deterministic while the process is running.
If a dynamic hostname changes address, restart Portbeam.

## Security

Portbeam exposes whatever target service you map. Bind to a specific trusted
interface address when possible, and do not expose unauthenticated private
services to untrusted networks.

## Development

```bash
make fmt
make vet
make test
make race
make bench
make build
```

Before sending a change, run:

```bash
make all
```
