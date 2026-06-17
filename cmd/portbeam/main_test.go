package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sandrino/portbeam"
)

func TestForwardFlag(t *testing.T) {
	var flags forwardFlag
	if err := flags.Set("127.0.0.1:10000=127.0.0.1:10001"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := flags.Set("127.0.0.1:10002=127.0.0.1:10003"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	got := flags.String()
	want := "127.0.0.1:10000=127.0.0.1:10001,127.0.0.1:10002=127.0.0.1:10003"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestMainExitsWithRunCode(t *testing.T) {
	originalArgs := os.Args
	originalExitProcess := exitProcess
	defer func() {
		os.Args = originalArgs
		exitProcess = originalExitProcess
	}()

	os.Args = []string{"portbeam", "-version"}
	var exitCode int
	exitProcess = func(code int) {
		exitCode = code
	}

	main()

	if exitCode != 0 {
		t.Fatalf("main exit code = %d, want 0", exitCode)
	}
}

func TestRunHandlesFlagParseError(t *testing.T) {
	var output bytes.Buffer
	code := run([]string{"-unknown"}, &output)

	if code != 2 {
		t.Fatalf("run exit code = %d, want 2", code)
	}
	if !strings.Contains(output.String(), "flag provided but not defined") {
		t.Fatalf("run output %q does not include parse error", output.String())
	}
}

func TestRunPrintsVersion(t *testing.T) {
	var output bytes.Buffer
	code := run([]string{"-version"}, &output)

	if code != 0 {
		t.Fatalf("run exit code = %d, want 0", code)
	}
	if output.String() != "portbeam dev\n" {
		t.Fatalf("run output = %q, want version line", output.String())
	}
}

func TestRunHandlesConfigurationError(t *testing.T) {
	var output bytes.Buffer
	code := run(nil, &output)

	if code != 2 {
		t.Fatalf("run exit code = %d, want 2", code)
	}
	if !strings.Contains(output.String(), "configuration error") {
		t.Fatalf("run output %q does not include configuration error", output.String())
	}
}

func TestRunStartsForwarder(t *testing.T) {
	withRunForwarder(t, func(ctx context.Context, specs []portbeam.Spec, options portbeam.Options) error {
		if ctx == nil {
			t.Fatal("runForwarder received nil context")
		}

		wantSpecs := []portbeam.Spec{{
			Listen: "127.0.0.1:10000",
			Target: "127.0.0.1:10001",
		}}
		if len(specs) != len(wantSpecs) || specs[0] != wantSpecs[0] {
			t.Fatalf("runForwarder specs = %#v, want %#v", specs, wantSpecs)
		}
		if options.ShutdownTimeout != 5*time.Second {
			t.Fatalf("ShutdownTimeout = %s, want 5s", options.ShutdownTimeout)
		}
		if options.DialTimeout != 6*time.Second {
			t.Fatalf("DialTimeout = %s, want 6s", options.DialTimeout)
		}
		if options.KeepAlive != -1*time.Second {
			t.Fatalf("KeepAlive = %s, want -1s", options.KeepAlive)
		}
		if options.Logger == nil {
			t.Fatal("Logger is nil")
		}
		return nil
	})

	var output bytes.Buffer
	code := run([]string{
		"-shutdown-timeout", "5s",
		"-dial-timeout", "6s",
		"-keepalive", "-1s",
		"-forward", "127.0.0.1:10000=127.0.0.1:10001",
	}, &output)

	if code != 0 {
		t.Fatalf("run exit code = %d, want 0", code)
	}
	if output.Len() != 0 {
		t.Fatalf("run output = %q, want empty output", output.String())
	}
}

func TestRunReportsForwarderError(t *testing.T) {
	withRunForwarder(t, func(ctx context.Context, specs []portbeam.Spec, options portbeam.Options) error {
		return errors.New("forwarder failed")
	})

	var output bytes.Buffer
	code := run([]string{"-forward", "127.0.0.1:10000=127.0.0.1:10001"}, &output)

	if code != 1 {
		t.Fatalf("run exit code = %d, want 1", code)
	}
	if !strings.Contains(output.String(), "portbeam stopped: forwarder failed") {
		t.Fatalf("run output %q does not include forwarder error", output.String())
	}
}

func withRunForwarder(
	t testing.TB,
	replacement func(context.Context, []portbeam.Spec, portbeam.Options) error,
) {
	t.Helper()

	original := runForwarder
	runForwarder = replacement
	t.Cleanup(func() {
		runForwarder = original
	})
}
