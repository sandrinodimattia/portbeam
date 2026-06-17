package portbeam

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"
)

func TestParseSpecs(t *testing.T) {
	t.Parallel()

	specs, err := ParseSpecs([]string{
		"0.0.0.0:8443=service.example.com:443",
		"127.0.0.1:18080=10.0.0.5:8080",
	})
	if err != nil {
		t.Fatalf("ParseSpecs returned error: %v", err)
	}

	want := []Spec{
		{Listen: "0.0.0.0:8443", Target: "service.example.com:443"},
		{Listen: "127.0.0.1:18080", Target: "10.0.0.5:8080"},
	}

	if !reflect.DeepEqual(specs, want) {
		t.Fatalf("specs mismatch\nwant: %#v\n got: %#v", want, specs)
	}
}

func TestParseSpecsRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []string
	}{
		{name: "empty", input: nil},
		{name: "missing equals", input: []string{"0.0.0.0:8443"}},
		{name: "empty listen", input: []string{"=service.example.com:443"}},
		{name: "empty target", input: []string{"0.0.0.0:8443="}},
		{name: "too many equals", input: []string{"0.0.0.0:8443=a=b"}},
		{name: "listen missing port", input: []string{"0.0.0.0=service.example.com:443"}},
		{name: "target missing port", input: []string{"0.0.0.0:8443=service.example.com"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseSpecs(test.input); err == nil {
				t.Fatalf("ParseSpecs(%v) returned nil error", test.input)
			}
		})
	}
}

func TestResolveTCPAddrPortParsesLiteralTarget(t *testing.T) {
	t.Parallel()

	addr, err := resolveTCPAddrPort(context.Background(), "127.0.0.1:8443")
	if err != nil {
		t.Fatalf("resolveTCPAddrPort returned error: %v", err)
	}

	want := netip.MustParseAddrPort("127.0.0.1:8443")
	if addr != want {
		t.Fatalf("resolved address mismatch: want %s, got %s", want, addr)
	}
}

func TestRunForwardsTCPConnections(t *testing.T) {
	target := startEchoServer(t)
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	conn := dialTCPWithRetry(t, listen)
	defer conn.Close()

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write to forwarder: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	buf := make([]byte, len("hello"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo through forwarder: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("forwarded response mismatch: %q", string(buf))
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close forwarded connection: %v", err)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunForwardsToHostnameTarget(t *testing.T) {
	target := startEchoServer(t)
	_, port, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("split target host port: %v", err)
	}
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: net.JoinHostPort("localhost", port)}}, Options{})
	}()

	assertEchoRoundTrip(t, listen, []byte("hostname-target"))

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunForwardsMultipleMappings(t *testing.T) {
	targetA := startEchoServer(t)
	targetB := startEchoServer(t)
	listenA := freeTCPAddress(t)
	listenB := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{
			{Listen: listenA, Target: targetA},
			{Listen: listenB, Target: targetB},
		}, Options{})
	}()

	assertEchoRoundTrip(t, listenA, []byte("alpha"))
	assertEchoRoundTrip(t, listenB, []byte("bravo"))

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunHandlesConcurrentConnections(t *testing.T) {
	target := startEchoServer(t)
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	const clientCount = 32
	resultCh := make(chan error, clientCount)
	for clientIndex := range clientCount {
		go func() {
			payload := bytes.Repeat([]byte(fmt.Sprintf("client-%02d:", clientIndex)), 4096)
			resultCh <- echoRoundTrip(listen, payload)
		}()
	}

	for range clientCount {
		if err := <-resultCh; err != nil {
			t.Fatal(err)
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestForwardConnectionPreservesClientHalfClose(t *testing.T) {
	target := startReadAllThenRespondServer(t, "response")
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	conn := dialTCPWithRetry(t, listen)
	defer conn.Close()

	if _, err := conn.Write([]byte("request")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("half-close client write side: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len("response"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read response after half-close: %v", err)
	}
	if string(buf) != "response" {
		t.Fatalf("response mismatch: %q", string(buf))
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestForwardConnectionPreservesTargetHalfClose(t *testing.T) {
	target, received := startRespondHalfCloseThenReadServer(t, "response")
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	conn := dialTCPWithRetry(t, listen)
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len("response"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read target response: %v", err)
	}
	if string(buf) != "response" {
		t.Fatalf("response mismatch: %q", string(buf))
	}

	if _, err := conn.Write([]byte("request")); err != nil {
		t.Fatalf("write after target half-close: %v", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("half-close client write side: %v", err)
	}

	select {
	case request := <-received:
		if request != "request" {
			t.Fatalf("target received %q, want request", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for target to receive request")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunWaitsForActiveConnectionsAfterCancellation(t *testing.T) {
	target, accepted := startHoldingServer(t)
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	client := dialTCPWithRetry(t, listen)
	defer client.Close()
	targetConn := receiveConn(t, accepted)
	defer targetConn.Close()
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("write through active connection: %v", err)
	}
	if err := targetConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set target read deadline: %v", err)
	}
	if _, err := io.ReadFull(targetConn, make([]byte, 1)); err != nil {
		t.Fatalf("read forwarded byte: %v", err)
	}

	cancel()
	waitForTCPAddressReusable(t, listen)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned after listener shutdown but before active connection closed: %v", err)
	default:
	}

	_ = client.Close()
	_ = targetConn.Close()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after active connection closed")
	}
}

func TestRunClosesActiveConnectionsAfterShutdownTimeout(t *testing.T) {
	target, accepted := startHoldingServer(t)
	listen := freeTCPAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{
			ShutdownTimeout: 25 * time.Millisecond,
		})
	}()

	client := dialTCPWithRetry(t, listen)
	defer client.Close()
	targetConn := receiveConn(t, accepted)
	defer targetConn.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after shutdown timeout")
	}
}

func BenchmarkRunForwardsTCPThroughput(b *testing.B) {
	target := startDiscardServer(b)
	listen := freeTCPAddress(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, []Spec{{Listen: listen, Target: target}}, Options{})
	}()

	conn := dialTCPWithRetry(b, listen)
	payload := make([]byte, 1024*1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := conn.Write(payload); err != nil {
			b.Fatalf("write benchmark payload: %v", err)
		}
	}
	b.StopTimer()

	_ = conn.Close()
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			b.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		b.Fatal("Run did not stop after benchmark")
	}
}

func startEchoServer(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo server: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(conn)
		}
	}()

	return listener.Addr().String()
}

func startReadAllThenRespondServer(t testing.TB, response string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target server: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = io.ReadAll(conn)
		_, _ = conn.Write([]byte(response))
	}()

	return listener.Addr().String()
}

func startRespondHalfCloseThenReadServer(t testing.TB, response string) (string, <-chan string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target server: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	received := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = conn.Write([]byte(response))
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
		request, _ := io.ReadAll(conn)
		received <- string(request)
	}()

	return listener.Addr().String(), received
}

func startHoldingServer(t testing.TB) (string, <-chan net.Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen holding server: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	return listener.Addr().String(), accepted
}

func startDiscardServer(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen discard server: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}(conn)
		}
	}()

	return listener.Addr().String()
}

func assertEchoRoundTrip(t testing.TB, address string, payload []byte) {
	t.Helper()

	if err := echoRoundTrip(address, payload); err != nil {
		t.Fatal(err)
	}
}

func echoRoundTrip(address string, payload []byte) error {
	var conn net.Conn
	var err error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if conn == nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	defer conn.Close()

	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return fmt.Errorf("read echo: %w", err)
	}
	if !bytes.Equal(buf, payload) {
		return errors.New("echo payload mismatch")
	}
	return nil
}

func freeTCPAddress(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close free TCP listener: %v", err)
	}
	return address
}

func waitForTCPAddressReusable(t testing.TB, address string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		listener, err := net.Listen("tcp", address)
		if err == nil {
			_ = listener.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to become reusable", address)
}

func receiveConn(t testing.TB, accepted <-chan net.Conn) net.Conn {
	t.Helper()

	select {
	case conn := <-accepted:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for target connection")
		return nil
	}
}

func dialTCPWithRetry(t testing.TB, address string) *net.TCPConn {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				_ = conn.Close()
				t.Fatalf("dial returned %T, want *net.TCPConn", conn)
			}
			return tcpConn
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out dialing %s", address)
	return nil
}
