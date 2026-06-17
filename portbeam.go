// Package portbeam provides a small, fast TCP forwarding engine.
//
// It is intentionally protocol-agnostic: bytes accepted on each listen address
// are copied to the configured target address, and bytes from the target are
// copied back to the client. This makes it suitable for forwarding SSH
// bastions, database listeners, local development services, and similar TCP
// services.
package portbeam

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultShutdownTimeout is the amount of time Run waits for active
	// connections to finish after the context is canceled.
	DefaultShutdownTimeout = 30 * time.Second

	// DefaultDialTimeout bounds connection attempts to a forwarding target.
	DefaultDialTimeout = 10 * time.Second

	// DefaultKeepAlive keeps idle TCP sessions from lingering forever when a
	// peer disappears without closing the connection cleanly.
	DefaultKeepAlive = 30 * time.Second
)

// Spec describes one TCP forwarding rule.
type Spec struct {
	// Listen is the local TCP address Portbeam listens on, such as
	// "127.0.0.1:8080" or "0.0.0.0:8443".
	Listen string

	// Target is the TCP address each accepted client connection is forwarded to.
	// Hostnames are resolved once when Run starts, keeping connection setup on
	// the hot path free from DNS work.
	Target string
}

// Options configures the forwarding engine. The zero value is ready to use.
type Options struct {
	// ShutdownTimeout controls how long Run waits for active connections after
	// cancellation before force-closing sockets. A zero value uses
	// DefaultShutdownTimeout; a negative value closes active sockets immediately.
	ShutdownTimeout time.Duration

	// DialTimeout bounds outbound connection setup. The default is
	// DefaultDialTimeout.
	DialTimeout time.Duration

	// KeepAlive configures TCP keepalive on accepted and outbound connections.
	// The default is DefaultKeepAlive. A negative value disables keepalive.
	KeepAlive time.Duration

	// Logger receives lifecycle and connection errors. Nil discards logs, which
	// is usually what library callers want.
	Logger *log.Logger
}

type resolvedSpec struct {
	listen      string
	target      string
	targetAddrs []netip.AddrPort
}

type normalizedOptions struct {
	shutdownTimeout time.Duration
	dialTimeout     time.Duration
	keepAlive       time.Duration
	logger          *log.Logger
}

// ParseSpec parses a "listen=target" forwarding rule.
func ParseSpec(value string) (Spec, error) {
	if strings.Count(value, "=") != 1 {
		return Spec{}, fmt.Errorf("invalid forward value %q: expected listen=target", value)
	}

	parts := strings.SplitN(value, "=", 2)
	listen := strings.TrimSpace(parts[0])
	target := strings.TrimSpace(parts[1])
	if listen == "" || target == "" {
		return Spec{}, fmt.Errorf("invalid forward value %q: listen and target must be non-empty", value)
	}
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return Spec{}, fmt.Errorf("invalid listen address %q: %w", listen, err)
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		return Spec{}, fmt.Errorf("invalid target address %q: %w", target, err)
	}

	return Spec{Listen: listen, Target: target}, nil
}

// ParseSpecs parses multiple "listen=target" forwarding rules.
func ParseSpecs(values []string) ([]Spec, error) {
	if len(values) == 0 {
		return nil, errors.New("at least one forward value is required")
	}

	specs := make([]Spec, 0, len(values))
	for _, value := range values {
		spec, err := ParseSpec(value)
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// Run starts all configured forwarders and blocks until ctx is canceled, a
// listener fails, or a startup error occurs.
//
// All listeners are created before Run accepts connections. Targets are resolved
// once during startup, so restart Portbeam if a target hostname moves to a new
// address.
func Run(ctx context.Context, specs []Spec, options Options) error {
	if len(specs) == 0 {
		return errors.New("at least one forward spec is required")
	}
	if ctx.Err() != nil {
		return nil
	}

	opts := normalizeOptions(options)
	resolvedSpecs, err := resolveSpecs(ctx, specs)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	listeners := make([]net.Listener, 0, len(resolvedSpecs))
	for _, spec := range resolvedSpecs {
		listener, err := net.Listen("tcp", spec.listen)
		if err != nil {
			closeListeners(listeners)
			return fmt.Errorf("listen on %s: %w", spec.listen, err)
		}

		listeners = append(listeners, listener)
		opts.logf("forwarding %s -> %s (%v)", spec.listen, spec.target, spec.targetAddrs)
	}

	go func() {
		<-ctx.Done()
		closeListeners(listeners)
	}()

	// Active connections are allowed to drain after listeners stop. The tracker
	// gives shutdown a bounded escape hatch for idle clients that never close.
	var activeConnections sync.WaitGroup
	tracker := newConnectionTracker()
	errCh := make(chan error, len(resolvedSpecs))
	for index, spec := range resolvedSpecs {
		listener := listeners[index]
		go func() {
			errCh <- serveListener(ctx, listener, spec, opts, &activeConnections, tracker)
		}()
	}

	var firstErr error
	for range resolvedSpecs {
		err := <-errCh
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		if firstErr == nil {
			firstErr = err
			cancel()
			closeListeners(listeners)
		}
	}

	waitForActiveConnections(&activeConnections, tracker, opts)
	return firstErr
}

func normalizeOptions(options Options) normalizedOptions {
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	dialTimeout := options.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = DefaultDialTimeout
	}

	keepAlive := options.KeepAlive
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlive
	}

	return normalizedOptions{
		shutdownTimeout: shutdownTimeout,
		dialTimeout:     dialTimeout,
		keepAlive:       keepAlive,
		logger:          options.Logger,
	}
}

func (options normalizedOptions) logf(format string, args ...any) {
	if options.logger != nil {
		options.logger.Printf(format, args...)
	}
}

func resolveSpecs(ctx context.Context, specs []Spec) ([]resolvedSpec, error) {
	resolvedSpecs := make([]resolvedSpec, 0, len(specs))
	for _, spec := range specs {
		targetAddrs, err := resolveTCPAddrPorts(ctx, spec.Target)
		if err != nil {
			return nil, fmt.Errorf("resolve target %s: %w", spec.Target, err)
		}
		resolvedSpecs = append(resolvedSpecs, resolvedSpec{
			listen:      spec.Listen,
			target:      spec.Target,
			targetAddrs: targetAddrs,
		})
	}
	return resolvedSpecs, nil
}

func resolveTCPAddrPort(ctx context.Context, address string) (netip.AddrPort, error) {
	addrs, err := resolveTCPAddrPorts(ctx, address)
	if err != nil {
		return netip.AddrPort{}, err
	}
	return addrs[0], nil
}

func resolveTCPAddrPorts(ctx context.Context, address string) ([]netip.AddrPort, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, errors.New("target host must be non-empty")
	}

	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("target port %q must be numeric: %w", portText, err)
	}
	targetPort := uint16(port)

	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.AddrPort{netip.AddrPortFrom(addr.Unmap(), targetPort)}, nil
	}

	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("no IP addresses returned")
	}

	targetAddrs := make([]netip.AddrPort, 0, len(addrs))
	seen := make(map[netip.Addr]struct{}, len(addrs))
	appendAddrs := func(wantIPv4 bool) {
		for _, addr := range addrs {
			addr = addr.Unmap()
			if addr.Is4() != wantIPv4 {
				continue
			}
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			targetAddrs = append(targetAddrs, netip.AddrPortFrom(addr, targetPort))
		}
	}
	appendAddrs(true)
	appendAddrs(false)
	return targetAddrs, nil
}

func closeListeners(listeners []net.Listener) {
	for _, listener := range listeners {
		_ = listener.Close()
	}
}

func serveListener(
	ctx context.Context,
	listener net.Listener,
	spec resolvedSpec,
	options normalizedOptions,
	activeConnections *sync.WaitGroup,
	tracker *connectionTracker,
) error {
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()

	for {
		client, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return context.Canceled
			}
			return fmt.Errorf("accept on %s: %w", listener.Addr(), err)
		}

		activeConnections.Add(1)
		go func() {
			defer activeConnections.Done()
			forwardConnection(ctx, client, spec, options, tracker)
		}()
	}
}

// connectionTracker owns the live sockets that are otherwise blocked inside
// io.Copy, giving shutdown a way to interrupt long-idle client sessions.
type connectionTracker struct {
	mutex       sync.Mutex
	connections map[net.Conn]struct{}
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{connections: make(map[net.Conn]struct{})}
}

func (tracker *connectionTracker) add(conn net.Conn) func() {
	tracker.mutex.Lock()
	tracker.connections[conn] = struct{}{}
	tracker.mutex.Unlock()

	return func() {
		tracker.mutex.Lock()
		delete(tracker.connections, conn)
		tracker.mutex.Unlock()
	}
}

func (tracker *connectionTracker) closeAll() {
	// Copy the connection list before closing sockets so connection cleanup can
	// take the same lock without deadlocking this forced shutdown path.
	tracker.mutex.Lock()
	connections := make([]net.Conn, 0, len(tracker.connections))
	for conn := range tracker.connections {
		connections = append(connections, conn)
	}
	tracker.mutex.Unlock()

	for _, conn := range connections {
		_ = conn.Close()
	}
}

func waitForActiveConnections(
	activeConnections *sync.WaitGroup,
	tracker *connectionTracker,
	options normalizedOptions,
) {
	// Graceful shutdown waits for in-flight TCP sessions, then force-closes
	// any remaining sockets so service managers and reboots cannot hang forever.
	done := make(chan struct{})
	go func() {
		activeConnections.Wait()
		close(done)
	}()

	if options.shutdownTimeout <= 0 {
		tracker.closeAll()
		<-done
		return
	}

	timer := time.NewTimer(options.shutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		options.logf("shutdown timeout reached after %s; closing active connections", options.shutdownTimeout)
		tracker.closeAll()
		<-done
	}
}

func forwardConnection(
	ctx context.Context,
	client net.Conn,
	spec resolvedSpec,
	options normalizedOptions,
	tracker *connectionTracker,
) {
	removeClient := tracker.add(client)
	defer removeClient()
	defer client.Close()
	tuneTCPConnection(client, options.keepAlive)

	upstream, _, err := dialTarget(ctx, spec, options)
	if err != nil {
		options.logf("connect %s -> %s (%v) failed: %v", client.RemoteAddr(), spec.target, spec.targetAddrs, err)
		return
	}
	removeUpstream := tracker.add(upstream)
	defer removeUpstream()
	defer upstream.Close()
	tuneTCPConnection(upstream, options.keepAlive)

	var wg sync.WaitGroup
	wg.Add(2)
	go copyAndCloseWrite(&wg, upstream, client, options)
	go copyAndCloseWrite(&wg, client, upstream, options)
	wg.Wait()
}

func dialTarget(ctx context.Context, spec resolvedSpec, options normalizedOptions) (*net.TCPConn, netip.AddrPort, error) {
	if len(spec.targetAddrs) == 0 {
		return nil, netip.AddrPort{}, errors.New("no target addresses")
	}

	dialer := net.Dialer{
		Timeout:   options.dialTimeout,
		KeepAlive: options.keepAlive,
	}

	var errs []error
	for _, targetAddr := range spec.targetAddrs {
		upstream, err := dialer.DialTCP(ctx, "tcp", netip.AddrPort{}, targetAddr)
		if err == nil {
			return upstream, targetAddr, nil
		}
		if ctx.Err() != nil {
			return nil, netip.AddrPort{}, ctx.Err()
		}
		errs = append(errs, fmt.Errorf("%s: %w", targetAddr, err))
	}

	return nil, netip.AddrPort{}, errors.Join(errs...)
}

func tuneTCPConnection(conn net.Conn, keepAlive time.Duration) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	// Most forwarded protocols are already application-buffered by their peers.
	// Keep latency low for CONNECT handshakes and clean up dead peers promptly.
	_ = tcpConn.SetNoDelay(true)
	if keepAlive < 0 {
		_ = tcpConn.SetKeepAlive(false)
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(keepAlive)
}

func copyAndCloseWrite(wg *sync.WaitGroup, dst net.Conn, src net.Conn, options normalizedOptions) {
	defer wg.Done()

	_, err := io.Copy(dst, src)
	if err != nil && !isClosedNetworkError(err) {
		options.logf("copy %s -> %s failed: %v", src.RemoteAddr(), dst.RemoteAddr(), err)
	}
	// Propagate EOF in one direction without tearing down the peer's read side;
	// protocols can still send a final response after the other side half-closes.
	if err := closeWrite(dst); err != nil && !isClosedNetworkError(err) {
		options.logf("close write %s failed: %v", dst.RemoteAddr(), err)
	}
}

func closeWrite(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return conn.Close()
	}
	return tcpConn.CloseWrite()
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	text := err.Error()
	return strings.Contains(text, "use of closed network connection") ||
		strings.Contains(text, "connection reset by peer") ||
		strings.Contains(text, "broken pipe")
}
