package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sandrino/portbeam"
)

var (
	version      = "dev"
	exitProcess  = os.Exit
	runForwarder = portbeam.Run
)

type forwardFlag []string

func (flags *forwardFlag) String() string {
	return strings.Join(*flags, ",")
}

func (flags *forwardFlag) Set(value string) error {
	*flags = append(*flags, value)
	return nil
}

func main() {
	exitProcess(run(os.Args[1:], os.Stderr))
}

func run(args []string, output io.Writer) int {
	var forwards forwardFlag
	var showVersion bool

	flags := flag.NewFlagSet("portbeam", flag.ContinueOnError)
	flags.SetOutput(output)
	shutdownTimeout := flags.Duration("shutdown-timeout", portbeam.DefaultShutdownTimeout, "maximum time to drain active connections after shutdown before closing them")
	dialTimeout := flags.Duration("dial-timeout", portbeam.DefaultDialTimeout, "maximum time to establish each target connection")
	keepAlive := flags.Duration("keepalive", portbeam.DefaultKeepAlive, "TCP keepalive period; set to a negative duration to disable")
	flags.BoolVar(&showVersion, "version", false, "print version and exit")
	flags.Var(&forwards, "forward", "TCP forward in listen=target form; may be repeated")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if showVersion {
		fmt.Fprintf(flags.Output(), "portbeam %s\n", version)
		return 0
	}

	specs, err := portbeam.ParseSpecs(forwards)
	if err != nil {
		fmt.Fprintf(flags.Output(), "configuration error: %v\n\n", err)
		flags.Usage()
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := log.New(output, "", log.LstdFlags)
	err = runForwarder(ctx, specs, portbeam.Options{
		ShutdownTimeout: *shutdownTimeout,
		DialTimeout:     *dialTimeout,
		KeepAlive:       *keepAlive,
		Logger:          logger,
	})
	if err != nil {
		logger.Printf("portbeam stopped: %v", err)
		return 1
	}
	return 0
}
