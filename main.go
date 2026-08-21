// Command ehz is a CLI for exploring and inspecting Strimzi Kafka clusters
// running on OpenShift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/chay-24/ehz/cmd"
	"github.com/joewhite86/cli"
)

const exitInterrupted = 130

func main() {
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(exitInterrupted)
		}

		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// run wraps the command so deferred cleanup still happens; os.Exit in main
// would skip it.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return cli.Run(ctx, cmd.Root())
}
