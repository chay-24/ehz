package main

import (
	"context"
	"fmt"
	"os"

	"github.com/joewhite86/cli"
)

func main() {
	root := &cli.Command{
		Name:    "ehz",
		Short:   "Explore and inspect your Kafka cluster.",
		Long:    "Explore and inspect your Kafka cluster.",
		Version: "0.1.0",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cli.Run(ctx, root); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
