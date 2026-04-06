package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/joewhite86/cli"
)

// outputFlag is the standard -o / --output flag shared by list and describe
// commands. Accepted values: "table" (default) or "json".
var outputFlag = cli.Flag{
	Short:       "o",
	Name:        "output",
	HasValue:    true,
	Description: `Output format: "table" (default) or "json".`,
	Default:     "table",
}

// outputFormat extracts the --output flag value from params, falling back to
// "table" if the flag was not provided.
func outputFormat(params cli.Params) string {
	if v, ok := params["output"].(string); ok {
		return v
	}

	return "table"
}

// printJSON marshals v to indented JSON and writes it to stdout.
func printJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

// dashes returns a string of n '-' characters, used for table header
// separators.
func dashes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}

	return string(b)
}

// k8sCondition is a standard Kubernetes status condition (type + status pair).
type k8sCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// readyStatus scans a condition slice and returns "Ready", "NotReady", or
// "Unknown".
func readyStatus(conditions []k8sCondition) string {
	for _, c := range conditions {
		if c.Type == "Ready" {
			if c.Status == "True" {
				return "Ready"
			}

			return "NotReady"
		}
	}

	return "Unknown"
}
