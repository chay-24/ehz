// Package shared provides CLI helpers shared across all ehz verb commands.
package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joewhite86/cli"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OutputFlag is the standard -o / --output flag shared by all list and describe
// commands. Accepted values: "table" (default) or "json".
var OutputFlag = cli.Flag{
	Short:       "o",
	Name:        "output",
	HasValue:    true,
	Description: `Output format: "table" (default) or "json".`,
	Default:     "table",
}

// OutputFormat extracts the --output flag value from params, falling back to
// "table" if the flag was not provided.
func OutputFormat(params cli.Params) string {
	if v, ok := params["output"].(string); ok {
		return v
	}

	return "table"
}

// PrintJSON marshals v to indented JSON and writes it to stdout.
func PrintJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

// Dashes returns a string of n '-' characters, used for table header separators.
func Dashes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}

	return string(b)
}

// ErrDone is a sentinel used to stop a consume loop cleanly.
var ErrDone = fmt.Errorf("done")

// PrintRecord writes a single Kafka record to stdout.
// In JSON mode it emits a full envelope; otherwise it pretty-prints the value.
func PrintRecord(r *kgo.Record, asJSON bool) error {
	if asJSON {
		envelope := map[string]interface{}{
			"partition": r.Partition,
			"offset":    r.Offset,
			"timestamp": r.Timestamp.Format(time.RFC3339),
			"key":       string(r.Key),
			"value":     json.RawMessage(r.Value),
		}

		return PrintJSON(envelope)
	}

	var pretty interface{}
	if json.Unmarshal(r.Value, &pretty) == nil {
		out, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Printf("[p%d o%d] %s\n", r.Partition, r.Offset, out)
	} else {
		fmt.Printf("[p%d o%d] %s\n", r.Partition, r.Offset, r.Value)
	}

	return nil
}

// BuildConsumeOpts constructs kgo consumer options from the consume/find flags.
func BuildConsumeOpts(topic string, fromBeginning bool, partition int32, offset int64) []kgo.Opt {
	if partition >= 0 {
		startOff := kgo.NewOffset().AtEnd()
		if fromBeginning {
			startOff = kgo.NewOffset().AtStart()
		}
		if offset >= 0 {
			startOff = kgo.NewOffset().At(offset)
		}
		return []kgo.Opt{
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {partition: startOff},
			}),
		}
	}

	resetOff := kgo.NewOffset().AtEnd()
	if fromBeginning {
		resetOff = kgo.NewOffset().AtStart()
	}
	return []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(resetOff),
	}
}

// Int64Parser parses an int64 flag value.
func Int64Parser(val string) (interface{}, error) {
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return int64(-1), err
	}

	return n, nil
}

// FormatInt32Slice renders a slice of broker IDs as "[1 2 3]".
func FormatInt32Slice(ids []int32) string {
	if len(ids) == 0 {
		return "[]"
	}

	s := "["
	for i, id := range ids {
		if i > 0 {
			s += " "
		}
		s += strconv.Itoa(int(id))
	}

	return s + "]"
}

// K8sCondition is a standard Kubernetes status condition (type + status pair).
type K8sCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// ReadyStatus scans a condition slice and returns "Ready", "NotReady", or "Unknown".
func ReadyStatus(conditions []K8sCondition) string {
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
