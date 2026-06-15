package kafka

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Filter holds a set of field conditions that must all match (AND semantics).
// Conditions are parsed from a comma-separated expression string.
//
// Supported operators:
//
//	field=value    exact match (string comparison after fmt.Sprintf)
//	field~=value   substring match
//
// Dot notation traverses nested objects: metadata.source=payments.
type Filter struct {
	conditions []condition
}

type condition struct {
	path  []string // dot-split field path
	op    string   // "=" or "~="
	value string
}

// ParseFilter parses a comma-separated list of filter expressions.
// Returns nil (no filter) if expr is empty.
func ParseFilter(expr string) (*Filter, error) {
	if expr == "" {
		return nil, nil
	}

	var f Filter
	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		c, err := parseCondition(part)
		if err != nil {
			return nil, err
		}

		f.conditions = append(f.conditions, c)
	}

	return &f, nil
}

func parseCondition(expr string) (condition, error) {
	if idx := strings.Index(expr, "~="); idx >= 0 {
		return condition{
			path:  strings.Split(expr[:idx], "."),
			op:    "~=",
			value: expr[idx+2:],
		}, nil
	}
	if idx := strings.Index(expr, "="); idx >= 0 {
		return condition{
			path:  strings.Split(expr[:idx], "."),
			op:    "=",
			value: expr[idx+1:],
		}, nil
	}

	return condition{}, fmt.Errorf("invalid filter %q: expected field=value or field~=value", expr)
}

// Match returns true if all conditions are satisfied by the JSON message value.
// Non-JSON payloads never match a non-nil filter.
func (f *Filter) Match(msg []byte) bool {
	if f == nil {
		return true
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(msg, &obj); err != nil {
		return false
	}

	for _, c := range f.conditions {
		v := walkPath(obj, c.path)
		if v == nil {
			return false
		}

		s := fmt.Sprintf("%v", v)
		switch c.op {
		case "=":
			if s != c.value {
				return false
			}

		case "~=":
			if !strings.Contains(s, c.value) {
				return false
			}
		}
	}

	return true
}

// walkPath traverses a nested map following the given key path.
// Returns nil if any segment is missing or not a map.
func walkPath(obj map[string]interface{}, path []string) interface{} {
	var cur interface{} = obj
	for _, key := range path {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}

		cur = m[key]
	}

	return cur
}
