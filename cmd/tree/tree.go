// Package tree implements the 'ehz tree' verb command.
package tree

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/chay-24/ehz/cmd/shared"
	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/openshift"
	"github.com/joewhite86/cli"
)

const labelCluster = "strimzi.io/cluster"

// Tree returns the 'ehz tree' command for displaying the resource tree of a
// Strimzi Kafka cluster.
func Tree() *cli.Command {
	return &cli.Command{
		Name:  "tree",
		Short: "Show Strimzi Kafka resource dependency tree.",
		Long:  "Show the resource dependency tree of a Strimzi Kafka cluster on OpenShift.",
		Args: []cli.Arg{
			{Name: "cluster", Description: "Kafka cluster name (defaults to all clusters in the namespace)"},
		},
		Flags: []cli.Flag{shared.OutputFlag},
		Run:   run,
	}
}

func run(_ context.Context, params cli.Params) error {
	cfg, env, err := shared.LoadEnv()
	if err != nil {
		return err
	}

	name, _ := params["cluster"].(string)

	clusters, err := fetchKafkas(env, name)
	if err != nil {
		return err
	}

	if len(clusters) == 0 {
		return fmt.Errorf("no Kafka CR found in namespace %q", env.Namespace)
	}

	if shared.OutputFormat(params) == "json" {
		out := make([]map[string]any, 0, len(clusters))
		for _, k := range clusters {
			t, err := build(env, k)
			if err != nil {
				return err
			}
			out = append(out, t.toJSON())
		}

		return shared.PrintJSON(out)
	}

	fmt.Printf("\nResource tree for %s / %s\n\n", cfg.Current, env.Namespace)

	for i, k := range clusters {
		t, err := build(env, k)
		if err != nil {
			return err
		}

		renderRoot(t)

		if i < len(clusters)-1 {
			fmt.Println()
		}
	}

	return nil
}

// build assembles the dependency tree rooted at the given Kafka CR.
func build(env *config.Environment, k kafkaCR) (*node, error) {
	sel := labelCluster + "=" + k.Metadata.Name

	pools, err := list(env, "kafkanodepool", sel)
	if err != nil {
		return nil, err
	}

	podsets, err := list(env, "strimzipodset", sel)
	if err != nil {
		return nil, err
	}

	pods, err := list(env, "pod", sel)
	if err != nil {
		return nil, err
	}

	services, err := list(env, "service", sel)
	if err != nil {
		return nil, err
	}

	topics, err := list(env, "kafkatopic", sel)
	if err != nil {
		return nil, err
	}

	users, err := list(env, "kafkauser", sel)
	if err != nil {
		return nil, err
	}

	// Pods grouped by owning StrimziPodSet.
	podsByOwner := make(map[string][]item)
	for _, p := range pods.Items {
		for _, ref := range p.Metadata.OwnerReferences {
			if ref.Kind == "StrimziPodSet" {
				podsByOwner[ref.Name] = append(podsByOwner[ref.Name], p)
			}
		}
	}

	// StrimziPodSets indexed by owning kafkaNodePool name.
	podsetByPool := make(map[string]item)
	for _, ps := range podsets.Items {
		owner := ""

		for _, ref := range ps.Metadata.OwnerReferences {
			if ref.Kind == "KafkaNodePool" {
				owner = ref.Name
				break
			}
		}

		if owner == "" {
			// Naming convention fallback: <cluster>-<pool>
			owner = strings.TrimPrefix(ps.Metadata.Name, k.Metadata.Name+"-")
		}

		podsetByPool[owner] = ps
	}

	root := &node{label: fmt.Sprintf("Kafka/%s [%s] v%s", k.Metadata.Name, shared.ReadyStatus(k.Status.Conditions), k.Status.KafkaVersion)}

	// KafkaNodePools -> StrimziPodSets -> Pods.
	sort.Slice(pools.Items, func(i, j int) bool {
		return pools.Items[i].Metadata.Name < pools.Items[j].Metadata.Name
	})

	for _, np := range pools.Items {
		roles, replicas := nodePoolInfo(np)
		n := root.add(fmt.Sprintf("KafkaNodePool/%s [%s] (replicas: %d, roles: %s)",
			np.Metadata.Name, shared.ReadyStatus(np.Status.Conditions), replicas, roles))

		ps, ok := podsetByPool[np.Metadata.Name]
		if !ok {
			continue
		}

		delete(podsetByPool, np.Metadata.Name)
		appendPodset(n, ps, podsByOwner[ps.Metadata.Name])
	}

	// Orphan StrimziPodSets (no matching KafkaNodePool)
	if len(podsetByPool) > 0 {
		orphans := make([]item, 0, len(podsetByPool))
		for _, ps := range podsetByPool {
			orphans = append(orphans, ps)
		}

		sort.Slice(orphans, func(i, j int) bool {
			return orphans[i].Metadata.Name < orphans[j].Metadata.Name
		})

		for _, ps := range orphans {
			appendPodset(root, ps, podsByOwner[ps.Metadata.Name])
		}
	}

	addNamedGroup(root, "Services", "Service", services.Items, withServiceType)
	addNamedGroup(root, "KafkaTopics", "KafkaTopic", topics.Items, withReady)
	addNamedGroup(root, "KafkaUsers", "KafkaUser", users.Items, withReady)

	return root, nil
}

// appendPodset adds a StrimziPodSet node and all of its pods.
func appendPodset(parent *node, ps item, pods []item) {
	psn := parent.add(fmt.Sprintf("StrimziPodSet/%s %s",
		ps.Metadata.Name, podsetSummary(ps, pods)))

	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Metadata.Name < pods[j].Metadata.Name
	})

	for _, p := range pods {
		psn.add(podLabel(p))
	}
}

// podsetSummary returns "pod=<ready>/<total> [Ready|NotReady]" using the
// StrimziPodSet status when present and falling back to live pod readiness.
func podsetSummary(ps item, pods []item) string {
	var s struct {
		Pods      int `json:"pods"`
		ReadyPods int `json:"readyPods"`
	}

	_ = json.Unmarshal(ps.Status.Raw, &s)

	total := s.Pods
	ready := s.ReadyPods
	if total == 0 {
		total = len(pods)
		for _, p := range pods {
			if podReady(p) {
				ready++
			}
		}
	}

	state := "Ready"
	if ready < total {
		state = "NotReady"
	}

	return fmt.Sprintf("pod=%d/%d [%s]", ready, total, state)
}

func podReady(p item) bool {
	if len(p.Status.ContainerStatuses) == 0 {
		return false
	}

	for _, c := range p.Status.ContainerStatuses {
		if !c.Ready {
			return false
		}
	}

	return true
}

// addNamedGroup adds a "<Group> (n)" parent node listing each item by kind/name,
// with an optional decorator for per item suffix
func addNamedGroup(parent *node, group, kind string, items []item, deco func(item) string) {
	if len(items) == 0 {
		return
	}

	g := parent.add(fmt.Sprintf("%s (%d)", group, len(items)))

	sort.Slice(items, func(i, j int) bool {
		return items[i].Metadata.Name < items[j].Metadata.Name
	})

	for _, it := range items {
		label := fmt.Sprintf("%s/%s", kind, it.Metadata.Name)
		if deco != nil {
			label += " " + deco(it)
		}

		g.add(label)
	}
}

func withReady(it item) string {
	return "[" + shared.ReadyStatus(it.Status.Conditions) + "]"
}

func withServiceType(it item) string {
	var s struct {
		Type      string `json:"type"`
		ClusterIP string `json:"clusterIP"`
	}

	_ = json.Unmarshal(it.Spec, &s)

	t := s.Type
	if t == "" {
		t = "ClusterIP"
	}

	if s.ClusterIP == "None" {
		t = "Headless"
	}

	return "(" + t + ")"
}

func nodePoolInfo(np item) (roles []string, replicas int) {
	var s struct {
		Replicas int      `json:"replicas"`
		Roles    []string `json:"roles"`
	}

	_ = json.Unmarshal(np.Spec, &s)

	return s.Roles, s.Replicas
}

// podLabel renders a pod node, surfacing container level error reasons such as
// CrashLoopBackOff or ImagePullBackOff or OOMKilled, plus restart count if > 0.
func podLabel(p item) string {
	ready, total := 0, len(p.Status.ContainerStatuses)
	restarts := 0
	var reasons []string
	seem := map[string]bool{}

	for _, c := range p.Status.ContainerStatuses {
		if c.Ready {
			ready++
		}

		restarts += c.RestartCount

		reason := ""

		switch {
		case c.State.Waiting != nil && c.State.Waiting.Reason != "":
			reason = c.State.Waiting.Reason
		case c.State.Terminated != nil && c.State.Terminated.Reason != "":
			reason = c.State.Terminated.Reason
		}

		if reason != "" && reason != "Completed" && !seem[reason] {
			reasons = append(reasons, reason)
			seem[reason] = true
		}
	}

	phase := p.Status.Phase
	if phase == "" {
		phase = "Unknown"
	}

	label := fmt.Sprintf("Pod/%s %d/%d %s", p.Metadata.Name, ready, total, phase)
	if len(reasons) > 0 {
		label += "  (" + strings.Join(reasons, ", ") + ")"
	}

	if restarts > 0 {
		label += fmt.Sprintf(" restarts: %d", restarts)
	}

	return label
}

// fetchKafkas returns all Kafka CRs in the namespace, optionally filtered to one.
func fetchKafkas(env *config.Environment, name string) ([]kafkaCR, error) {
	args := []string{"get", "kafka", "-o", "json"}
	if name != "" {
		args = []string{"get", "kafka", name, "-o", "json"}
	}

	out, err := openshift.Run(env.Cluster, env.Namespace, args...)
	if err != nil {
		return nil, err
	}

	if name != "" {
		var k kafkaCR
		if err := json.Unmarshal(out, &k); err != nil {
			return nil, fmt.Errorf("parsing Kafka CR %q: %w", name, err)
		}

		return []kafkaCR{k}, nil
	}

	var l struct {
		Items []kafkaCR `json:"items"`
	}

	if err := json.Unmarshal(out, &l); err != nil {
		return nil, fmt.Errorf("parsing Kafka list: %w", err)
	}

	return l.Items, nil
}

// list runs 'oc get <kind> -l <selector> -o json'. Returns an empty list when
// the CRD is not installed on the cluster.
func list(env *config.Environment, kind, selector string) (itemList, error) {
	var l itemList

	out, err := openshift.Run(env.Cluster, env.Namespace,
		"get", kind,
		"-l", selector,
		"-o", "json",
	)
	if err != nil {
		if isCRDMissing(err) {
			return l, nil
		}

		return l, err
	}

	if len(out) == 0 {
		return l, nil
	}

	if err := json.Unmarshal(out, &l); err != nil {
		return l, fmt.Errorf("parsing %s list: %w", kind, err)
	}

	return l, nil
}

// isCRDMissing recognizes the 'oc get' error returned when the requested kind
// is not registered on the cluster.
func isCRDMissing(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "doesn't have a resource type") ||
		strings.Contains(msg, "the server could not find the requested resource") ||
		strings.Contains(msg, "no matches for kind")
}

// minimal types for unmarshalling the oc -o json output
type kafkaCR struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		KafkaVersion string                `json:"kafkaVersion"`
		Conditions   []shared.K8sCondition `json:"conditions"`
	} `json:"status"`
}

type item struct {
	Metadata struct {
		Name            string     `json:"name"`
		OwnerReferences []ownerRef `json:"ownerReferences"`
	} `json:"metadata"`
	Spec   json.RawMessage `json:"spec"`
	Status itemStatus      `json:"status"`
}

type itemStatus struct {
	Conditions        []shared.K8sCondition `json:"conditions"`
	Phase             string                `json:"phase,omitempty"`
	ContainerStatuses []containerStatus     `json:"containerStatuses,omitempty"`
	Raw               json.RawMessage       `json:"-"`
}

func (s *itemStatus) UnmarshalJSON(data []byte) error {
	type alias itemStatus
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*s = itemStatus(a)
	s.Raw = append(s.Raw[:0], data...)

	return nil
}

type ownerRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type containerStatus struct {
	Ready        bool           `json:"ready"`
	RestartCount int            `json:"restartCount"`
	State        containerState `json:"state"`
}

type containerState struct {
	Waiting    *containerStateReason `json:"waiting,omitempty"`
	Terminated *containerStateReason `json:"terminated,omitempty"`
}

type containerStateReason struct {
	Reason string `json:"reason"`
}

type itemList struct {
	Items []item `json:"items"`
}

// tree node + renderer

type node struct {
	label    string
	children []*node
}

func (n *node) add(label string) *node {
	child := &node{label: label}
	n.children = append(n.children, child)

	return child
}

func (n *node) toJSON() map[string]any {
	m := map[string]any{"label": n.label}
	if len(n.children) > 0 {
		children := make([]map[string]any, 0, len(n.children))
		for _, c := range n.children {
			children = append(children, c.toJSON())
		}
		m["children"] = children
	}

	return m
}

func renderRoot(n *node) {
	fmt.Println(n.label)

	render(n, "")
}

func render(n *node, prefix string) {
	for i, c := range n.children {
		last := i == len(n.children)-1
		branch, ext := "├── ", "│   "
		if last {
			branch, ext = "└── ", "    "
		}

		fmt.Println(prefix + branch + c.label)
		render(c, prefix+ext)
	}
}
