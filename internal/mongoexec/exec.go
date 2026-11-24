// Package mongoexec runs mongosh inside a member pod through the Kubernetes
// exec API. The operator and the chaos tool both use it, so neither needs a
// network path to the pods: it works the same from a laptop against kind as
// it does in-cluster.
package mongoexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

// Container is the name of the mongod container in every member pod.
const Container = "mongod"

// Executor runs mongosh scripts in pods.
type Executor struct {
	config    *rest.Config
	clientset kubernetes.Interface
}

// New builds an Executor from a rest config.
func New(config *rest.Config) (*Executor, error) {
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Executor{config: config, clientset: cs}, nil
}

// Eval runs a mongosh script in the given pod and returns its stdout.
// args are appended after --eval, for example a connection string.
func (e *Executor) Eval(ctx context.Context, namespace, pod, script string, args ...string) (string, error) {
	cmd := append([]string{"mongosh", "--quiet", "--norc"}, args...)
	cmd = append(cmd, "--eval", script)

	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: Container,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	// WebSocket is the current exec protocol; fall back to SPDY on older servers.
	ws, err := remotecommand.NewWebSocketExecutor(e.config, "GET", req.URL().String())
	if err != nil {
		return "", err
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return "", err
	}
	exec, err := remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return "", err
	}

	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return stdout.String(), fmt.Errorf("exec in %s/%s: %w (stderr: %s)", namespace, pod, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// EvalJSON runs a script that prints one JSON document and decodes it into out.
func (e *Executor) EvalJSON(ctx context.Context, namespace, pod, script string, out any, args ...string) error {
	raw, err := e.Eval(ctx, namespace, pod, script, args...)
	if err != nil {
		return err
	}
	line := lastLine(raw)
	if err := json.Unmarshal([]byte(line), out); err != nil {
		return fmt.Errorf("decode mongosh output %q: %w", line, err)
	}
	return nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// ReplSetStatus is the part of replSetGetStatus the operator uses.
type ReplSetStatus struct {
	OK      int      `json:"ok"`
	Code    int      `json:"code"`
	ErrMsg  string   `json:"errmsg"`
	Members []Member `json:"members"`
}

// Member is one entry of replSetGetStatus.members.
type Member struct {
	Name     string  `json:"name"`
	State    string  `json:"state"`
	Health   float64 `json:"health"`
	OptimeMS int64   `json:"optime"`
	// LastHeartbeatMS is when the answering member last heard from this one
	// (0 for the answering member itself).
	LastHeartbeatMS int64 `json:"lastHeartbeat"`
	// ElectionMS is when this member became primary (primaries only).
	ElectionMS int64 `json:"election"`
}

// Mongo error codes the operator branches on.
const (
	CodeNotYetInitialized    = 94
	CodeAlreadyInitialized   = 23
	CodeNoReplicationEnabled = 76
)

// statusScript prints replSetGetStatus as JSON, or the error code when the
// replica set is not initiated yet. It never throws, so exec only fails when
// the pod itself is unreachable.
const statusScript = `
try {
  const s = db.adminCommand({ replSetGetStatus: 1 });
  print(JSON.stringify({
    ok: s.ok, code: s.code || 0, errmsg: s.errmsg || "",
    members: (s.members || []).map(m => ({
      name: m.name, state: m.stateStr, health: m.health,
      optime: m.optimeDate ? m.optimeDate.getTime() : 0,
      lastHeartbeat: m.lastHeartbeatRecv ? m.lastHeartbeatRecv.getTime() : 0,
      election: m.electionDate ? m.electionDate.getTime() : 0
    }))
  }));
} catch (e) {
  print(JSON.stringify({ ok: 0, code: e.code || -1, errmsg: e.message }));
}`

// Status reads replSetGetStatus from one member.
func (e *Executor) Status(ctx context.Context, namespace, pod string) (*ReplSetStatus, error) {
	var st ReplSetStatus
	if err := e.EvalJSON(ctx, namespace, pod, statusScript, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// Initiate runs rs.initiate with the given hosts. An already initiated set
// counts as success, which keeps the call idempotent across restarts.
func (e *Executor) Initiate(ctx context.Context, namespace, pod, setName string, hosts []string) error {
	members := make([]map[string]any, len(hosts))
	for i, h := range hosts {
		members[i] = map[string]any{"_id": i, "host": h}
	}
	cfg, _ := json.Marshal(map[string]any{"_id": setName, "members": members})
	script := fmt.Sprintf(`
try { const r = rs.initiate(%s); print(JSON.stringify({ ok: r.ok, code: r.code || 0, errmsg: r.errmsg || "" })); }
catch (e) { print(JSON.stringify({ ok: 0, code: e.code || -1, errmsg: e.message })); }`, cfg)
	var res ReplSetStatus
	if err := e.EvalJSON(ctx, namespace, pod, script, &res); err != nil {
		return err
	}
	if res.OK == 1 || res.Code == CodeAlreadyInitialized {
		return nil
	}
	return fmt.Errorf("rs.initiate failed (code %d): %s", res.Code, res.ErrMsg)
}

// AddMember adds one host to the replica set. It must run on the primary.
// The operator adds and removes one voting member per reconfig, which is what
// MongoDB's safe reconfig rules allow.
func (e *Executor) AddMember(ctx context.Context, namespace, primaryPod, host string) error {
	return e.reconfig(ctx, namespace, primaryPod, fmt.Sprintf(`rs.add(%q)`, host))
}

// RemoveMember removes one host from the replica set. It must run on the primary.
func (e *Executor) RemoveMember(ctx context.Context, namespace, primaryPod, host string) error {
	return e.reconfig(ctx, namespace, primaryPod, fmt.Sprintf(`rs.remove(%q)`, host))
}

func (e *Executor) reconfig(ctx context.Context, namespace, pod, call string) error {
	script := fmt.Sprintf(`
try { const r = %s; print(JSON.stringify({ ok: r.ok, code: r.code || 0, errmsg: r.errmsg || "" })); }
catch (e) { print(JSON.stringify({ ok: 0, code: e.code || -1, errmsg: e.message })); }`, call)
	var res ReplSetStatus
	if err := e.EvalJSON(ctx, namespace, pod, script, &res); err != nil {
		return err
	}
	if res.OK != 1 {
		return fmt.Errorf("%s failed (code %d): %s", call, res.Code, res.ErrMsg)
	}
	return nil
}
