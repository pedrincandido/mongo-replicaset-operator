// Command chaos kills the primary of a MongoReplicaSet over and over and
// measures, from a client's point of view, how long majority writes were
// unavailable each time.
//
// The writer sets socketTimeoutMS: without it, a write sent to a hung
// primary waits on an open TCP socket long after a new primary exists.
//
// A writer runs inside a surviving member (mongosh with the replica set
// connection string, so the driver finds the new primary itself). It inserts
// with writeConcern majority every 50 ms and reports the longest gap between
// two successful writes. The tool deletes the primary pod a few seconds into
// that window, waits for the replica set to be Ready again, and repeats.
//
//	go run ./cmd/chaos --name demo --rounds 5 --mode graceful
//	go run ./cmd/chaos --name demo --rounds 5 --mode freeze
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	osexec "os/exec"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	dbv1alpha1 "github.com/pedrincandido/mongo-replicaset-operator/api/v1alpha1"
	"github.com/pedrincandido/mongo-replicaset-operator/internal/mongoexec"
)

const writerScript = `
const coll = db.getSiblingDB("chaos").probe;
const end = Date.now() + %d;
let last = 0, maxGap = 0, ok = 0, failed = 0;
while (Date.now() < end) {
  try {
    coll.insertOne({ t: new Date() }, { writeConcern: { w: "majority", wtimeout: 1000 } });
    const now = Date.now();
    if (last) maxGap = Math.max(maxGap, now - last);
    last = now; ok++;
  } catch (e) { failed++; }
  sleep(50);
}
print(JSON.stringify({ maxGapMs: maxGap, ok: ok, failed: failed }));`

type writerResult struct {
	MaxGapMS int64 `json:"maxGapMs"`
	OK       int   `json:"ok"`
	Failed   int   `json:"failed"`
}

type round struct {
	n               int
	killed          string
	newPrimary      string
	gap             time.Duration
	failedWrites    int
	operatorSeconds string
}

func main() {
	name := flag.String("name", "demo", "MongoReplicaSet name")
	namespace := flag.String("namespace", "default", "namespace")
	rounds := flag.Int("rounds", 5, "how many times to kill the primary")
	mode := flag.String("mode", "graceful", "graceful | force | freeze (see inject)")
	window := flag.Duration("window", 45*time.Second, "how long the writer runs per round")
	driver := flag.String("driver", "default", "default: driver defaults; tuned: heartbeatFrequencyMS=1000, connectTimeoutMS=2000")
	out := flag.String("out", "", "append a markdown report to this file")
	flag.Parse()

	if err := run(*name, *namespace, *rounds, *mode, *driver, *window, *out); err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
}

func run(name, namespace string, rounds int, mode, driver string, window time.Duration, out string) error {
	if mode != "graceful" && mode != "force" && mode != "freeze" {
		return fmt.Errorf("--mode must be graceful, force or freeze")
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = dbv1alpha1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	ex, err := mongoexec.New(cfg)
	if err != nil {
		return err
	}
	ctx := context.Background()
	key := types.NamespacedName{Namespace: namespace, Name: name}

	var results []round
	for i := 1; i <= rounds; i++ {
		m, err := waitReady(ctx, c, key, 5*time.Minute)
		if err != nil {
			return err
		}
		primaryPod := strings.SplitN(m.Status.Primary, ".", 2)[0]
		writerPod := ""
		for j := range m.Spec.Members {
			if p := fmt.Sprintf("%s-%d", name, j); p != primaryPod {
				writerPod = p
				break
			}
		}
		uri := fmt.Sprintf("mongodb://%s/?replicaSet=%s&serverSelectionTimeoutMS=1000&socketTimeoutMS=2000",
			strings.Join(hosts(m), ","), name)
		if driver == "tuned" {
			// The driver's monitor finds out about a hung primary through its own
			// heartbeat; by default that check is slow, and it dominates the gap.
			uri += "&heartbeatFrequencyMS=1000&connectTimeoutMS=2000"
		}

		fmt.Printf("round %d: primary %s, writer in %s, mode %s\n", i, primaryPod, writerPod, mode)
		done := make(chan writerResult, 1)
		errc := make(chan error, 1)
		go func() {
			var res writerResult
			script := fmt.Sprintf(writerScript, window.Milliseconds())
			if err := ex.EvalJSON(ctx, namespace, writerPod, script, &res, uri); err != nil {
				errc <- err
				return
			}
			done <- res
		}()

		time.Sleep(5 * time.Second) // steady writes before the failure
		if err := inject(ctx, c, mode, namespace, primaryPod); err != nil {
			return err
		}

		var res writerResult
		select {
		case res = <-done:
		case err := <-errc:
			return fmt.Errorf("writer: %w", err)
		}
		if err := heal(ctx, c, mode, namespace, primaryPod); err != nil {
			return err
		}

		after, err := waitReady(ctx, c, key, 5*time.Minute)
		if err != nil {
			return err
		}
		r := round{
			n: i, killed: primaryPod, newPrimary: strings.SplitN(after.Status.Primary, ".", 2)[0],
			gap: time.Duration(res.MaxGapMS) * time.Millisecond, failedWrites: res.Failed,
			operatorSeconds: after.Status.LastFailoverSeconds,
		}
		fmt.Printf("  write gap %.2fs, failed attempts %d, new primary %s, operator saw %s\n",
			r.gap.Seconds(), r.failedWrites, r.newPrimary, orDash(r.operatorSeconds))
		results = append(results, r)
	}

	report := render(name, mode, driver, results)
	fmt.Print("\n" + report)
	if out != "" {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteString(report + "\n")
		return err
	}
	return nil
}

// inject causes the failure for one round.
//
//	graceful:  normal pod delete. mongod gets SIGTERM and steps down cleanly.
//	force:     delete with grace period 0. The kubelet still sends SIGTERM, so
//	           mongod usually still steps down: close to graceful in practice.
//	freeze:    SIGSTOP the primary's mongod (kind only). It stops answering
//	           without exiting, like a hung or partitioned node: the others must
//	           wait for missed heartbeats and electionTimeoutMillis (10 s by
//	           default) before they elect. A deny-all NetworkPolicy was tried
//	           first and did not cut the already open replication connections.
func inject(ctx context.Context, c client.Client, mode, namespace, podName string) error {
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		return err
	}
	switch mode {
	case "graceful":
		return c.Delete(ctx, pod)
	case "force":
		return c.Delete(ctx, pod, client.GracePeriodSeconds(0))
	case "freeze":
		return signalMongod(ctx, pod, "STOP")
	}
	return fmt.Errorf("unknown mode %q", mode)
}

// heal resumes a frozen mongod. It comes back as a stale primary, sees a
// newer term, and steps down to secondary on its own.
func heal(ctx context.Context, c client.Client, mode, namespace, podName string) error {
	if mode != "freeze" {
		return nil
	}
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		return err
	}
	return signalMongod(ctx, pod, "CONT")
}

// signalMongod sends a signal to the mongod process from the kind node's PID
// namespace (inside the container mongod is PID 1, which ignores SIGSTOP from
// its own namespace). kind only: the node is a Docker container named after
// the Kubernetes node.
func signalMongod(ctx context.Context, pod *corev1.Pod, sig string) error {
	if len(pod.Status.ContainerStatuses) == 0 {
		return fmt.Errorf("%s has no container status", pod.Name)
	}
	id := pod.Status.ContainerStatuses[0].ContainerID
	id = id[strings.Index(id, "://")+3:]
	script := fmt.Sprintf(`kill -%s $(crictl inspect --output go-template --template '{{.info.pid}}' %s)`, sig, id)
	out, err := osexec.CommandContext(ctx, "docker", "exec", pod.Spec.NodeName, "sh", "-c", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("signal %s to mongod in %s: %w: %s", sig, pod.Name, err, out)
	}
	return nil
}

func waitReady(ctx context.Context, c client.Client, key types.NamespacedName, timeout time.Duration) (*dbv1alpha1.MongoReplicaSet, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var m dbv1alpha1.MongoReplicaSet
		if err := c.Get(ctx, key, &m); err == nil &&
			m.Status.Phase == dbv1alpha1.PhaseReady && m.Status.ReadyMembers == m.Spec.Members && m.Status.PrimaryLostAt == nil {
			return &m, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("%s not Ready after %s", key, timeout)
}

func hosts(m *dbv1alpha1.MongoReplicaSet) []string {
	h := make([]string, m.Spec.Members)
	for i := range m.Spec.Members {
		h[i] = fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:27017", m.Name, i, m.Name, m.Namespace)
	}
	return h
}

func render(name, mode, driver string, rs []round) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s — mode `%s`, driver `%s`, %d rounds (%s)\n\n", name, mode, driver, len(rs), time.Now().Format("2006-01-02"))
	b.WriteString("| Round | Failed member | New primary | Client majority-write gap | Failed attempts | Election (server, from operator) |\n|---|---|---|---|---|---|\n")
	gaps := make([]float64, 0, len(rs))
	for _, r := range rs {
		fmt.Fprintf(&b, "| %d | %s | %s | %.2fs | %d | %s |\n", r.n, r.killed, r.newPrimary, r.gap.Seconds(), r.failedWrites, orDash(r.operatorSeconds))
		gaps = append(gaps, r.gap.Seconds())
	}
	sort.Float64s(gaps)
	if len(gaps) > 0 {
		fmt.Fprintf(&b, "\n**p50 %.2fs · p95 %.2fs · max %.2fs**\n", pct(gaps, 50), pct(gaps, 95), gaps[len(gaps)-1])
	}
	return b.String()
}

func pct(sorted []float64, p float64) float64 {
	i := int(float64(len(sorted)-1) * p / 100)
	return sorted[i]
}

// orDash formats the operator's election measurement.
func orDash(s string) string {
	if s == "" {
		return "n/a"
	}
	return s + "s"
}
