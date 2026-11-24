/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	dbv1alpha1 "github.com/pedrincandido/mongo-replicaset-operator/api/v1alpha1"
	"github.com/pedrincandido/mongo-replicaset-operator/internal/mongoexec"
)

const (
	// pollHealthy is how often a Ready replica set is re-checked.
	pollHealthy = 5 * time.Second
	// pollUnsettled is used while initiating or while there is no primary, so
	// the failover duration the operator records is accurate to ~2s.
	pollUnsettled = 2 * time.Second
	execTimeout   = 15 * time.Second
	// statusTimeout bounds each status read. A hung member (process stopped,
	// TCP still open) never answers, so it must not hold up the others.
	statusTimeout = 4 * time.Second
)

// MongoReplicaSetReconciler reconciles a MongoReplicaSet object
type MongoReplicaSetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Exec     *mongoexec.Executor
	Recorder recorder.EventRecorder
}

// +kubebuilder:rbac:groups=db.pedrincandido.dev,resources=mongoreplicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.pedrincandido.dev,resources=mongoreplicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.pedrincandido.dev,resources=mongoreplicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create;get
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives one MongoReplicaSet toward its spec:
//  1. make sure the Kubernetes objects exist (Service, StatefulSet, PDB, backups);
//  2. read the replica set's own view of itself from a running member;
//  3. initiate it once, add or remove one member when the spec changes;
//  4. publish what it saw to status, events and metrics.
//
// Mongo elects the primary on its own. The operator never picks one: it
// protects the conditions for an election (majority up, members spread) and
// measures how long the cluster went without a primary.
func (r *MongoReplicaSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var m dbv1alpha1.MongoReplicaSet
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.ensureChildren(ctx, &m); err != nil {
		return ctrl.Result{}, err
	}

	pods, err := r.memberPods(ctx, &m)
	if err != nil {
		return ctrl.Result{}, err
	}
	ready := readyPods(pods)

	st := &m.Status
	st.ObservedGeneration = m.Generation

	rs, from := r.readStatus(ctx, &m, ready)
	switch {
	case rs == nil:
		// No member answered. Before initiation that just means pods are still
		// starting; afterwards it is an outage.
		if meta.IsStatusConditionTrue(st.Conditions, dbv1alpha1.ConditionInitialized) {
			r.observe(&m, &mongoexec.ReplSetStatus{})
		} else {
			st.Phase = dbv1alpha1.PhasePending
		}

	case rs.OK == 0 && rs.Code == mongoexec.CodeNotYetInitialized:
		if int32(len(ready)) < m.Spec.Members {
			st.Phase = dbv1alpha1.PhasePending
			break
		}
		// Every member answers: form the replica set from member 0.
		cctx, cancel := context.WithTimeout(ctx, execTimeout)
		err := r.Exec.Initiate(cctx, m.Namespace, from, m.Name, hostsFor(&m))
		cancel()
		if err != nil {
			r.event(&m, corev1.EventTypeWarning, "InitiateFailed", "Initiate", err.Error())
			return ctrl.Result{RequeueAfter: pollUnsettled}, nil
		}
		log.Info("replica set initiated", "members", m.Spec.Members)
		r.event(&m, corev1.EventTypeNormal, "Initiated", "Initiate", fmt.Sprintf("rs.initiate with %d members", m.Spec.Members))
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: dbv1alpha1.ConditionInitialized, Status: metav1.ConditionTrue, Reason: "Initiated",
			Message: "rs.initiate succeeded", ObservedGeneration: m.Generation,
		})
		st.Phase = dbv1alpha1.PhaseInitializing

	case rs.OK == 1:
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: dbv1alpha1.ConditionInitialized, Status: metav1.ConditionTrue, Reason: "Initiated",
			Message: "replica set is initiated", ObservedGeneration: m.Generation,
		})
		r.observe(&m, rs)
		if err := r.reconcileMembership(ctx, &m, rs, ready); err != nil {
			r.event(&m, corev1.EventTypeWarning, "ReconfigFailed", "Reconfig", err.Error())
		}

	default:
		st.Phase = dbv1alpha1.PhaseDegraded
		log.Info("unexpected replSetGetStatus", "code", rs.Code, "errmsg", rs.ErrMsg)
	}

	if err := r.observeBackups(ctx, &m); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Status().Update(ctx, &m); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	if st.Phase == dbv1alpha1.PhaseReady {
		return ctrl.Result{RequeueAfter: pollHealthy}, nil
	}
	return ctrl.Result{RequeueAfter: pollUnsettled}, nil
}

// ensureChildren creates or updates every object the replica set needs.
func (r *MongoReplicaSetReconciler) ensureChildren(ctx context.Context, m *dbv1alpha1.MongoReplicaSet) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		desired := headlessService(m)
		svc.Labels = desired.Labels
		if svc.CreationTimestamp.IsZero() {
			svc.Spec = desired.Spec
		} else {
			// clusterIP is immutable; keep the rest in line with the spec.
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			svc.Spec.PublishNotReadyAddresses = true
		}
		return controllerutil.SetControllerReference(m, svc, r.Scheme)
	}); err != nil {
		return fmt.Errorf("service: %w", err)
	}

	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		mutateStatefulSet(m, sts)
		// On scale-down, keep the pods until the replica set has removed
		// them; otherwise mongo loses members it still counts for majority.
		if current := int32(len(m.Status.Members)); current > m.Spec.Members {
			sts.Spec.Replicas = &current
		}
		return controllerutil.SetControllerReference(m, sts, r.Scheme)
	}); err != nil {
		return fmt.Errorf("statefulset: %w", err)
	}

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		mutatePDB(m, pdb)
		return controllerutil.SetControllerReference(m, pdb, r.Scheme)
	}); err != nil {
		return fmt.Errorf("pdb: %w", err)
	}

	if m.Spec.Backup == nil {
		return nil
	}
	pvc := backupPVC(m)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); apierrors.IsNotFound(err) {
		// No owner reference: backups must outlive the replica set they came from.
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("backup pvc: %w", err)
		}
	} else if err != nil {
		return err
	}
	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: backupName(m), Namespace: m.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		mutateCronJob(m, cj)
		return controllerutil.SetControllerReference(m, cj, r.Scheme)
	}); err != nil {
		return fmt.Errorf("backup cronjob: %w", err)
	}
	return nil
}

func (r *MongoReplicaSetReconciler) memberPods(ctx context.Context, m *dbv1alpha1.MongoReplicaSet) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(m.Namespace), client.MatchingLabels(labelsFor(m))); err != nil {
		return nil, err
	}
	pods := list.Items
	sort.Slice(pods, func(i, j int) bool { return ordinal(pods[i].Name) < ordinal(pods[j].Name) })
	return pods, nil
}

func readyPods(pods []corev1.Pod) []corev1.Pod {
	var out []corev1.Pod
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				out = append(out, p)
			}
		}
	}
	return out
}

func ordinal(podName string) int {
	n, err := strconv.Atoi(podName[strings.LastIndex(podName, "-")+1:])
	if err != nil {
		return -1
	}
	return n
}

// podFromHost turns "name-2.name.ns.svc.cluster.local:27017" into "name-2".
func podFromHost(host string) string { return strings.SplitN(host, ".", 2)[0] }

// readStatus asks every ready member at once and uses the lowest-ordinal
// member that answered. Reading them one by one let a single hung member
// delay detection by a full timeout.
func (r *MongoReplicaSetReconciler) readStatus(ctx context.Context, m *dbv1alpha1.MongoReplicaSet, ready []corev1.Pod) (*mongoexec.ReplSetStatus, string) {
	type answer struct {
		rs  *mongoexec.ReplSetStatus
		err error
	}
	answers := make([]answer, len(ready))
	var wg sync.WaitGroup
	for i, p := range ready {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, statusTimeout)
			defer cancel()
			rs, err := r.Exec.Status(cctx, m.Namespace, p.Name)
			answers[i] = answer{rs, err}
		}()
	}
	wg.Wait()
	for i, a := range answers {
		if a.err == nil {
			return a.rs, ready[i].Name
		}
		logf.FromContext(ctx).V(1).Info("member did not answer", "pod", ready[i].Name, "err", a.err)
	}
	return nil, ""
}

// observe turns replSetGetStatus into status fields, events and metrics, and
// tracks primary changes and the time spent without a primary.
func (r *MongoReplicaSetReconciler) observe(m *dbv1alpha1.MongoReplicaSet, rs *mongoexec.ReplSetStatus) {
	st := &m.Status
	labels := []string{m.Namespace, m.Name}

	primary := ""
	var primaryOptime int64
	for _, mem := range rs.Members {
		if mem.State == "PRIMARY" && mem.Health == 1 {
			primary, primaryOptime = mem.Name, mem.OptimeMS
		}
	}

	var readyCount int32
	members := make([]dbv1alpha1.MemberStatus, 0, len(rs.Members))
	for _, mem := range rs.Members {
		healthy := mem.Health == 1
		if healthy && (mem.State == "PRIMARY" || mem.State == "SECONDARY") {
			readyCount++
		}
		var lag int64
		if primary != "" && mem.State == "SECONDARY" && mem.OptimeMS > 0 {
			lag = max(0, (primaryOptime-mem.OptimeMS)/1000)
			replicationLag.WithLabelValues(m.Namespace, m.Name, mem.Name).Set(float64(lag))
		}
		members = append(members, dbv1alpha1.MemberStatus{Name: mem.Name, State: mem.State, Healthy: healthy, LagSeconds: lag})
	}
	st.Members = members
	st.ReadyMembers = readyCount
	readyMembers.WithLabelValues(labels...).Set(float64(readyCount))

	now := metav1.Now()
	previous := st.Primary
	elected := func() {
		st.PrimaryChanges++
		primaryChanges.WithLabelValues(labels...).Inc()
		// Measure from mongo's own clocks: the new primary's electionDate
		// minus the last heartbeat anyone received from the old primary.
		// That doesn't depend on how often the operator polls.
		if secs, ok := electionGap(rs, previous, primary); ok {
			st.LastFailoverSeconds = fmt.Sprintf("%.1f", secs)
			failoverSeconds.WithLabelValues(labels...).Observe(secs)
		}
	}
	switch {
	case primary == "" && previous != "" && st.PrimaryLostAt == nil:
		// Lost the primary we had: start the clock.
		st.PrimaryLostAt = &now
		r.event(m, corev1.EventTypeWarning, "PrimaryLost", "Failover", fmt.Sprintf("no primary; last primary was %s", previous))
	case primary != "" && st.PrimaryLostAt != nil:
		st.PrimaryLostAt = nil
		elected()
		r.event(m, corev1.EventTypeNormal, "PrimaryElected", "Failover", fmt.Sprintf("%s elected; election took %ss after the last heartbeat from %s", primary, orUnknown(st.LastFailoverSeconds), previous))
	case primary != "" && previous != "" && primary != previous:
		// Changed between two polls: a step-down, or an election faster than the poll.
		elected()
		r.event(m, corev1.EventTypeNormal, "PrimaryChanged", "Failover", fmt.Sprintf("primary moved from %s to %s (election %ss after the last heartbeat)", previous, primary, orUnknown(st.LastFailoverSeconds)))
	}
	if primary != "" || previous != "" {
		// Keep the last known primary while it is lost, so the next
		// election is counted as a change.
		st.Primary = primary
	}

	available := primary != ""
	primaryAvailable.WithLabelValues(labels...).Set(map[bool]float64{true: 1, false: 0}[available])
	cond := metav1.Condition{Type: dbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse, Reason: "NoPrimary", Message: "no member is PRIMARY", ObservedGeneration: m.Generation}
	if available {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, "PrimaryElected", primary+" is PRIMARY"
	}
	meta.SetStatusCondition(&st.Conditions, cond)

	switch {
	case available && readyCount == m.Spec.Members && int32(len(rs.Members)) == m.Spec.Members:
		st.Phase = dbv1alpha1.PhaseReady
	case !available && st.PrimaryChanges == 0 && st.LastFailoverSeconds == "" && previous == "":
		st.Phase = dbv1alpha1.PhaseInitializing // first election after rs.initiate
	default:
		st.Phase = dbv1alpha1.PhaseDegraded
	}
}

// electionGap is how long the replica set had no primary, from the point of
// view of the member that answered: new primary's electionDate minus the last
// heartbeat received from the old primary. A clean step-down keeps heartbeats
// flowing, so it comes out as zero.
func electionGap(rs *mongoexec.ReplSetStatus, oldPrimary, newPrimary string) (float64, bool) {
	var lastHeartbeat, election int64
	for _, mem := range rs.Members {
		switch mem.Name {
		case oldPrimary:
			lastHeartbeat = mem.LastHeartbeatMS
		case newPrimary:
			election = mem.ElectionMS
		}
	}
	if lastHeartbeat == 0 || election == 0 {
		return 0, false
	}
	return max(0, float64(election-lastHeartbeat)/1000), true
}

func orUnknown(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// reconcileMembership adds or removes one member per pass, on the primary.
// One voting member per reconfig is the safe-reconfig rule: a bigger jump can
// leave two configs with two different majorities.
func (r *MongoReplicaSetReconciler) reconcileMembership(ctx context.Context, m *dbv1alpha1.MongoReplicaSet, rs *mongoexec.ReplSetStatus, ready []corev1.Pod) error {
	if m.Status.Primary == "" || m.Status.PrimaryLostAt != nil {
		return nil // only reconfigure from a healthy primary
	}
	primaryPod := podFromHost(m.Status.Primary)

	current := map[string]bool{}
	for _, mem := range rs.Members {
		current[mem.Name] = true
	}
	readyNames := map[string]bool{}
	for _, p := range ready {
		readyNames[p.Name] = true
	}

	cctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	for _, host := range hostsFor(m) {
		if !current[host] && readyNames[podFromHost(host)] {
			if err := r.Exec.AddMember(cctx, m.Namespace, primaryPod, host); err != nil {
				return err
			}
			r.event(m, corev1.EventTypeNormal, "MemberAdded", "Reconfig", "rs.add "+host)
			return nil
		}
	}

	desired := map[string]bool{}
	for _, h := range hostsFor(m) {
		desired[h] = true
	}
	for _, mem := range rs.Members {
		if desired[mem.Name] {
			continue
		}
		if mem.Name == m.Status.Primary {
			// Can't remove the primary: ask it to step down and retry next pass.
			if _, err := r.Exec.Eval(cctx, m.Namespace, primaryPod, `rs.stepDown(30)`); err != nil {
				logf.FromContext(ctx).V(1).Info("stepDown returned", "err", err)
			}
			r.event(m, corev1.EventTypeNormal, "StepDown", "Reconfig", "primary steps down before it is removed")
			return nil
		}
		if err := r.Exec.RemoveMember(cctx, m.Namespace, primaryPod, mem.Name); err != nil {
			return err
		}
		r.event(m, corev1.EventTypeNormal, "MemberRemoved", "Reconfig", "rs.remove "+mem.Name)
		return nil
	}
	return nil
}

// observeBackups copies the CronJob's last success into status and metrics.
func (r *MongoReplicaSetReconciler) observeBackups(ctx context.Context, m *dbv1alpha1.MongoReplicaSet) error {
	if m.Spec.Backup == nil {
		return nil
	}
	var cj batchv1.CronJob
	if err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: backupName(m)}, &cj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if t := cj.Status.LastSuccessfulTime; t != nil {
		m.Status.LastBackupTime = t
		lastBackup.WithLabelValues(m.Namespace, m.Name).Set(float64(t.Unix()))
	}
	return nil
}

func (r *MongoReplicaSetReconciler) event(m *dbv1alpha1.MongoReplicaSet, eventType, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(m, nil, eventType, reason, action, "%s", note)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1alpha1.MongoReplicaSet{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&batchv1.CronJob{}).
		Named("mongoreplicaset").
		Complete(r)
}
