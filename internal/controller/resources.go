package controller

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	dbv1alpha1 "github.com/pedrincandido/mongo-replicaset-operator/api/v1alpha1"
	"github.com/pedrincandido/mongo-replicaset-operator/internal/mongoexec"
)

const (
	mongoPort = 27017
	appName   = "mongodb"
)

func labelsFor(m *dbv1alpha1.MongoReplicaSet) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       appName,
		"app.kubernetes.io/instance":   m.Name,
		"app.kubernetes.io/managed-by": "mongo-replicaset-operator",
	}
}

func image(m *dbv1alpha1.MongoReplicaSet) string { return "mongo:" + m.Spec.Version }

var defaultStorage = resource.MustParse("1Gi")

// orDefault fills in a zero Quantity. The CRD defaults storageSize, but a Go
// client that leaves the field empty still sends "0", and an API-server
// default never applies to a value that is present.
func orDefault(q resource.Quantity) resource.Quantity {
	if q.IsZero() {
		return defaultStorage
	}
	return q
}

// hostFor is the stable DNS name of member i, served by the headless Service.
func hostFor(m *dbv1alpha1.MongoReplicaSet, i int32) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", m.Name, i, m.Name, m.Namespace, mongoPort)
}

func hostsFor(m *dbv1alpha1.MongoReplicaSet) []string {
	hosts := make([]string, m.Spec.Members)
	for i := range m.Spec.Members {
		hosts[i] = hostFor(m, i)
	}
	return hosts
}

// connectionString lets clients (and mongodump) discover the primary themselves.
func connectionString(m *dbv1alpha1.MongoReplicaSet, readPreference string) string {
	return fmt.Sprintf("mongodb://%s/?replicaSet=%s&readPreference=%s", strings.Join(hostsFor(m), ","), m.Name, readPreference)
}

// majority is the number of voting members needed to elect a primary.
func majority(members int32) int32 { return members/2 + 1 }

// headlessService gives every member a stable DNS name. Not-ready addresses
// are published because members must resolve each other before rs.initiate,
// which is also before they pass the readiness probe.
func headlessService(m *dbv1alpha1.MongoReplicaSet) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace, Labels: labelsFor(m)},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 labelsFor(m),
			Ports:                    []corev1.ServicePort{{Name: appName, Port: mongoPort, TargetPort: intstr.FromInt32(mongoPort)}},
		},
	}
}

// mutateStatefulSet sets the fields the operator owns on the StatefulSet.
// Immutable fields (selector, serviceName, volumeClaimTemplates) are only set
// on create.
func mutateStatefulSet(m *dbv1alpha1.MongoReplicaSet, sts *appsv1.StatefulSet) {
	labels := labelsFor(m)
	replicas := m.Spec.Members
	sts.Labels = labels
	sts.Spec.Replicas = &replicas

	if sts.CreationTimestamp.IsZero() {
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.ServiceName = m.Name
		// Parallel: members don't depend on each other to start; the replica
		// set forms after all of them answer, not one by one.
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: labels},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: orDefault(m.Spec.StorageSize)}},
			},
		}}
	}

	grace := int64(30)
	sts.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &grace,
			// Spread members across nodes so one node failure can't take the
			// majority. Preferred, not required, so it still schedules on a
			// single-node kind cluster.
			Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey:   "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
					},
				}},
			}},
			Containers: []corev1.Container{{
				Name:            mongoexec.Container,
				Image:           image(m),
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"mongod", "--replSet", m.Name, "--bind_ip_all", "--port", fmt.Sprint(mongoPort), "--dbpath", "/data/db"},
				Ports:           []corev1.ContainerPort{{Name: appName, ContainerPort: mongoPort}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data/db"}},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				}},
				// Ready means mongod answers; replica set health is tracked in
				// the CR status, not by pulling pods out of the Service.
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"mongosh", "--quiet", "--norc", "--eval", "db.adminCommand('ping').ok"}}},
					InitialDelaySeconds: 5, PeriodSeconds: 5, TimeoutSeconds: 5,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(mongoPort)}},
					InitialDelaySeconds: 20, PeriodSeconds: 10, FailureThreshold: 6,
				},
			}},
		},
	}
}

// mutatePDB keeps a majority of members up through voluntary disruptions
// (node drains, cluster upgrades), so a drain can never cost the primary's
// quorum.
func mutatePDB(m *dbv1alpha1.MongoReplicaSet, pdb *policyv1.PodDisruptionBudget) {
	minAvailable := intstr.FromInt32(majority(m.Spec.Members))
	pdb.Labels = labelsFor(m)
	pdb.Spec.MinAvailable = &minAvailable
	pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: labelsFor(m)}
}

func backupName(m *dbv1alpha1.MongoReplicaSet) string { return m.Name + "-backup" }

func backupPVC(m *dbv1alpha1.MongoReplicaSet) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: backupName(m), Namespace: m.Namespace, Labels: labelsFor(m)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: orDefault(m.Spec.Backup.StorageSize)}},
		},
	}
}

// backupScript dumps from a secondary when one is available, so backups don't
// load the primary, then keeps only the newest `retain` archives.
func backupScript(m *dbv1alpha1.MongoReplicaSet) string {
	return fmt.Sprintf(`set -eu
f=/backup/dump-$(date -u +%%Y%%m%%dT%%H%%M%%SZ).archive.gz
mongodump --uri '%s' --gzip --archive="$f"
echo "wrote $f"
ls -1t /backup/dump-*.archive.gz | tail -n +%d | xargs -r rm --
ls -l /backup`, connectionString(m, "secondaryPreferred"), m.Spec.Backup.Retain+1)
}

func mutateCronJob(m *dbv1alpha1.MongoReplicaSet, cj *batchv1.CronJob) {
	history := int32(3)
	backoff := int32(1)
	cj.Labels = labelsFor(m)
	cj.Spec.Schedule = m.Spec.Backup.Schedule
	cj.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
	cj.Spec.SuccessfulJobsHistoryLimit = &history
	cj.Spec.FailedJobsHistoryLimit = &history
	cj.Spec.JobTemplate = batchv1.JobTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labelsFor(m)},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "mongodump",
						Image:           image(m),
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", backupScript(m)},
						VolumeMounts:    []corev1.VolumeMount{{Name: "backup", MountPath: "/backup"}},
					}},
					Volumes: []corev1.Volume{{
						Name:         "backup",
						VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: backupName(m)}},
					}},
				},
			},
		},
	}
}
