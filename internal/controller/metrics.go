package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// crLabels identify the MongoReplicaSet a series belongs to.
var crLabels = []string{"namespace", "name"}

// SLIs exported on the manager's /metrics endpoint. Labels are the
// MongoReplicaSet's namespace and name.
var (
	primaryAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongors_primary_available",
		Help: "1 when the replica set has a primary that can take writes, 0 otherwise.",
	}, crLabels)

	readyMembers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongors_ready_members",
		Help: "Members that are healthy and in PRIMARY or SECONDARY state.",
	}, crLabels)

	primaryChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mongors_primary_changes_total",
		Help: "Times the operator observed a new primary.",
	}, crLabels)

	failoverSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mongors_failover_duration_seconds",
		Help:    "Election duration from mongo's clocks: new primary's electionDate minus the last heartbeat from the old one.",
		Buckets: []float64{1, 2, 5, 10, 15, 20, 30, 60},
	}, crLabels)

	replicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongors_replication_lag_seconds",
		Help: "How far each secondary's oplog is behind the primary.",
	}, append(append([]string{}, crLabels...), "member"))

	lastBackup = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mongors_last_backup_timestamp_seconds",
		Help: "Unix time of the last successful backup job.",
	}, crLabels)
)

func init() {
	metrics.Registry.MustRegister(primaryAvailable, readyMembers, primaryChanges, failoverSeconds, replicationLag, lastBackup)
}
