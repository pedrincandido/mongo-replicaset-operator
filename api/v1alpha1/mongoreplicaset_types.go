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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Phase is the coarse lifecycle state of a replica set.
// +kubebuilder:validation:Enum=Pending;Initializing;Ready;Degraded
type Phase string

const (
	PhasePending      Phase = "Pending"      // StatefulSet created, members not all running yet
	PhaseInitializing Phase = "Initializing" // members running, replica set not initiated or no primary yet
	PhaseReady        Phase = "Ready"        // one primary, every member healthy
	PhaseDegraded     Phase = "Degraded"     // initiated, but no primary or a member is unhealthy
)

// Condition types written to status.conditions.
const (
	ConditionInitialized = "Initialized" // rs.initiate has run successfully
	ConditionAvailable   = "Available"   // a primary accepts writes
)

// BackupSpec schedules logical backups with mongodump.
type BackupSpec struct {
	// schedule is a cron expression, for example "*/30 * * * *".
	// +kubebuilder:validation:MinLength=9
	Schedule string `json:"schedule"`

	// retain is how many backup archives to keep on the backup volume.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	Retain int32 `json:"retain,omitempty"`

	// storageSize is the size of the volume that holds the archives.
	// +kubebuilder:default="1Gi"
	// +optional
	StorageSize resource.Quantity `json:"storageSize,omitempty"`
}

// MongoReplicaSetSpec defines the desired state of MongoReplicaSet.
// +kubebuilder:validation:XValidation:rule="self.members % 2 == 1",message="members must be odd so an election always has a majority"
type MongoReplicaSetSpec struct {
	// members is the number of mongod instances in the replica set.
	// An odd number keeps a clear majority when one member is lost.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:validation:Maximum=7
	// +kubebuilder:default=3
	// +optional
	Members int32 `json:"members,omitempty"`

	// version is the MongoDB image tag, for example "8.0".
	// +kubebuilder:default="8.0"
	// +optional
	Version string `json:"version,omitempty"`

	// storageSize is the size of each member's data volume.
	// +kubebuilder:default="1Gi"
	// +optional
	StorageSize resource.Quantity `json:"storageSize,omitempty"`

	// backup enables scheduled backups. Omit it to run without backups.
	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`
}

// MemberStatus is one member as the replica set reports it.
type MemberStatus struct {
	// name is the member's host:port.
	Name string `json:"name"`
	// state is the replica set state, for example PRIMARY or SECONDARY.
	State string `json:"state"`
	// healthy is false when the other members cannot reach it.
	Healthy bool `json:"healthy"`
	// lagSeconds is how far its oplog is behind the primary.
	// +optional
	LagSeconds int64 `json:"lagSeconds,omitempty"`
}

// MongoReplicaSetStatus defines the observed state of MongoReplicaSet.
type MongoReplicaSetStatus struct {
	// observedGeneration is the spec generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is the coarse state shown by kubectl get.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// primary is the host:port of the current primary, empty when there is none.
	// +optional
	Primary string `json:"primary,omitempty"`

	// readyMembers counts members that are healthy and in PRIMARY or SECONDARY state.
	// +optional
	ReadyMembers int32 `json:"readyMembers,omitempty"`

	// members is the per-member view from replSetGetStatus.
	// +optional
	Members []MemberStatus `json:"members,omitempty"`

	// primaryChanges counts how many times the operator saw the primary change.
	// +optional
	PrimaryChanges int32 `json:"primaryChanges,omitempty"`

	// primaryLostAt is set while there is no primary, and cleared when one is elected.
	// +optional
	PrimaryLostAt *metav1.Time `json:"primaryLostAt,omitempty"`

	// lastFailoverSeconds is the last election's duration from mongo's own
	// clocks: the new primary's electionDate minus the last heartbeat received
	// from the old primary.
	// +optional
	LastFailoverSeconds string `json:"lastFailoverSeconds,omitempty"`

	// lastBackupTime is when the last backup job completed successfully.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// conditions follow the Kubernetes API conventions (Initialized, Available).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mrs
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.primary`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyMembers`
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.spec.members`
// +kubebuilder:printcolumn:name="Failovers",type=integer,JSONPath=`.status.primaryChanges`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MongoReplicaSet is the Schema for the mongoreplicasets API
type MongoReplicaSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MongoReplicaSet
	// +required
	Spec MongoReplicaSetSpec `json:"spec"`

	// status defines the observed state of MongoReplicaSet
	// +optional
	Status MongoReplicaSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MongoReplicaSetList contains a list of MongoReplicaSet
type MongoReplicaSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MongoReplicaSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MongoReplicaSet{}, &MongoReplicaSetList{})
		return nil
	})
}
