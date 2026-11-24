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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1alpha1 "github.com/pedrincandido/mongo-replicaset-operator/api/v1alpha1"
)

var _ = Describe("MongoReplicaSet Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		mongoreplicaset := &dbv1alpha1.MongoReplicaSet{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind MongoReplicaSet")
			err := k8sClient.Get(ctx, typeNamespacedName, mongoreplicaset)
			if err != nil && errors.IsNotFound(err) {
				resource := &dbv1alpha1.MongoReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &dbv1alpha1.MongoReplicaSet{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance MongoReplicaSet")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &MongoReplicaSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("creating a headless Service that publishes not-ready members")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())

			By("creating a StatefulSet with the default 3 members and 1Gi volumes")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))
			size := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(size.String()).To(Equal("1Gi"))
			Expect(sts.Spec.Template.Spec.Containers[0].Command).To(ContainElements("--replSet", resourceName))

			By("protecting a majority with a PodDisruptionBudget")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, pdb)).To(Succeed())
			Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(2))

			By("reporting Pending while no member is running")
			got := &dbv1alpha1.MongoReplicaSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(dbv1alpha1.PhasePending))
			Expect(got.Status.ObservedGeneration).To(Equal(got.Generation))
		})

		It("rejects an even member count", func() {
			even := &dbv1alpha1.MongoReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: "even", Namespace: resourceNamespace},
				Spec:       dbv1alpha1.MongoReplicaSetSpec{Members: 4},
			}
			err := k8sClient.Create(ctx, even)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("members must be odd"))
		})
	})
})
