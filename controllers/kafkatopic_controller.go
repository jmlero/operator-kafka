/*
Copyright 2022.

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

package controllers

import (
	"context"
	"net"
	"strconv"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/jmlero/operator-kafka/api/v1alpha1"

	"github.com/segmentio/kafka-go"
)

const kafkatopicFinalizer = "cache.example.com/finalizer"

// KafkaTopicReconciler reconciles a KafkaTopic object
type KafkaTopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=cache.example.com,resources=kafkatopics,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.example.com,resources=kafkatopics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.example.com,resources=kafkatopics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the KafkaTopic object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *KafkaTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	log := log.FromContext(ctx)

	// Fetch the KafkaTopic instance (CR information)
	kafkaTopic := &cachev1alpha1.KafkaTopic{}
	err := r.Get(ctx, req.NamespacedName, kafkaTopic)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("CR not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get CR")
		return ctrl.Result{}, err
	}

	// examine DeletionTimestamp to determine if object is under deletion
	if kafkaTopic.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, then lets add the finalizer and update the object
		if !controllerutil.ContainsFinalizer(kafkaTopic, kafkatopicFinalizer) {
			controllerutil.AddFinalizer(kafkaTopic, kafkatopicFinalizer)
			if err := r.Update(ctx, kafkaTopic); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Create Kafka Topic
		err = createTopic(kafkaTopic)
		if err != nil {
			log.Error(err, "Error, Topic can't be created")
			return ctrl.Result{}, err
		}
		log.Info("Topic successfully created")
		if err := r.setIsRunning(ctx, kafkaTopic); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	} else {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(kafkaTopic, kafkatopicFinalizer) {
			deleteTopic(kafkaTopic)
			log.Info("Topic successfully deleted")
			// remove our finalizer from the list and update it
			controllerutil.RemoveFinalizer(kafkaTopic, kafkatopicFinalizer)
			if err := r.Update(ctx, kafkaTopic); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
}

func createTopic(kafkaTopic *cachev1alpha1.KafkaTopic) error {
	conn, _ := kafka.Dial("tcp", "localhost:9092")
	defer conn.Close()
	controller, _ := conn.Controller()

	//var controllerConn *kafka.Conn
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             kafkaTopic.Name,
			NumPartitions:     kafkaTopic.Spec.NumPartitions,
			ReplicationFactor: kafkaTopic.Spec.ReplicationFactor,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}
	return nil
}

func deleteTopic(kafkaTopic *cachev1alpha1.KafkaTopic) {
	conn, _ := kafka.Dial("tcp", "localhost:9092")
	defer conn.Close()

	controller, _ := conn.Controller()
	//var controllerConn *kafka.Conn
	controllerConn, _ := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	defer controllerConn.Close()

	err := controllerConn.DeleteTopics(kafkaTopic.Name)
	if err != nil {
		panic(err.Error())
	}

}

func (k *KafkaTopicReconciler) setIsRunning(ctx context.Context, kafkaTopic *cachev1alpha1.KafkaTopic) error {
	kafkaTopic.Status.IsRunning = true
	err := k.Status().Update(ctx, kafkaTopic)
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *KafkaTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.KafkaTopic{}).
		Complete(r)
}
