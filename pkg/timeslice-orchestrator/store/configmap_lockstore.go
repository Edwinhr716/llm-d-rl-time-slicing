package store

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	// Namespace is the default namespace where the locks configmap resides.
	Namespace = "timeslice-system"
	// ConfigMapName is the default name of the configmap storing the locks.
	ConfigMapName = "timeslice-orchestrator-locks"
)

// ConfigMapLockStore implements LockStore using a Kubernetes ConfigMap.
type ConfigMapLockStore struct {
	client    kubernetes.Interface
	namespace string
	name      string
}

// ConfigMapLockStoreOption configures a ConfigMapLockStore.
type ConfigMapLockStoreOption func(*ConfigMapLockStore)

// WithConfigMap stores the locks in the ConfigMap namespace/name instead of
// the default Namespace/ConfigMapName, so that two orchestrator installs in
// one cluster do not share (and fight over) one lock table. An empty value
// keeps the corresponding default.
func WithConfigMap(namespace, name string) ConfigMapLockStoreOption {
	return func(s *ConfigMapLockStore) {
		if namespace != "" {
			s.namespace = namespace
		}
		if name != "" {
			s.name = name
		}
	}
}

// NewConfigMapLockStore creates a new ConfigMapLockStore. Without options it
// uses the ConfigMap Namespace/ConfigMapName.
func NewConfigMapLockStore(client kubernetes.Interface, opts ...ConfigMapLockStoreOption) *ConfigMapLockStore {
	s := &ConfigMapLockStore{
		client:    client,
		namespace: Namespace,
		name:      ConfigMapName,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ConfigMapRef returns "namespace/name" of the ConfigMap holding the locks.
func (s *ConfigMapLockStore) ConfigMapRef() string {
	return s.namespace + "/" + s.name
}

func (s *ConfigMapLockStore) getOrCreateConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	var cm *corev1.ConfigMap

	err := retry.OnError(retry.DefaultRetry, apierrors.IsAlreadyExists, func() error {
		var getErr error
		cm, getErr = s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if getErr == nil {
			return nil
		}
		if !apierrors.IsNotFound(getErr) {
			return getErr
		}

		// Create it
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      s.name,
				Namespace: s.namespace,
			},
			Data: make(map[string]string),
		}
		var createErr error
		cm, createErr = s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, newCM, metav1.CreateOptions{})
		return createErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get or create configmap after %d attempts: %w", retry.DefaultRetry.Steps, err)
	}
	return cm, nil
}

// GetLock returns the job_id currently holding the lock for the group.
func (s *ConfigMapLockStore) GetLock(ctx context.Context, groupID string) (string, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil // ConfigMap doesn't exist, so no locks
		}
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}
	if cm.Data == nil {
		return "", nil
	}
	return cm.Data[groupID], nil
}

// Lock persistently sets the job_id holding the lock for the group.
func (s *ConfigMapLockStore) Lock(ctx context.Context, groupID, jobID string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, err := s.getOrCreateConfigMap(ctx)
		if err != nil {
			return err
		}

		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}

		current := cm.Data[groupID]
		if current != "" && current != jobID {
			return ErrAlreadyLocked
		}

		cm.Data[groupID] = jobID
		_, err = s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

// Unlock persistently releases the lock for the group.
func (s *ConfigMapLockStore) Unlock(ctx context.Context, groupID, jobID string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil // ConfigMap doesn't exist, so already unlocked
			}
			return fmt.Errorf("failed to get configmap: %w", err)
		}

		if cm.Data == nil {
			return nil // No data, so already unlocked
		}

		current := cm.Data[groupID]
		if current == "" {
			return nil // already unlocked
		}
		if current != jobID {
			return ErrNotLockHolder
		}

		delete(cm.Data, groupID)
		_, err = s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}
