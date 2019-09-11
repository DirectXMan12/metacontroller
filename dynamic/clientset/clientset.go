/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clientset

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"

	dynamicdiscovery "metacontroller.app/dynamic/discovery"
	dynamicobject "metacontroller.app/dynamic/object"
)

// TODO: rebase these on the controller runtime client

// AtomicUpdate performs an atomic read-modify-write loop, retrying on
// optimistic concurrency conflicts.
//
// It only uses the identity (name/namespace/uid) of the provided 'orig' object,
// not the contents. The object passed to the update() func will be from a live
// GET against the API server.
//
// This should only be used for unconditional writes, as in, "I want to make
// this change right now regardless of what else may have changed since I last
// read the object."
//
// The update() func should modify the passed object and return true to go ahead
// with the update, or false if no update is required.
func (rc *ResourceClient) AtomicUpdate(orig *unstructured.Unstructured, update func(obj *unstructured.Unstructured) bool) (result *unstructured.Unstructured, err error) {
	name := orig.GetName()

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := rc.Get(name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.GetUID() != orig.GetUID() {
			// The original object was deleted and replaced with a new one.
			return apierrors.NewNotFound(rc.GroupResource(), name)
		}
		if changed := update(current); !changed {
			// There's nothing to do.
			result = current
			return nil
		}
		result, err = rc.Update(current)
		return err
	})
	return result, err
}

// AddFinalizer adds the given finalizer to the list, if it isn't there already.
func (rc *ResourceClient) AddFinalizer(orig *unstructured.Unstructured, name string) (*unstructured.Unstructured, error) {
	return rc.AtomicUpdate(orig, func(obj *unstructured.Unstructured) bool {
		if dynamicobject.HasFinalizer(obj, name) {
			// Nothing to do. Abort update.
			return false
		}
		dynamicobject.AddFinalizer(obj, name)
		return true
	})
}

// RemoveFinalizer removes the given finalizer from the list, if it's there.
func (rc *ResourceClient) RemoveFinalizer(orig *unstructured.Unstructured, name string) (*unstructured.Unstructured, error) {
	return rc.AtomicUpdate(orig, func(obj *unstructured.Unstructured) bool {
		if !dynamicobject.HasFinalizer(obj, name) {
			// Nothing to do. Abort update.
			return false
		}
		dynamicobject.RemoveFinalizer(obj, name)
		return true
	})
}

// AtomicStatusUpdate is similar to AtomicUpdate, except that it updates status.
func (rc *ResourceClient) AtomicStatusUpdate(orig *unstructured.Unstructured, update func(obj *unstructured.Unstructured) bool) (result *unstructured.Unstructured, err error) {
	name := orig.GetName()

	// We should call GetStatus (if it HasSubresource) to respect subresource
	// RBAC rules, but the dynamic client does not support this yet.
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := rc.Get(name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.GetUID() != orig.GetUID() {
			// The original object was deleted and replaced with a new one.
			return apierrors.NewNotFound(rc.GroupResource(), name)
		}
		if changed := update(current); !changed {
			// There's nothing to do.
			result = current
			return nil
		}

		if rc.HasSubresource("status") {
			result, err = rc.UpdateStatus(current)
		} else {
			result, err = rc.Update(current)
		}
		return err
	})
	return result, err
}
