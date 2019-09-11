/*
Copyright 2018 Google Inc.

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

package decorator

import (
	"fmt"
	"reflect"
	"time"
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"github.com/go-logr/logr"

	"metacontroller.app/apis/metacontroller/v1alpha1"
	"metacontroller.app/controller/common"
	"metacontroller.app/controller/common/finalizer"
	dynamicobject "metacontroller.app/dynamic/object"
)

const (
	decoratorControllerAnnotation = "metacontroller.k8s.io/decorator-controller"
)

type apiRes struct {
	APIVersion string
	Kind string
}

type decoratorController struct {
	dc *v1alpha1.DecoratorController

	client.Client
	log logr.Logger

	resources apimeta.RESTMapper

	parentKind     apiRes
	parentSelector *decoratorSelector

	updateStrategy updateStrategyMap

	finalizer *finalizer.Manager
	childIndex string
}

// TODO: move to common
func resourceToGVK(apiVer, res string, mapper apimeta.RESTMapper) (schema.GroupVersionKind, error) {
	groupVer, err := schema.ParseGroupVersion(apiVer)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("can't parse API version", apiVer)
	}
	gvr := groupVer.WithResource(res)
	gvk, err := mapper.KindFor(gvr)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("unable to find GVK for group-version-resource %q", gvr)
	}

	return gvk, nil
}

func newDecoratorControllers(mgr ctrl.Manager, dc *v1alpha1.DecoratorController, mapper apimeta.RESTMapper) error {
	for _, parentRes := range dc.Spec.Resources {
		// TODO: make stoppable

		gvk, err := resourceToGVK(parentRes.APIVersion, parentRes.Resource, mapper)
		if err != nil {
			return fmt.Errorf("unable to find GVK for group-version-resource %q %q", parentRes.APIVersion, parentRes.Resource)
		}

		c := &decoratorController{
			resources: mapper,
			dc:              dc,
			parentKind: apiRes{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind},
			finalizer: &finalizer.Manager{
				Name:    "metacontroller.app/decoratorcontroller-" + dc.Name,
				Enabled: dc.Spec.Hooks.Finalize != nil,
			},
		}

		c.parentSelector, err = newDecoratorSelector(mapper, dc)
		if err != nil {
			return err
		}

		// Remember the update strategy for each child type.
		c.updateStrategy, err = makeUpdateStrategyMap(mapper, dc)
		if err != nil {
			return err
		}

		parent := newUnstructuredFor(c.parentKind)
		bldr := ctrl.NewControllerManagedBy(mgr).For(parent)

		for _, childRes := range dc.Spec.Attachments {
			groupVer, err := schema.ParseGroupVersion(childRes.APIVersion)
			if err != nil {
				return fmt.Errorf("can't parse API version", childRes.APIVersion)
			}
			gvr := groupVer.WithResource(childRes.Resource)
			gvk, err := mapper.KindFor(gvr)
			if err != nil {
				return fmt.Errorf("unable to find GVK for group-version-resource %q", gvr)
			}
			child := newUnstructuredFor(apiRes{APIVersion: childRes.APIVersion, Kind: gvk.Kind})
			bldr = bldr.Owns(child)
		}

		if err := bldr.Complete(c); err != nil {
			return err
		}
	}

	return nil
}

// TODO: a way to unregister watches (gc?)
// we might not need to do this anyway if we're doing sidecar form

func newUnstructuredFor(kind apiRes) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(kind.APIVersion)
	u.SetKind(kind.Kind)

	return u
}

func (c *decoratorController) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := c.log.WithValues("parent", req)

	parent := newUnstructuredFor(c.parentKind)
	if err := c.Get(ctx, req.NamespacedName, parent); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// it was *actually* deleted (not just pre-finalized), so nothing to do
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch parent")
		return ctrl.Result{}, err
	}

	// make sure we care about this
	if !c.parentSelector.Matches(parent) && !dynamicobject.HasFinalizer(parent, c.finalizer.Name) {
		return ctrl.Result{}, nil
	}

	// Before taking any other action, add our finalizer (if desired).
	// This ensures we have a chance to clean up after any action we later take.
	err := c.finalizer.SyncObject(ctx, c.Client, parent)
	if err != nil {
		log.Error(err, "unable to sync finalizer for parent")
		return ctrl.Result{}, err
	}

	// Check the finalizer again in case we just removed it.
	if !c.parentSelector.Matches(parent) && !dynamicobject.HasFinalizer(parent, c.finalizer.Name) {
		return ctrl.Result{}, nil
	}

	// List all children belonging to this parent, of the kinds we care about.
	// This only lists the children we created. Existing children are ignored.
	observedChildren, err := c.getChildren(ctx, parent)
	if err != nil {
		log.Error(err, "unable to get child objects")
		return ctrl.Result{}, err
	}

	// Call the sync hook to get the desired annotations and children.
	syncRequest := &SyncHookRequest{
		Controller:  c.dc,
		Object:      parent,
		Attachments: observedChildren,
	}
	syncResult, err := c.callSyncHook(syncRequest)
	if err != nil {
		log.Error(err, "unable to call sync hook")
		return ctrl.Result{}, err
	}

	desiredChildren := common.MakeChildMap(parent, syncResult.Attachments)

	var res ctrl.Result
	if syncResult.ResyncAfterSeconds > 0 {
		res.RequeueAfter = time.Duration(syncResult.ResyncAfterSeconds*float64(time.Second))
	}

	// Set desired labels and annotations on parent.
	// Also remove finalizer if requested.
	parentLabels := parent.GetLabels()
	if parentLabels == nil {
		parentLabels = make(map[string]string)
	}
	parentAnnotations := parent.GetAnnotations()
	if parentAnnotations == nil {
		parentAnnotations = make(map[string]string)
	}

	parentStatus, _, _ := unstructured.NestedMap(parent.Object, "status")
	if syncResult.Status == nil {
		// assume nil status means don't update the status
		syncResult.Status = parentStatus
	}

	labelsChanged := updateStringMap(parentLabels, syncResult.Labels)
	annotationsChanged := updateStringMap(parentAnnotations, syncResult.Annotations)
	statusChanged := !reflect.DeepEqual(parentStatus, syncResult.Status)

	// Only do the update if something changed.
	if labelsChanged || annotationsChanged || statusChanged ||
		(syncResult.Finalized && dynamicobject.HasFinalizer(parent, c.finalizer.Name)) {
		parent.SetLabels(parentLabels)
		parent.SetAnnotations(parentAnnotations)

		// TODO: care about error?
		unstructured.SetNestedField(parent.Object, syncResult.Status, "status")

		// TODO: check if has status subresource here
		if statusChanged {
			// The regular Update below will ignore changes to .status so we do it separately.

			// don't mutate the input object so we can update the finalizer below.
			// TODO: don't have to do this ^
			parentCpy := parent.DeepCopy()
			if err := c.Status().Update(ctx, parentCpy); err != nil {
				log.Error(err, "unable to update parent status")
				return ctrl.Result{}, err
			}

			// The Update below needs to use the latest ResourceVersion.
			parent.SetResourceVersion(parentCpy.GetResourceVersion())
		}

		if syncResult.Finalized {
			dynamicobject.RemoveFinalizer(parent, c.finalizer.Name)
		}

		log.V(1).Info("updating parent")
		if err := c.Update(ctx, parent); err != nil {
			log.Error(err, "unable to update parent")
			return ctrl.Result{}, err
		}
	}

	// Add an annotation to all desired children to remember that they were
	// created by this decorator.
	for _, group := range desiredChildren {
		for _, child := range group {
			ann := child.GetAnnotations()
			if ann[decoratorControllerAnnotation] == c.dc.Name {
				continue
			}
			if ann == nil {
				ann = make(map[string]string)
			}
			ann[decoratorControllerAnnotation] = c.dc.Name
			child.SetAnnotations(ann)
		}
	}

	// Reconcile child objects belonging to this parent.
	// Remember manage error, but continue to update status regardless.
	//
	// We only manage children if the parent is "alive" (not pending deletion),
	// or if it's pending deletion and we have a `finalize` hook.
	var manageErr error
	if parent.GetDeletionTimestamp() == nil || c.finalizer.ShouldFinalize(parent) {
		// Reconcile children.
		if err := common.ManageChildren(ctx, c.Client, c.updateStrategy, parent, observedChildren, desiredChildren); err != nil {
			manageErr = fmt.Errorf("can't reconcile children for %v %v/%v: %v", parent.GetKind(), parent.GetNamespace(), parent.GetName(), err)
		}
	}

	return res, manageErr
}

func (c *decoratorController) getChildren(ctx context.Context, parent *unstructured.Unstructured) (common.ChildMap, error) {
	parentNamespace := parent.GetNamespace()
	childMap := make(common.ChildMap)

	for _, child := range c.dc.Spec.Attachments {
		// List all objects of the child kind in the parent object's namespace,
		// or in all namespaces if the parent is cluster-scoped.

		gvk, err := resourceToGVK(child.APIVersion, child.Resource, c.resources)
		if err != nil {
			return nil, fmt.Errorf("can't find resource %q in apiVersion %q: %v", child.Resource, child.APIVersion, err)
		}

		// TODO: need to set the GVK
		var all unstructured.UnstructuredList
		all.SetAPIVersion(child.APIVersion)
		all.SetKind(gvk.Kind+"List")
		if parentNamespace != "" {
			// childIndex is "belong to parent for this decorator (via decorator annotation)
			err = c.List(ctx, &all, client.InNamespace(parentNamespace), client.MatchingField(c.childIndex, parent.GetName()))
		} else {
			err = c.List(ctx, &all, client.MatchingField(c.childIndex, parent.GetName()))
		}
		if err != nil {
			return nil, fmt.Errorf("can't list children for resource %q in apiVersion %q: %v", child.Resource, child.APIVersion, err)
		}

		childMap.InitGroup(child.APIVersion, gvk.Kind)

		// Take only the objects that belong to this parent,
		// and that were created by this decorator.
		for _, obj := range all.Items {
			childMap.Insert(parent, &obj)
		}
	}
	return childMap, nil
}

type updateStrategyMap map[string]*v1alpha1.DecoratorControllerAttachmentUpdateStrategy

func (m updateStrategyMap) GetMethod(apiGroup, kind string) v1alpha1.ChildUpdateMethod {
	strategy := m.get(apiGroup, kind)
	if strategy == nil || strategy.Method == "" {
		return v1alpha1.ChildUpdateOnDelete
	}
	return strategy.Method
}

func (m updateStrategyMap) get(apiGroup, kind string) *v1alpha1.DecoratorControllerAttachmentUpdateStrategy {
	return m[updateStrategyMapKey(apiGroup, kind)]
}

func updateStrategyMapKey(apiGroup, kind string) string {
	return fmt.Sprintf("%s.%s", kind, apiGroup)
}

func makeUpdateStrategyMap(resources apimeta.RESTMapper, dc *v1alpha1.DecoratorController) (updateStrategyMap, error) {
	m := make(updateStrategyMap)
	for _, child := range dc.Spec.Attachments {
		if child.UpdateStrategy != nil && child.UpdateStrategy.Method != v1alpha1.ChildUpdateOnDelete {
			// Map resource name to kind name.
			gvk, err := resourceToGVK(child.APIVersion, child.Resource, resources)
			if err != nil {
				return nil, fmt.Errorf("can't find child resource %q in %v: %v", child.Resource, child.APIVersion, err)
			}
			// Ignore API version.
			apiGroup, _ := common.ParseAPIVersion(child.APIVersion)
			key := updateStrategyMapKey(apiGroup, gvk.Kind)
			m[key] = child.UpdateStrategy
		}
	}
	return m, nil
}

func updateStringMap(dest map[string]string, updates map[string]*string) (changed bool) {
	for k, v := range updates {
		if v == nil {
			// nil/null means delete the key
			if _, exists := dest[k]; exists {
				delete(dest, k)
				changed = true
			}
			continue
		}
		// Add/Update the key.
		oldValue, exists := dest[k]
		if !exists || oldValue != *v {
			dest[k] = *v
			changed = true
		}
	}
	return changed
}
