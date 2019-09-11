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

package composite

import (
	"fmt"
	"time"
	"context"

	// TODO

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrl "sigs.k8s.io/controller-runtime"
	"github.com/go-logr/logr"

	"metacontroller.app/apis/metacontroller/v1alpha1"
	"metacontroller.app/controller/common"
	"metacontroller.app/controller/common/finalizer"
	dynamiccontrollerref "metacontroller.app/dynamic/controllerref"
	k8s "metacontroller.app/third_party/kubernetes"
	dynamicobject "metacontroller.app/dynamic/object"
)

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

// TODO: move to common
func newUnstructuredFor(kind schema.GroupVersionKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(kind.GroupVersion().String())
	u.SetKind(kind.Kind)

	return u
}

type parentController struct {
	cc *v1alpha1.CompositeController

	client.Client
	log logr.Logger

	resources      apimeta.RESTMapper
	parentResource apimeta.RESTMapping

	updateStrategy updateStrategyMap

	finalizer *finalizer.Manager
	childIndex string
}

func newParentController(resources apimeta.RESTMapper, mgr ctrl.Manager, cc *v1alpha1.CompositeController) error {
	parentGVK, err := resourceToGVK(cc.Spec.ParentResource.APIVersion, cc.Spec.ParentResource.Resource, resources)
	if err != nil {
		return err
	}
	parentResource, err := resources.RESTMapping(parentGVK.GroupKind(), parentGVK.Version)
	if err != nil {
		return err
	}

	updateStrategy, err := makeUpdateStrategyMap(resources, cc)
	if err != nil {
		return err
	}

	// TODO: label selector watches ???
	/*for _, child := range cc.Spec.ChildResources {
		childInformer, err := dynInformers.Resource(child.APIVersion, child.Resource)
		if err != nil {
			return nil, fmt.Errorf("can't create informer for child resource: %v", err)
		}
		childInformers.Set(child.APIVersion, child.Resource, childInformer)
	}*/

	pc := &parentController{
		cc:             cc,
		resources:      resources,
		parentResource: *parentResource,
		Client: mgr.GetClient(), 
		updateStrategy: updateStrategy,
		finalizer: &finalizer.Manager{
			Name:    "metacontroller.app/compositecontroller-" + cc.Name,
			Enabled: cc.Spec.Hooks.Finalize != nil,
		},
	}

	// TODO: support resync period

	parent := newUnstructuredFor(parentGVK)
	bldr := ctrl.NewControllerManagedBy(mgr).For(parent)

	for _, childRes := range cc.Spec.ChildResources {
		childGVK, err := resourceToGVK(childRes.APIVersion, childRes.Resource, resources)
		if err != nil {
			return err
		}
		child := newUnstructuredFor(childGVK)
		// TODO: this watch is an "owns", falling back to orphan logic (ewwww):
		// findPotentialParents, enqueue for each parent (but not if it's being deleted)
		bldr.Watches(&source.Kind{Type: child}, &handler.EnqueueRequestsFromMapFunc{ToRequests: handler.ToRequestsFunc(func(obj handler.MapObject) []ctrl.Request {
			// TODO: not if deleted

			// see if we can use the owner ref
			ownerRef := metav1.GetControllerOf(obj.Meta)
			if ownerRef != nil {
				// we've got an owner ref, check and use that
				// TODO: do we actually want to also check the UID?
				refGV, _ := schema.ParseGroupVersion(ownerRef.APIVersion)
				if refGV.Group == parentGVK.Group && ownerRef.Kind == parentGVK.Kind {
					req := ctrl.Request{NamespacedName: client.ObjectKey{Name: ownerRef.Name}}
					if parentResource.Scope.Name() == apimeta.RESTScopeNameNamespace {
						req.Namespace = obj.Meta.GetNamespace()
					}

					return []ctrl.Request{req}
				}
			}

			// otherwise, find potential parents and enqueue those
			potentialParents := pc.findPotentialParents(obj.Object.(*unstructured.Unstructured))
			var res []ctrl.Request
			for _, potentialParent := range potentialParents {
				res = append(res, ctrl.Request{NamespacedName: client.ObjectKey{Name: potentialParent.GetName(), Namespace: potentialParent.GetNamespace()}})
			}
			return res
		})})

		// TODO: filter child resources on non-resource version changes
	}

	return bldr.Complete(pc)
}

// We used to ignore our own status updates, but we don't anymore.
// It's sometimes necessary for a hook to see its own status updates
// so they know that the status was committed to storage.
// This could cause endless sync hot-loops if your hook always returns a
// different status (e.g. you have some incrementing counter).
// Doing that is an anti-pattern anyway because status generation should be
// idempotent if nothing meaningful has actually changed in the system.

func (pc *parentController) findPotentialParents(child *unstructured.Unstructured) []*unstructured.Unstructured {
	childLabels := labels.Set(child.GetLabels())

	var parents unstructured.UnstructuredList
	parents.SetAPIVersion(pc.parentResource.GroupVersionKind.GroupVersion().String())
	parents.SetKind(pc.parentResource.GroupVersionKind.Kind+"List")

	var err error
	if pc.parentResource.Scope.Name() == apimeta.RESTScopeNameNamespace {
		// If the parent is namespaced, it must be in the same namespace as the child.
		err = pc.List(context.TODO(), &parents, client.InNamespace(child.GetNamespace()))
	} else {
		err = pc.List(context.TODO(), &parents)
	}
	if err != nil {
		return nil
	}

	// TODO: replace this with MatchingLabelsSelector one it merges
	var matchingParents []*unstructured.Unstructured
	for _, parent := range parents.Items {
		selector, err := pc.makeSelector(&parent, nil)
		if err != nil || selector.Empty() {
			continue
		}
		if selector.Matches(childLabels) {
			parent := parent // copy, don't save a ref into the slice
			matchingParents = append(matchingParents, &parent)
		}
	}
	return matchingParents
}

func (pc *parentController) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := pc.log.WithValues("parent", req)

	parent := newUnstructuredFor(pc.parentResource.GroupVersionKind)
	if err := pc.Get(ctx, req.NamespacedName, parent); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// it was *actually* deleted (not just pre-finalized), so nothing to do
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch parent")
		return ctrl.Result{}, err
	}

	// Before taking any other action, add our finalizer (if desired).
	// This ensures we have a chance to clean up after any action we later take.
	err := pc.finalizer.SyncObject(ctx, pc.Client, parent)
	if err != nil {
		// If we fail to do this, abort before doing anything else and requeue.
		log.Error(err, "unable to sync finalizer")
		return ctrl.Result{}, err
	}

	// Claim all matching child resources, including orphan/adopt as necessary.
	observedChildren, err := pc.claimChildren(ctx, parent)
	if err != nil {
		log.Error(err, "unable to claim children")
		return ctrl.Result{}, err
	}

	// Reconcile ControllerRevisions belonging to this parent.
	// Call the sync hook for each revision, then compute the overall status and
	// desired children, accounting for any rollout in progress.
	syncResult, err := pc.syncRevisions(parent, observedChildren)
	if err != nil {
		log.Error(err, "unable to sync controller revisions")
		return ctrl.Result{}, err
	}
	desiredChildren := common.MakeChildMap(parent, syncResult.Children)

	// Enqueue a delayed resync, if requested.
	var res ctrl.Result
	if syncResult.ResyncAfterSeconds > 0 {
		res.RequeueAfter = time.Duration(syncResult.ResyncAfterSeconds*float64(time.Second))
	}
	// TODO: global resync period from spec

	// If all revisions agree that they've finished finalizing,
	// remove our finalizer.
	if syncResult.Finalized {
		// TODO: this used to use client.removefinalizer, which checks for the presence of the finalizer and loops
		dynamicobject.RemoveFinalizer(parent, pc.finalizer.Name)
		err := pc.Update(ctx, parent)
		if err != nil {
			log.Error(err, "can't remove finalizer")
			return ctrl.Result{}, err
		}
	}

	// Enforce invariants between parent selector and child labels.
	selector, err := pc.makeSelector(parent, nil)
	if err != nil {
		log.Error(err, "can't make parent's selector")
		return ctrl.Result{}, err
	}
	for _, group := range desiredChildren {
		for _, obj := range group {
			// We don't use GetLabels() because that swallows conversion errors.
			objLabels, _, err := unstructured.NestedStringMap(obj.UnstructuredContent(), "metadata", "labels")
			if err != nil {
				log.Error(err, "invalid labels on desired child", "child kind", obj.GetKind(), "child", client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()})
				return ctrl.Result{}, err
			}
			// If selector generation is enabled, add the controller-uid label to all
			// desired children so they match the generated selector.
			if pc.cc.Spec.GenerateSelector != nil && *pc.cc.Spec.GenerateSelector {
				if objLabels == nil {
					objLabels = make(map[string]string, 1)
				}
				if _, ok := objLabels["controller-uid"]; !ok {
					objLabels["controller-uid"] = string(parent.GetUID())
					obj.SetLabels(objLabels)
				}
			}
			// Make sure all desired children match the parent's selector.
			// We consider it user error to try to create children that would be
			// immediately orphaned.
			if !selector.Matches(labels.Set(objLabels)) {
				log.Error(err, "labels on desired child don't match parent selector", "child kind", obj.GetKind(), "child", client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()})
				return ctrl.Result{}, fmt.Errorf("labels on desired child %v %v/%v don't match parent selector", obj.GetKind(), obj.GetNamespace(), obj.GetName())
			}
		}
	}

	// Reconcile child objects belonging to this parent.
	// Remember manage error, but continue to update status regardless.
	//
	// We only manage children if the parent is "alive" (not pending deletion),
	// or if it's pending deletion and we have a `finalize` hook.
	var manageErr error
	if parent.GetDeletionTimestamp() == nil || pc.finalizer.ShouldFinalize(parent) {
		// Reconcile children.
		if err := common.ManageChildren(ctx, pc.Client, pc.updateStrategy, parent, observedChildren, desiredChildren); err != nil {
			manageErr = fmt.Errorf("can't reconcile children for %v %v/%v: %v", pc.parentResource.GroupVersionKind.Kind, parent.GetNamespace(), parent.GetName(), err)
		}
	}

	// Update parent status.
	// We'll want to make sure this happens after manageChildren once we support observedGeneration.
	if err := pc.updateParentStatus(ctx, parent, syncResult.Status); err != nil {
		return ctrl.Result{}, fmt.Errorf("can't update status for %v %v/%v: %v", pc.parentResource.GroupVersionKind, parent.GetNamespace(), parent.GetName(), err)
	}

	return res, manageErr
}

func (pc *parentController) makeSelector(parent *unstructured.Unstructured, extraMatchLabels map[string]string) (labels.Selector, error) {
	labelSelector := &metav1.LabelSelector{}

	if pc.cc.Spec.GenerateSelector != nil && *pc.cc.Spec.GenerateSelector {
		// Select by controller-uid, like Job does.
		// Any selector on the parent is ignored in this case.
		labelSelector = metav1.AddLabelToSelector(labelSelector, "controller-uid", string(parent.GetUID()))
	} else {
		// Get the parent's LabelSelector.
		if err := k8s.GetNestedFieldInto(labelSelector, parent.UnstructuredContent(), "spec", "selector"); err != nil {
			return nil, fmt.Errorf("can't get label selector from %v %v/%v", pc.parentResource.GroupVersionKind, parent.GetNamespace(), parent.GetName())
		}
		// An empty selector doesn't make sense for a CompositeController parent.
		// This is likely user error, and could be dangerous (selecting everything).
		if len(labelSelector.MatchLabels) == 0 && len(labelSelector.MatchExpressions) == 0 {
			return nil, fmt.Errorf(".spec.selector must have either matchLabels, matchExpressions, or both")
		}
	}

	for key, value := range extraMatchLabels {
		labelSelector = metav1.AddLabelToSelector(labelSelector, key, value)
	}

	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, fmt.Errorf("can't convert label selector (%#v): %v", labelSelector, err)
	}
	return selector, nil
}

func (pc *parentController) canAdoptFunc(parent *unstructured.Unstructured) func() error {
	var fresh unstructured.Unstructured
	fresh.SetAPIVersion(parent.GetAPIVersion())
	fresh.SetKind(parent.GetKind())
	parentKey := client.ObjectKey{Name: parent.GetName(), Namespace: parent.GetNamespace()}

	return k8s.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		// Make sure this is always an uncached read.
		err := pc.Get(context.TODO(), parentKey, &fresh)
		if err != nil {
			return nil, err
		}
		if fresh.GetUID() != parent.GetUID() {
			return nil, fmt.Errorf("original %v %v/%v is gone: got uid %v, wanted %v", pc.parentResource.GroupVersionKind, parent.GetNamespace(), parent.GetName(), fresh.GetUID(), parent.GetUID())
		}
		return &fresh, nil
	})
}

func (pc *parentController) claimChildren(ctx context.Context, parent *unstructured.Unstructured) (common.ChildMap, error) {
	// Set up values common to all child types.
	parentNamespace := parent.GetNamespace()
	parentGVK := pc.parentResource.GroupVersionKind
	selector, err := pc.makeSelector(parent, nil)
	if err != nil {
		return nil, err
	}
	canAdoptFunc := pc.canAdoptFunc(parent)

	// Claim all child types.
	childMap := make(common.ChildMap)
	for _, child := range pc.cc.Spec.ChildResources {
		// List all objects of the child kind in the parent object's namespace,
		// or in all namespaces if the parent is cluster-scoped.

		gvk, err := resourceToGVK(child.APIVersion, child.Resource, pc.resources)
		if err != nil {
			return nil, err
		}

		// TODO: need to set the gvk here
		var all unstructured.UnstructuredList
		all.SetAPIVersion(child.APIVersion)
		all.SetKind(gvk.Kind+"List")

		if pc.parentResource.Scope.Name() == apimeta.RESTScopeNameNamespace {
			err = pc.List(ctx, &all, client.InNamespace(parentNamespace))
		} else {
			err = pc.List(ctx, &all)
		}
		if err != nil {
			return nil, fmt.Errorf("can't list %v children: %v", gvk.Kind, err)
		}

		// Always include the requested groups, even if there are no entries.
		childMap.InitGroup(child.APIVersion, gvk.Kind)

		// Handle orphan/adopt and filter by owner+selector.
		crm := dynamiccontrollerref.NewUnstructuredManager(pc.Client, parent, selector, parentGVK, gvk, canAdoptFunc)
		children, err := crm.ClaimChildren(all.Items)
		if err != nil {
			return nil, fmt.Errorf("can't claim %v children: %v", gvk.Kind, err)
		}

		// Add children to map by name.
		// Note that we limit each parent to only working within its own namespace.
		for _, obj := range children {
			childMap.Insert(parent, obj)
		}
	}
	return childMap, nil
}

func (pc *parentController) updateParentStatus(ctx context.Context, parent *unstructured.Unstructured, status map[string]interface{}) error {
	// Inject ObservedGeneration before comparing with old status,
	// so we're comparing against the final form we desire.
	if status == nil {
		status = make(map[string]interface{})
	}
	status["observedGeneration"] = parent.GetGeneration()

	// TODO: this used to loop until we set the status succesfully, *checking the UID matched first*
	// TODO: this used to no-op if the status was equal
	// TODO: this used to work for resources w/o status subresource support
	// TODO: this used to check that the UID matched and whatnot

	// Overwrite .status field of parent object without touching other parts.
	// We can't use Patch() because we need to ensure that the UID matches.
	parent.UnstructuredContent()["status"] = status
	return pc.Status().Update(ctx, parent)
}
