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
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"github.com/go-logr/logr"

	"metacontroller.app/apis/metacontroller/v1alpha1"
)

type Metacontroller struct {
	client.Client
	log logr.Logger

	decoratorControllers map[string]*decoratorController

	mgr ctrl.Manager
}

func (mc *Metacontroller) SetupWithManager(mgr ctrl.Manager) error {
	mc.decoratorControllers = make(map[string]*decoratorController)
	mc.mgr = mgr

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DecoratorController{}).
		Complete(mc)
}

// TODO: stop all child controllers

func (mc *Metacontroller) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := mc.log.WithValues("decoratorcontroller", req)

	// load the controller, and stop it if it was deleted, and is still running
	var contCfg v1alpha1.DecoratorController
	if err := mc.Get(ctx, req.NamespacedName, &contCfg); err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.V(1).Info("decorator controller has been deleted")
			if cont, ok := mc.decoratorControllers[req.Name]; ok {
				cont.Stop()
				delete(mc.decoratorControllers, req.Name)
			}
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch controller")
		return ctrl.Result{}, err
	}

	// check if we need to stop the controller to recreate it because it changed configuration
	if cont, alreadyStarted := mc.decoratorControllers[req.Name]; alreadyStarted {
		if apiequality.Semantic.DeepEqual(contCfg.Spec, cont.dc.Spec) {
			// nothing changed
			return ctrl.Result{}, nil
		}

		// TODO: we've got no good way to stop these but still share caches

		// otherwise, stop the controller and recreate it with the new settings
		cont.Stop()
		delete(mc.decoratorControllers, req.Name)
	}

	// (re)create the controller
	err := newDecoratorControllers(mc.mgr, contCfg)
	if err != nil {
		log.Error(err, "unable to create the controller from the config")
		return ctrl.Result{}, err
	}
	mc.decoratorControllers[req.Name] = conts
	return ctrl.Result{}, nil
}
