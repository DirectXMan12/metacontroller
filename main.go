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

package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats/view"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

var (
	debugAddr         = flag.String("debug-addr", ":9999", "The address to bind the debug http endpoints")
	// TODO: client-config-path is different from --kubeconfig
	informerRelist    = flag.Duration("cache-flush-interval", 30*time.Minute, "How often to flush local caches and relist objects from the API server")
	// TODO: ^ we shouldn't do this
)

func main() {
	flag.Parse()
	ctrl.SetLogger(zap.Logger(true))

	setupLog := ctrl.Log.WithName("setup")

	setupLog.Info("debug http server address", "address", *debugAddr)

	config := ctrl.GetConfigOrDie()

	// TODO: opencensus support
	// TODO: relisting discovery client
	mgr := ctrl.NewManager(config, ctrl.Options{
		RelistInterval: informerRelist,
	})

	exporter, err := prometheus.NewExporter(prometheus.Options{})
	if err != nil {
		setupLog.Error(err, "unable to set up prometheus exporter")
		os.Exit(1)
	}
	view.RegisterExporter(exporter)

	mux := http.NewServeMux()
	mux.Handle("/metrics", exporter)
	srv := &http.Server{
		Addr:    *debugAddr,
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			setupLog.Error(err, "unable to serve debug endpoint")
		}
	}()

	// On SIGTERM, stop all controllers gracefully.
	stopChan := ctrl.SetupSignalHandlers()

	<-sigchan
	setupLog.Info("Received stop signal. Shutting down...")

	stopServer()
	srv.Shutdown(context.Background())
}
