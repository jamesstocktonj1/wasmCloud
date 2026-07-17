// Package api exposes WorkloadDeployment management and host visibility over
// plain HTTP/JSON — the surface a caller uses instead of `kubectl apply`ing
// a WorkloadDeployment CRD.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"go.wasmcloud.dev/runtime-apiserver/internal/hostregistry"
	"go.wasmcloud.dev/runtime-apiserver/internal/reconciler"
	"go.wasmcloud.dev/runtime-apiserver/internal/store"
)

// Deps are the dependencies the HTTP handlers operate on.
type Deps struct {
	Store      *store.Store
	Hosts      *hostregistry.Registry
	Reconciler *reconciler.Reconciler
	// Ready reports whether the server is ready to serve, e.g. NATS is
	// currently connected. Nil means always ready.
	Ready func() bool
}

type server struct {
	deps Deps
	log  *slog.Logger
}

// NewHandler builds the apiserver's HTTP handler.
func NewHandler(deps Deps, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	s := &server{deps: deps, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	mux.HandleFunc("GET /v1/hosts", s.handleListHosts)
	mux.HandleFunc("GET /v1/hosts/{id}", s.handleGetHost)

	mux.HandleFunc("POST /v1/workloaddeployments", s.handleCreateWorkloadDeployment)
	mux.HandleFunc("GET /v1/workloaddeployments", s.handleListWorkloadDeployments)
	mux.HandleFunc("GET /v1/workloaddeployments/{name}", s.handleGetWorkloadDeployment)
	mux.HandleFunc("PUT /v1/workloaddeployments/{name}", s.handleUpdateWorkloadDeployment)
	mux.HandleFunc("DELETE /v1/workloaddeployments/{name}", s.handleDeleteWorkloadDeployment)

	return withLogging(log, mux)
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.deps.Ready != nil && !s.deps.Ready() {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("not ready"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// listResponse wraps every list endpoint's body in a stable envelope so
// pagination metadata has somewhere to live later without a breaking change.
type listResponse[T any] struct {
	Items []T `json:"items"`
}

type errorBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorBody{Error: err.Error()})
}

func decodeJSON(r *http.Request, into any) error {
	defer func() { _ = r.Body.Close() }()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
