package api

import (
	"errors"
	"net/http"

	"go.wasmcloud.dev/runtime-apiserver/internal/store"
	"go.wasmcloud.dev/runtime-apiserver/internal/types"
)

type createWorkloadDeploymentRequest struct {
	Name        string                       `json:"name"`
	Labels      map[string]string            `json:"labels,omitempty"`
	Annotations map[string]string            `json:"annotations,omitempty"`
	Spec        types.WorkloadDeploymentSpec `json:"spec"`
}

type updateWorkloadDeploymentRequest struct {
	Labels      map[string]string            `json:"labels,omitempty"`
	Annotations map[string]string            `json:"annotations,omitempty"`
	Spec        types.WorkloadDeploymentSpec `json:"spec"`
}

func (s *server) handleCreateWorkloadDeployment(w http.ResponseWriter, r *http.Request) {
	var req createWorkloadDeploymentRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	obj, err := s.deps.Store.Create(req.Name, req.Labels, req.Annotations, req.Spec)
	if err != nil {
		writeError(w, statusForStoreErr(err), err)
		return
	}

	s.deps.Reconciler.Nudge()
	writeJSON(w, http.StatusCreated, obj)
}

func (s *server) handleListWorkloadDeployments(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, listResponse[types.WorkloadDeployment]{Items: s.deps.Store.List()})
}

func (s *server) handleGetWorkloadDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	obj, err := s.deps.Store.Get(name)
	if err != nil {
		writeError(w, statusForStoreErr(err), err)
		return
	}
	writeJSON(w, http.StatusOK, obj)
}

func (s *server) handleUpdateWorkloadDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var req updateWorkloadDeploymentRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	obj, err := s.deps.Store.UpdateSpec(name, req.Labels, req.Annotations, req.Spec)
	if err != nil {
		writeError(w, statusForStoreErr(err), err)
		return
	}

	s.deps.Reconciler.Nudge()
	writeJSON(w, http.StatusOK, obj)
}

func (s *server) handleDeleteWorkloadDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	if err := s.deps.Store.MarkDeleting(name); err != nil {
		writeError(w, statusForStoreErr(err), err)
		return
	}

	s.deps.Reconciler.Nudge()
	w.WriteHeader(http.StatusAccepted)
}

func statusForStoreErr(err error) int {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, store.ErrAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, store.ErrDeleting):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}
