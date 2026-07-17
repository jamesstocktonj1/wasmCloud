// Package store provides a thread-safe, in-memory WorkloadDeployment store.
//
// There is deliberately no persistence layer: the apiserver is meant for
// running wash-hosts without standing up Kubernetes/etcd, and process
// restarts are expected to be rare in that setting. If a durable store is
// needed later, this is the seam to add one behind.
package store

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"go.wasmcloud.dev/runtime-apiserver/internal/types"
)

// ErrNotFound is returned when a named WorkloadDeployment doesn't exist.
var ErrNotFound = fmt.Errorf("workload deployment not found")

// ErrAlreadyExists is returned by Create when the name is already in use.
var ErrAlreadyExists = fmt.Errorf("workload deployment already exists")

// ErrDeleting is returned when mutating a WorkloadDeployment that is
// currently draining after a Delete call.
var ErrDeleting = fmt.Errorf("workload deployment is being deleted")

// record is the store's internal bookkeeping around a WorkloadDeployment.
// Fields here are never serialized to API responses.
type record struct {
	obj types.WorkloadDeployment

	// templateHash is the hash of the last spec.Template the reconciler
	// acted on; the reconciler recreates instances when this drifts from
	// the spec's current hash (a "Recreate" deploy policy).
	templateHash string
	// nextSlot is a monotonically increasing counter used to mint unique
	// instance slot IDs, so a scale-down followed by a scale-up (or a
	// recreate) never reuses a workload_id that a host might still be
	// tearing down.
	nextSlot int
	// deleting marks a deployment as draining: the reconciler scales it to
	// zero and then removes the record.
	deleting bool
}

// Store is a thread-safe, in-memory collection of WorkloadDeployments.
type Store struct {
	mu      sync.RWMutex
	records map[string]*record
}

// New returns an empty Store.
func New() *Store {
	return &Store{records: make(map[string]*record)}
}

// Create adds a new WorkloadDeployment. The Name must be unique and pass
// types.ValidateName; Spec must pass Spec.Validate.
func (s *Store) Create(name string, labels, annotations map[string]string, spec types.WorkloadDeploymentSpec) (types.WorkloadDeployment, error) {
	if err := types.ValidateName(name); err != nil {
		return types.WorkloadDeployment{}, err
	}
	if err := spec.Validate(); err != nil {
		return types.WorkloadDeployment{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.records[name]; ok {
		return types.WorkloadDeployment{}, ErrAlreadyExists
	}

	now := time.Now().UTC()
	obj := types.WorkloadDeployment{
		UID:         uuid.NewString(),
		Name:        name,
		Labels:      cloneMap(labels),
		Annotations: cloneMap(annotations),
		Generation:  1,
		CreatedAt:   now,
		UpdatedAt:   now,
		Spec:        spec,
		Status: types.WorkloadDeploymentStatus{
			Phase:     types.PhasePending,
			Replicas:  types.ReplicaStatus{Desired: spec.ReplicasOrDefault()},
			UpdatedAt: now,
		},
	}

	s.records[name] = &record{obj: deepCopyDeployment(obj)}
	return deepCopyDeployment(obj), nil
}

// Get returns a copy of the named WorkloadDeployment.
func (s *Store) Get(name string) (types.WorkloadDeployment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.records[name]
	if !ok {
		return types.WorkloadDeployment{}, ErrNotFound
	}
	return deepCopyDeployment(rec.obj), nil
}

// List returns copies of all WorkloadDeployments, sorted by name.
func (s *Store) List() []types.WorkloadDeployment {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]types.WorkloadDeployment, 0, len(s.records))
	for _, rec := range s.records {
		out = append(out, deepCopyDeployment(rec.obj))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// UpdateSpec replaces the spec (and optionally labels/annotations) of an
// existing, non-deleting WorkloadDeployment, bumping its Generation.
func (s *Store) UpdateSpec(name string, labels, annotations map[string]string, spec types.WorkloadDeploymentSpec) (types.WorkloadDeployment, error) {
	if err := spec.Validate(); err != nil {
		return types.WorkloadDeployment{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[name]
	if !ok {
		return types.WorkloadDeployment{}, ErrNotFound
	}
	if rec.deleting {
		return types.WorkloadDeployment{}, ErrDeleting
	}

	rec.obj.Spec = spec
	rec.obj.Labels = cloneMap(labels)
	rec.obj.Annotations = cloneMap(annotations)
	rec.obj.Generation++
	rec.obj.UpdatedAt = time.Now().UTC()
	rec.obj.Status.Replicas.Desired = spec.ReplicasOrDefault()

	return deepCopyDeployment(rec.obj), nil
}

// MarkDeleting flags a WorkloadDeployment for deletion. The reconciler will
// drain its instances and the record is removed once that finishes (see
// Remove). Returns ErrNotFound if it doesn't exist, and is a no-op (not an
// error) if already marked.
func (s *Store) MarkDeleting(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[name]
	if !ok {
		return ErrNotFound
	}
	rec.deleting = true
	rec.obj.UpdatedAt = time.Now().UTC()
	return nil
}

// IsDeleting reports whether a WorkloadDeployment has been marked for deletion.
func (s *Store) IsDeleting(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[name]
	return ok && rec.deleting
}

// Remove deletes the record outright. Callers (the reconciler) should only
// do this once a deleting deployment's instances have all been drained.
func (s *Store) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, name)
}

// Names returns the current set of WorkloadDeployment names, for the
// reconciler to iterate without holding the store lock.
func (s *Store) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.records))
	for name := range s.records {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TemplateHash returns the last template hash the reconciler recorded for
// name, and whether the deployment exists.
func (s *Store) TemplateHash(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[name]
	if !ok {
		return "", false
	}
	return rec.templateHash, true
}

// SetTemplateHash records the template hash the reconciler last acted on.
func (s *Store) SetTemplateHash(name, hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[name]; ok {
		rec.templateHash = hash
	}
}

// NextSlot returns the next unique slot index for name and advances the
// counter. Returns false if the deployment no longer exists.
func (s *Store) NextSlot(name string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[name]
	if !ok {
		return 0, false
	}
	slot := rec.nextSlot
	rec.nextSlot++
	return slot, true
}

// UpdateStatus applies mutate to the named deployment's status and persists
// the result. mutate receives a pointer to a working copy; returning an
// error aborts the update. Returns ErrNotFound if the deployment is gone
// (e.g. concurrently deleted mid-reconcile).
func (s *Store) UpdateStatus(name string, mutate func(*types.WorkloadDeploymentStatus)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[name]
	if !ok {
		return ErrNotFound
	}
	mutate(&rec.obj.Status)
	rec.obj.Status.UpdatedAt = time.Now().UTC()
	return nil
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// deepCopyDeployment returns a fully independent copy of obj so callers can
// never mutate store-owned state through a returned value. A JSON
// round-trip is a simple, correctness-obvious way to deep-copy a tree of
// maps/slices/structs that will only ever grow more fields over time.
func deepCopyDeployment(obj types.WorkloadDeployment) types.WorkloadDeployment {
	data, err := json.Marshal(obj)
	if err != nil {
		// obj is always JSON-marshalable (plain data types only); a failure
		// here would indicate a programming error, not a runtime condition.
		panic(fmt.Sprintf("store: deep copy failed: %v", err))
	}
	var out types.WorkloadDeployment
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Sprintf("store: deep copy failed: %v", err))
	}
	return out
}
