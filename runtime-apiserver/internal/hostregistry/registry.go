// Package hostregistry tracks wash-hosts by listening for the heartbeats
// they broadcast over NATS, without any Kubernetes Host CRD involved.
//
// A wash-host generates a random UUID host ID at process startup and
// publishes a wasmcloud.runtime.v2.HostHeartbeat to
// "runtime.operator.heartbeat.<hostID>" on a fixed interval (15s by
// default; see crates/wash-runtime/src/washlet/mod.rs). This mirrors what
// runtime-operator's hostStatusUpdater does when it upserts Host CRDs, but
// keeps the result in memory instead of etcd.
package hostregistry

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"go.wasmcloud.dev/runtime-operator/v2/pkg/wasmbus"

	runtimev2 "go.wasmcloud.dev/runtime-operator/v2/pkg/rpc/wasmcloud/runtime/v2"
)

// heartbeatSubject matches every host's heartbeat, mirroring
// runtime-operator's hostStatusUpdater subscription.
const heartbeatSubject = "runtime.operator.heartbeat.>"

// HostInfo is a snapshot of the most recent heartbeat received from a host.
type HostInfo struct {
	ID                string
	FriendlyName      string
	Hostname          string
	Environment       string
	Labels            map[string]string
	Version           string
	OSName            string
	OSArch            string
	OSKernel          string
	SystemCPUUsage    float32
	SystemMemoryTotal uint64
	SystemMemoryFree  uint64
	ComponentCount    uint64
	WorkloadCount     uint64
	HTTPPort          uint32
	LastSeen          time.Time
}

// Registry is a thread-safe, in-memory set of hosts, kept fresh by
// subscribing to host heartbeats.
type Registry struct {
	ttl time.Duration

	mu    sync.RWMutex
	hosts map[string]HostInfo

	cursor atomic.Uint64
}

// New returns a Registry that considers a host unavailable once ttl has
// elapsed since its last heartbeat.
func New(ttl time.Duration) *Registry {
	return &Registry{
		ttl:   ttl,
		hosts: make(map[string]HostInfo),
	}
}

// Run subscribes to host heartbeats and updates the registry until ctx is
// canceled. It's meant to be run in its own goroutine.
func (r *Registry) Run(ctx context.Context, bus wasmbus.Bus) error {
	sub, err := bus.Subscribe(heartbeatSubject, 256)
	if err != nil {
		return err
	}

	sub.Handle(func(msg *wasmbus.Message) {
		r.handleHeartbeat(msg.Data)
	})

	<-ctx.Done()
	return sub.Drain()
}

func (r *Registry) handleHeartbeat(data []byte) {
	var hb runtimev2.HostHeartbeat
	if err := protojson.Unmarshal(data, &hb); err != nil || hb.GetId() == "" {
		return
	}

	info := HostInfo{
		ID:                hb.GetId(),
		FriendlyName:      hb.GetFriendlyName(),
		Hostname:          hb.GetHostname(),
		Environment:       hb.GetEnvironment(),
		Labels:            hb.GetLabels(),
		Version:           hb.GetVersion(),
		OSName:            hb.GetOsName(),
		OSArch:            hb.GetOsArch(),
		OSKernel:          hb.GetOsKernel(),
		SystemCPUUsage:    hb.GetSystemCpuUsage(),
		SystemMemoryTotal: hb.GetSystemMemoryTotal(),
		SystemMemoryFree:  hb.GetSystemMemoryFree(),
		ComponentCount:    hb.GetComponentCount(),
		WorkloadCount:     hb.GetWorkloadCount(),
		HTTPPort:          hb.GetHttpPort(),
		LastSeen:          time.Now(),
	}

	r.mu.Lock()
	r.hosts[info.ID] = info
	r.mu.Unlock()
}

// isAvailable reports whether info has reported a heartbeat within the TTL.
func (r *Registry) isAvailable(info HostInfo) bool {
	return time.Since(info.LastSeen) < r.ttl
}

// Get returns the last known info for hostID.
func (r *Registry) Get(hostID string) (HostInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.hosts[hostID]
	return info, ok
}

// IsAvailable reports whether hostID is known and has reported a heartbeat
// within the TTL.
func (r *Registry) IsAvailable(hostID string) bool {
	info, ok := r.Get(hostID)
	return ok && r.isAvailable(info)
}

// List returns every host the registry has ever heard from, freshest data
// last received, regardless of TTL. Intended for diagnostics (GET /v1/hosts).
func (r *Registry) List() []HostInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]HostInfo, 0, len(r.hosts))
	for _, info := range r.hosts {
		out = append(out, info)
	}
	return out
}

// Available returns hosts that have reported a heartbeat within the TTL.
func (r *Registry) Available() []HostInfo {
	all := r.List()
	out := make([]HostInfo, 0, len(all))
	for _, info := range all {
		if r.isAvailable(info) {
			out = append(out, info)
		}
	}
	return out
}

// PickNext selects a host to place a new instance on: preferredID if it's
// alive, otherwise the next available host in round-robin order. Returns
// false if no host qualifies.
func (r *Registry) PickNext(preferredID string) (HostInfo, bool) {
	if preferredID != "" {
		info, ok := r.Get(preferredID)
		if ok && r.isAvailable(info) {
			return info, true
		}
		return HostInfo{}, false
	}

	available := r.Available()
	if len(available) == 0 {
		return HostInfo{}, false
	}
	// Sort for a deterministic round-robin order; List()'s map iteration
	// order is randomized and would make the cursor meaningless otherwise.
	sortHostsByID(available)
	idx := r.cursor.Add(1) % uint64(len(available))
	return available[idx], true
}

func sortHostsByID(hosts []HostInfo) {
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].ID < hosts[j].ID })
}
