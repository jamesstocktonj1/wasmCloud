package api

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"go.wasmcloud.dev/runtime-apiserver/internal/hostregistry"
)

// hostResponse is the wire representation of a host's last known heartbeat.
type hostResponse struct {
	ID                string            `json:"id"`
	FriendlyName      string            `json:"friendlyName,omitempty"`
	Hostname          string            `json:"hostname,omitempty"`
	Environment       string            `json:"environment,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Version           string            `json:"version,omitempty"`
	OSName            string            `json:"osName,omitempty"`
	OSArch            string            `json:"osArch,omitempty"`
	OSKernel          string            `json:"osKernel,omitempty"`
	SystemCPUUsage    float32           `json:"systemCpuUsage,omitempty"`
	SystemMemoryTotal uint64            `json:"systemMemoryTotal,omitempty"`
	SystemMemoryFree  uint64            `json:"systemMemoryFree,omitempty"`
	ComponentCount    uint64            `json:"componentCount,omitempty"`
	WorkloadCount     uint64            `json:"workloadCount,omitempty"`
	HTTPPort          uint32            `json:"httpPort,omitempty"`
	LastSeen          time.Time         `json:"lastSeen"`
	// Available reports whether this host has reported a heartbeat recently
	// enough to be eligible for new placements.
	Available bool `json:"available"`
}

func toHostResponse(info hostregistry.HostInfo, available bool) hostResponse {
	return hostResponse{
		ID:                info.ID,
		FriendlyName:      info.FriendlyName,
		Hostname:          info.Hostname,
		Environment:       info.Environment,
		Labels:            info.Labels,
		Version:           info.Version,
		OSName:            info.OSName,
		OSArch:            info.OSArch,
		OSKernel:          info.OSKernel,
		SystemCPUUsage:    info.SystemCPUUsage,
		SystemMemoryTotal: info.SystemMemoryTotal,
		SystemMemoryFree:  info.SystemMemoryFree,
		ComponentCount:    info.ComponentCount,
		WorkloadCount:     info.WorkloadCount,
		HTTPPort:          info.HTTPPort,
		LastSeen:          info.LastSeen,
		Available:         available,
	}
}

func (s *server) handleListHosts(w http.ResponseWriter, _ *http.Request) {
	hosts := s.deps.Hosts.List()
	out := make([]hostResponse, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, toHostResponse(h, s.deps.Hosts.IsAvailable(h.ID)))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	writeJSON(w, http.StatusOK, listResponse[hostResponse]{Items: out})
}

func (s *server) handleGetHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	info, ok := s.deps.Hosts.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("host %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, toHostResponse(info, s.deps.Hosts.IsAvailable(id)))
}
