package reconciler

import (
	"encoding/json"
	"fmt"
	"hash/fnv"

	runtimev2 "go.wasmcloud.dev/runtime-operator/v2/pkg/rpc/wasmcloud/runtime/v2"

	"go.wasmcloud.dev/runtime-apiserver/internal/types"
)

// workloadNamespace is a constant placeholder for runtimev2.Workload's
// Namespace field. It's a Kubernetes-namespace hangover in the wire
// protocol that wash-host only ever uses for tracing/log fields (see
// workload.namespace instrumentation in crates/wash-runtime), never for
// identity or isolation, so there's nothing meaningful to put there without
// Kubernetes.
const workloadNamespace = "default"

// hashTemplate returns a stable content hash of a WorkloadTemplate, used to
// detect spec changes that require recreating a deployment's instances.
func hashTemplate(tpl types.WorkloadTemplate) string {
	// Marshaling can't fail: WorkloadTemplate is built entirely from plain
	// data types (strings, maps, slices, structs of the same).
	data, _ := json.Marshal(tpl)
	h := fnv.New64a()
	_, _ = h.Write(data)
	return fmt.Sprintf("%x", h.Sum64())
}

// buildWorkload converts a WorkloadDeployment's template into the
// wire-format Workload the host's WorkloadStart RPC expects.
func buildWorkload(obj types.WorkloadDeployment) *runtimev2.Workload {
	tpl := obj.Spec.Template

	witWorld := &runtimev2.WitWorld{
		Components:     make([]*runtimev2.Component, 0, len(tpl.Components)),
		HostInterfaces: make([]*runtimev2.WitInterface, 0, len(tpl.HostInterfaces)),
	}
	for _, hi := range tpl.HostInterfaces {
		witWorld.HostInterfaces = append(witWorld.HostInterfaces, &runtimev2.WitInterface{
			Namespace:  hi.Namespace,
			Package:    hi.Package,
			Version:    hi.Version,
			Interfaces: hi.Interfaces,
			Config:     hi.Config,
			Name:       hi.Name,
		})
	}
	for _, c := range tpl.Components {
		witWorld.Components = append(witWorld.Components, &runtimev2.Component{
			Name:            c.Name,
			Image:           c.Image,
			ImagePullPolicy: translatePullPolicy(c.ImagePullPolicy),
			PoolSize:        c.PoolSize,
			MaxInvocations:  c.MaxInvocations,
			LocalResources:  buildLocalResources(c.LocalResources),
		})
	}

	var service *runtimev2.Service
	if s := tpl.Service; s != nil {
		service = &runtimev2.Service{
			Image:           s.Image,
			ImagePullPolicy: translatePullPolicy(s.ImagePullPolicy),
			LocalResources:  buildLocalResources(s.LocalResources),
			MaxRestarts:     s.MaxRestarts,
		}
	}

	volumes := make([]*runtimev2.Volume, 0, len(tpl.Volumes))
	for _, v := range tpl.Volumes {
		vol := &runtimev2.Volume{Name: v.Name}
		if v.HostPath != "" {
			vol.VolumeType = &runtimev2.Volume_HostPath{
				HostPath: &runtimev2.HostPathVolume{LocalPath: v.HostPath},
			}
		} else {
			vol.VolumeType = &runtimev2.Volume_EmptyDir{EmptyDir: &runtimev2.EmptyDirVolume{}}
		}
		volumes = append(volumes, vol)
	}

	return &runtimev2.Workload{
		Namespace:   workloadNamespace,
		Name:        obj.Name,
		Annotations: obj.Annotations,
		WitWorld:    witWorld,
		Volumes:     volumes,
		Service:     service,
	}
}

func buildLocalResources(lr *types.LocalResources) *runtimev2.LocalResources {
	if lr == nil {
		return nil
	}
	out := &runtimev2.LocalResources{
		Config:        lr.Config,
		Environment:   lr.Environment,
		AllowedHosts:  lr.AllowedHosts,
		MemoryLimitMb: lr.MemoryLimitMB,
		CpuLimit:      lr.CPULimit,
	}
	for _, vm := range lr.VolumeMounts {
		out.VolumeMounts = append(out.VolumeMounts, &runtimev2.VolumeMount{
			Name:      vm.Name,
			MountPath: vm.MountPath,
			ReadOnly:  vm.ReadOnly,
		})
	}
	return out
}

func translatePullPolicy(p types.ImagePullPolicy) runtimev2.ImagePullPolicy {
	switch p {
	case types.ImagePullPolicyAlways:
		return runtimev2.ImagePullPolicy_IMAGE_PULL_POLICY_ALWAYS
	case types.ImagePullPolicyIfNotPresent:
		return runtimev2.ImagePullPolicy_IMAGE_PULL_POLICY_IF_NOT_PRESENT
	case types.ImagePullPolicyNever:
		return runtimev2.ImagePullPolicy_IMAGE_PULL_POLICY_NEVER
	default:
		return runtimev2.ImagePullPolicy_IMAGE_PULL_POLICY_UNSPECIFIED
	}
}

func mapWorkloadState(state runtimev2.WorkloadState) types.Phase {
	switch state {
	case runtimev2.WorkloadState_WORKLOAD_STATE_STARTING:
		return types.PhaseStarting
	case runtimev2.WorkloadState_WORKLOAD_STATE_RUNNING:
		return types.PhaseRunning
	case runtimev2.WorkloadState_WORKLOAD_STATE_COMPLETED:
		return types.PhaseCompleted
	case runtimev2.WorkloadState_WORKLOAD_STATE_STOPPING:
		return types.PhaseStopping
	case runtimev2.WorkloadState_WORKLOAD_STATE_ERROR:
		return types.PhaseError
	default:
		return types.PhaseUnknown
	}
}
