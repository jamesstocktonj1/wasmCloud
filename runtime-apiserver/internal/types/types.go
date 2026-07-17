// Package types defines the JSON-facing WorkloadDeployment API model.
//
// This mirrors the shape of the runtime-operator's WorkloadDeployment CRD
// (see runtime-operator/api/runtime/v1alpha1) closely enough that the two
// feel like the same concept, but it is a plain Go/JSON model with no
// Kubernetes dependency: there is no CRD, no etcd, and no controller-runtime
// involved. The apiserver stores these objects in memory and drives
// wash-hosts directly over NATS.
package types

import (
	"fmt"
	"regexp"
	"time"
)

// Phase is the coarse-grained lifecycle state of a WorkloadDeployment or of
// a single instance within it.
type Phase string

const (
	PhasePending     Phase = "Pending"  // waiting for a host to become available
	PhaseStarting    Phase = "Starting" // start request sent, host hasn't confirmed Running yet
	PhaseRunning     Phase = "Running"
	PhaseCompleted   Phase = "Completed"
	PhaseProgressing Phase = "Progressing" // deployment-level: some but not all instances ready
	PhaseStopping    Phase = "Stopping"
	PhaseStopped     Phase = "Stopped" // replicas == 0 and fully drained
	PhaseError       Phase = "Error"
	PhaseUnknown     Phase = "Unknown"
)

// nameRE mirrors the Kubernetes DNS-1123 label rules that the operator's CRD
// names already follow, so a name that's valid here is also valid if it's
// ever round-tripped through the CRD-based operator.
var nameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxNameLength = 63

// ValidateName reports whether name is a valid WorkloadDeployment name.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	if len(name) > maxNameLength {
		return fmt.Errorf("name must be %d characters or fewer", maxNameLength)
	}
	if !nameRE.MatchString(name) {
		return fmt.Errorf("name must consist of lowercase alphanumeric characters or '-', and must start and end with an alphanumeric character")
	}
	return nil
}

// LocalResources mirrors runtime-operator's LocalResources, minus the
// Kubernetes ConfigMap/Secret references: Config and Environment are taken
// as literal key/value maps since there is no ConfigMap/Secret store here.
type LocalResources struct {
	// Config is passed through to the component/service as low-level config.
	Config map[string]string `json:"config,omitempty"`
	// Environment variables, mapped to wasi:cli/environment.
	Environment map[string]string `json:"environment,omitempty"`
	// VolumeMounts mounts Volumes declared on the WorkloadTemplate.
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty"`
	// AllowedHosts is the outbound egress allowlist. Empty denies all
	// outbound requests (fail-closed); use ["*"] for unrestricted egress.
	AllowedHosts []string `json:"allowedHosts,omitempty"`
	// MemoryLimitMB caps the component/service's memory, in MiB. 0 means host default.
	MemoryLimitMB int32 `json:"memoryLimitMb,omitempty"`
	// CPULimit caps CPU usage. 0 means host default.
	CPULimit int32 `json:"cpuLimit,omitempty"`
}

// VolumeMount mounts a Volume declared on the WorkloadTemplate into a
// component or service.
type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// Volume is a named volume that can be mounted by components/services.
// Exactly one of Ephemeral or HostPath should be set; Ephemeral is assumed
// when neither is set.
type Volume struct {
	Name string `json:"name"`
	// Ephemeral requests a temporary directory scoped to the workload's lifetime.
	Ephemeral bool `json:"ephemeral,omitempty"`
	// HostPath, if set, mounts a pre-existing path from the host machine.
	HostPath string `json:"hostPath,omitempty"`
}

// ImagePullPolicy mirrors runtimev2.ImagePullPolicy.
type ImagePullPolicy string

const (
	ImagePullPolicyUnspecified  ImagePullPolicy = ""
	ImagePullPolicyAlways       ImagePullPolicy = "Always"
	ImagePullPolicyIfNotPresent ImagePullPolicy = "IfNotPresent"
	ImagePullPolicyNever        ImagePullPolicy = "Never"
)

// Component is a stateless, invocation-driven unit of computation within a workload.
type Component struct {
	Name            string          `json:"name"`
	Image           string          `json:"image"`
	PoolSize        int32           `json:"poolSize,omitempty"`
	MaxInvocations  int32           `json:"maxInvocations,omitempty"`
	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy,omitempty"`
	LocalResources  *LocalResources `json:"localResources,omitempty"`
}

// Service is the optional long-running "sidecar" component of a workload.
type Service struct {
	Image           string          `json:"image"`
	MaxRestarts     uint64          `json:"maxRestarts,omitempty"`
	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy,omitempty"`
	LocalResources  *LocalResources `json:"localResources,omitempty"`
}

// HostInterface declares a host-provided WIT interface the workload needs.
type HostInterface struct {
	// Name disambiguates multiple entries sharing the same namespace/package.
	Name       string            `json:"name,omitempty"`
	Namespace  string            `json:"namespace"`
	Package    string            `json:"package"`
	Version    string            `json:"version,omitempty"`
	Interfaces []string          `json:"interfaces"`
	Config     map[string]string `json:"config,omitempty"`
}

// WorkloadTemplate is the resolved-WIT-world blueprint for one instance of a
// WorkloadDeployment: the components, optional service, host interfaces and
// volumes that make up a single placement on a host.
type WorkloadTemplate struct {
	Components     []Component     `json:"components,omitempty"`
	Service        *Service        `json:"service,omitempty"`
	HostInterfaces []HostInterface `json:"hostInterfaces,omitempty"`
	Volumes        []Volume        `json:"volumes,omitempty"`
}

// WorkloadDeploymentSpec is the desired state of a WorkloadDeployment.
type WorkloadDeploymentSpec struct {
	// Replicas is the desired instance count. Defaults to 1 when nil.
	// Setting it to 0 stops all instances while keeping the deployment record.
	Replicas *int32 `json:"replicas,omitempty"`
	// HostID pins every instance to a specific host by its heartbeat-reported
	// ID. Leave empty to let the apiserver spread instances round-robin
	// across all hosts currently reporting a heartbeat.
	HostID string `json:"hostId,omitempty"`
	// Template is the workload blueprint placed on each instance.
	Template WorkloadTemplate `json:"template"`
}

// Validate checks the spec for structural problems the reconciler can't recover from.
func (s *WorkloadDeploymentSpec) Validate() error {
	if s.Replicas != nil && *s.Replicas < 0 {
		return fmt.Errorf("replicas must be >= 0")
	}
	if len(s.Template.Components) == 0 && s.Template.Service == nil {
		return fmt.Errorf("template must declare at least one component or a service")
	}
	seen := make(map[string]bool, len(s.Template.Components))
	for i, c := range s.Template.Components {
		if c.Name == "" {
			return fmt.Errorf("template.components[%d].name is required", i)
		}
		if c.Image == "" {
			return fmt.Errorf("template.components[%d].image is required", i)
		}
		if seen[c.Name] {
			return fmt.Errorf("template.components[%d]: duplicate component name %q", i, c.Name)
		}
		seen[c.Name] = true
	}
	if s.Template.Service != nil && s.Template.Service.Image == "" {
		return fmt.Errorf("template.service.image is required")
	}
	volumes := make(map[string]bool, len(s.Template.Volumes))
	for i, v := range s.Template.Volumes {
		if v.Name == "" {
			return fmt.Errorf("template.volumes[%d].name is required", i)
		}
		volumes[v.Name] = true
	}
	checkMounts := func(owner string, lr *LocalResources) error {
		if lr == nil {
			return nil
		}
		for i, vm := range lr.VolumeMounts {
			if !volumes[vm.Name] {
				return fmt.Errorf("%s.localResources.volumeMounts[%d]: volume %q not declared in template.volumes", owner, i, vm.Name)
			}
		}
		return nil
	}
	for _, c := range s.Template.Components {
		if err := checkMounts(fmt.Sprintf("template.components[%s]", c.Name), c.LocalResources); err != nil {
			return err
		}
	}
	if s.Template.Service != nil {
		if err := checkMounts("template.service", s.Template.Service.LocalResources); err != nil {
			return err
		}
	}
	return nil
}

// ReplicasOrDefault returns the desired replica count, defaulting to 1 when unset.
func (s *WorkloadDeploymentSpec) ReplicasOrDefault() int32 {
	if s.Replicas == nil {
		return 1
	}
	return *s.Replicas
}

// WorkloadInstance is the observed state of one placed copy of the template.
type WorkloadInstance struct {
	// Slot is a stable per-instance identifier, also used as the workload_id
	// sent to the host on start so restarts/reconciles are idempotent.
	Slot       string `json:"slot"`
	HostID     string `json:"hostId,omitempty"`
	WorkloadID string `json:"workloadId,omitempty"`
	Phase      Phase  `json:"phase"`
	Message    string `json:"message,omitempty"`
}

// ReplicaStatus summarizes instance counts.
type ReplicaStatus struct {
	Desired int32 `json:"desired"`
	Ready   int32 `json:"ready"`
}

// WorkloadDeploymentStatus is the observed state of a WorkloadDeployment.
type WorkloadDeploymentStatus struct {
	Phase     Phase              `json:"phase"`
	Message   string             `json:"message,omitempty"`
	Replicas  ReplicaStatus      `json:"replicas"`
	Instances []WorkloadInstance `json:"instances,omitempty"`
	UpdatedAt time.Time          `json:"updatedAt,omitempty"`
}

// WorkloadDeployment is the top-level API object managed by the apiserver.
type WorkloadDeployment struct {
	UID         string            `json:"uid"`
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Generation  int64             `json:"generation"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`

	Spec   WorkloadDeploymentSpec   `json:"spec"`
	Status WorkloadDeploymentStatus `json:"status"`
}
