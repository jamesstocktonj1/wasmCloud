// Package reconciler drives WorkloadDeployments to their desired state by
// talking to wash-hosts directly over NATS. It plays the role that
// runtime-operator's Workload/WorkloadReplicaSet/WorkloadDeployment
// controllers play together, minus everything that exists only to satisfy
// Kubernetes (CRDs, finalizers, owner references, etcd watches): state lives
// in the in-memory store, and "watching for changes" is a plain ticker loop
// plus an explicit Nudge from the API layer for snappy convergence.
//
// Deploy policy is always "Recreate": when a WorkloadDeployment's template
// changes, all of its instances are stopped and replaced. There is no
// rolling-update support (that requires running old and new instances side
// by side, which is a meaningful chunk of extra complexity for a tool aimed
// at small/dev/edge deployments without a scheduler to spread the load).
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	runtimev2 "go.wasmcloud.dev/runtime-operator/v2/pkg/rpc/wasmcloud/runtime/v2"
	"go.wasmcloud.dev/runtime-operator/v2/pkg/wasmbus"

	"go.wasmcloud.dev/runtime-apiserver/internal/hostclient"
	"go.wasmcloud.dev/runtime-apiserver/internal/hostregistry"
	"go.wasmcloud.dev/runtime-apiserver/internal/store"
	"go.wasmcloud.dev/runtime-apiserver/internal/types"
)

const (
	startTimeout  = 60 * time.Second
	statusTimeout = 5 * time.Second
	stopTimeout   = 30 * time.Second
)

// Reconciler drives every WorkloadDeployment in Store toward its desired
// state on a periodic tick.
type Reconciler struct {
	Store    *store.Store
	Hosts    *hostregistry.Registry
	Bus      wasmbus.Bus
	Interval time.Duration
	Log      *slog.Logger

	nudge chan struct{}
}

// New builds a Reconciler. Call Run to start it.
func New(st *store.Store, hosts *hostregistry.Registry, bus wasmbus.Bus, interval time.Duration, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		Store:    st,
		Hosts:    hosts,
		Bus:      bus,
		Interval: interval,
		Log:      log,
		nudge:    make(chan struct{}, 1),
	}
}

// Nudge requests an immediate reconcile pass instead of waiting for the next
// tick. Safe to call from any goroutine; never blocks.
func (r *Reconciler) Nudge() {
	select {
	case r.nudge <- struct{}{}:
	default:
	}
}

// Run reconciles every WorkloadDeployment on each tick of Interval (and
// whenever Nudge is called) until ctx is canceled.
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	r.reconcileAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileAll(ctx)
		case <-r.nudge:
			r.reconcileAll(ctx)
		}
	}
}

func (r *Reconciler) reconcileAll(ctx context.Context) {
	for _, name := range r.Store.Names() {
		if err := r.reconcileOne(ctx, name); err != nil {
			r.Log.Error("reconcile failed", "workloadDeployment", name, "error", err)
		}
	}
}

func (r *Reconciler) reconcileOne(ctx context.Context, name string) error {
	obj, err := r.Store.Get(name)
	if errors.Is(err, store.ErrNotFound) {
		return nil // deleted concurrently, nothing to do
	}
	if err != nil {
		return err
	}

	deleting := r.Store.IsDeleting(name)
	var desired int32
	if !deleting {
		desired = obj.Spec.ReplicasOrDefault()
	}

	instances := append([]types.WorkloadInstance(nil), obj.Status.Instances...)

	if !deleting {
		newHash := hashTemplate(obj.Spec.Template)
		oldHash, ok := r.Store.TemplateHash(name)
		if ok && oldHash != "" && oldHash != newHash && len(instances) > 0 {
			r.Log.Info("template changed, recreating instances", "workloadDeployment", name)
			for i := range instances {
				r.stopInstance(ctx, name, &instances[i])
			}
			instances = instances[:0]
		}
		r.Store.SetTemplateHash(name, newHash)
	}

	// Scale down (also drains everything when deleting, since desired is 0).
	for int32(len(instances)) > desired {
		last := instances[len(instances)-1]
		r.stopInstance(ctx, name, &last)
		instances = instances[:len(instances)-1]
	}

	// Scale up.
	for int32(len(instances)) < desired {
		slot, ok := r.Store.NextSlot(name)
		if !ok {
			break // deployment was removed concurrently
		}
		instances = append(instances, types.WorkloadInstance{
			Slot:  fmt.Sprintf("%s-%d", obj.UID, slot),
			Phase: types.PhasePending,
		})
	}

	// Place unplaced instances and refresh status for placed ones.
	for i := range instances {
		r.reconcileInstance(ctx, obj, &instances[i])
	}

	if deleting && len(instances) == 0 {
		r.Store.Remove(name)
		return nil
	}

	return r.Store.UpdateStatus(name, func(status *types.WorkloadDeploymentStatus) {
		applyStatus(status, instances, desired, deleting)
	})
}

// reconcileInstance places inst if it isn't yet, or refreshes its status
// from the host if it is.
func (r *Reconciler) reconcileInstance(ctx context.Context, obj types.WorkloadDeployment, inst *types.WorkloadInstance) {
	if inst.HostID == "" {
		host, ok := r.Hosts.PickNext(obj.Spec.HostID)
		if !ok {
			inst.Phase = types.PhasePending
			if obj.Spec.HostID != "" {
				inst.Message = fmt.Sprintf("pinned host %q is not reporting a heartbeat", obj.Spec.HostID)
			} else {
				inst.Message = "no hosts are currently reporting a heartbeat"
			}
			return
		}
		inst.HostID = host.ID
	}

	client := hostclient.New(r.Bus, inst.HostID)

	if inst.WorkloadID == "" {
		r.startInstance(ctx, client, obj, inst)
		return
	}

	r.refreshInstance(ctx, client, inst)
}

func (r *Reconciler) startInstance(ctx context.Context, client *hostclient.Client, obj types.WorkloadDeployment, inst *types.WorkloadInstance) {
	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	resp, err := client.Start(startCtx, &runtimev2.WorkloadStartRequest{
		WorkloadId: inst.Slot,
		Workload:   buildWorkload(obj),
	})
	if err != nil {
		inst.Phase = types.PhaseError
		inst.Message = err.Error()
		// The host may be unreachable; let the next pass try a fresh pick
		// rather than hammering the same host every tick.
		inst.HostID = ""
		return
	}

	inst.WorkloadID = resp.GetWorkloadStatus().GetWorkloadId()
	if inst.WorkloadID == "" {
		inst.WorkloadID = inst.Slot
	}
	inst.Phase = mapWorkloadState(resp.GetWorkloadStatus().GetWorkloadState())
	inst.Message = resp.GetWorkloadStatus().GetMessage()
}

func (r *Reconciler) refreshInstance(ctx context.Context, client *hostclient.Client, inst *types.WorkloadInstance) {
	statusCtx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()

	resp, err := client.Status(statusCtx, &runtimev2.WorkloadStatusRequest{WorkloadId: inst.WorkloadID})
	if err != nil {
		inst.Phase = types.PhaseUnknown
		inst.Message = err.Error()
		return
	}

	state := resp.GetWorkloadStatus().GetWorkloadState()
	if state == runtimev2.WorkloadState_WORKLOAD_STATE_NOT_FOUND {
		// The host no longer knows this workload (e.g. it restarted with a
		// fresh in-memory state). Clear placement so the next pass re-places
		// it from scratch instead of polling a dead workload_id forever.
		inst.HostID = ""
		inst.WorkloadID = ""
		inst.Phase = types.PhasePending
		inst.Message = "workload not found on host, will re-place"
		return
	}

	inst.Phase = mapWorkloadState(state)
	inst.Message = resp.GetWorkloadStatus().GetMessage()
}

// stopInstance best-effort stops a placed instance. Failures are logged, not
// returned: the caller is removing the instance from the desired set either
// way (scale-down, recreate, or delete), and a dangling workload on an
// unreachable host isn't something a retry here can fix.
func (r *Reconciler) stopInstance(ctx context.Context, deploymentName string, inst *types.WorkloadInstance) {
	if inst.HostID == "" || inst.WorkloadID == "" {
		return
	}
	client := hostclient.New(r.Bus, inst.HostID)
	stopCtx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()
	if _, err := client.Stop(stopCtx, &runtimev2.WorkloadStopRequest{WorkloadId: inst.WorkloadID}); err != nil {
		r.Log.Warn("failed to stop workload on host",
			"workloadDeployment", deploymentName, "slot", inst.Slot, "hostId", inst.HostID, "error", err)
	}
}

// applyStatus aggregates per-instance state into the deployment-level status.
func applyStatus(status *types.WorkloadDeploymentStatus, instances []types.WorkloadInstance, desired int32, deleting bool) {
	status.Instances = instances
	status.Replicas.Desired = desired

	var ready int32
	hasError := false
	for _, inst := range instances {
		if inst.Phase == types.PhaseRunning || inst.Phase == types.PhaseCompleted {
			ready++
		}
		if inst.Phase == types.PhaseError {
			hasError = true
		}
	}
	status.Replicas.Ready = ready

	switch {
	case deleting:
		status.Phase = types.PhaseStopping
		status.Message = fmt.Sprintf("draining %d instance(s)", len(instances))
	case desired == 0 && len(instances) == 0:
		status.Phase = types.PhaseStopped
		status.Message = ""
	case desired > 0 && ready == desired:
		status.Phase = types.PhaseRunning
		status.Message = ""
	case ready > 0:
		status.Phase = types.PhaseProgressing
		status.Message = fmt.Sprintf("%d/%d instances ready", ready, desired)
	case hasError:
		status.Phase = types.PhaseError
		status.Message = firstMatchingMessage(instances, types.PhaseError, "one or more instances failed to start")
	default:
		status.Phase = types.PhasePending
		status.Message = firstMatchingMessage(instances, "", "")
	}
}

// firstMatchingMessage returns the Message of the first instance whose Phase
// equals want (or the first non-empty Message of any instance, when want is
// empty), falling back to fallback if nothing matches.
func firstMatchingMessage(instances []types.WorkloadInstance, want types.Phase, fallback string) string {
	for _, inst := range instances {
		if inst.Message == "" {
			continue
		}
		if want == "" || inst.Phase == want {
			return inst.Message
		}
	}
	return fallback
}
