// Package hostclient talks directly to a single wash-host over NATS
// request/reply, using the same wasmcloud.runtime.v2 wire protocol that
// runtime-operator's controllers use (see
// runtime-operator/internal/controller/runtime/host_client.go). That client
// lives in an internal package and isn't importable outside the
// runtime-operator module, so this is a small, deliberate re-implementation
// against the same public pkg/wasmbus and pkg/rpc/.../v2 packages — the wire
// format is identical, only the orchestration around it (no CRDs, no
// controller-runtime) differs.
package hostclient

import (
	"context"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.wasmcloud.dev/runtime-operator/v2/pkg/wasmbus"

	runtimev2 "go.wasmcloud.dev/runtime-operator/v2/pkg/rpc/wasmcloud/runtime/v2"
)

// RoundtripTimeout is the default max timeout for a host RPC call; callers
// may set a lower context deadline as needed.
const RoundtripTimeout = 1 * time.Minute

// Client talks to one wash-host, addressed by its heartbeat-reported host ID.
type Client struct {
	Bus    wasmbus.Bus
	HostID string
}

// New returns a Client scoped to a single host.
func New(bus wasmbus.Bus, hostID string) *Client {
	return &Client{Bus: bus, HostID: hostID}
}

func (c *Client) subject(parts ...string) string {
	return strings.Join(append([]string{"runtime", "host", c.HostID}, parts...), ".")
}

// Heartbeat requests an on-demand heartbeat/status snapshot from the host.
func (c *Client) Heartbeat(ctx context.Context) (*runtimev2.HostHeartbeat, error) {
	var resp runtimev2.HostHeartbeat
	if err := roundTrip(ctx, c.Bus, c.subject("heartbeat"), &emptypb.Empty{}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Start asks the host to start a workload.
func (c *Client) Start(ctx context.Context, req *runtimev2.WorkloadStartRequest) (*runtimev2.WorkloadStartResponse, error) {
	var resp runtimev2.WorkloadStartResponse
	if err := roundTrip(ctx, c.Bus, c.subject("workload.start"), req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Status asks the host for a workload's current status.
func (c *Client) Status(ctx context.Context, req *runtimev2.WorkloadStatusRequest) (*runtimev2.WorkloadStatusResponse, error) {
	var resp runtimev2.WorkloadStatusResponse
	if err := roundTrip(ctx, c.Bus, c.subject("workload.status"), req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Stop asks the host to stop a workload.
func (c *Client) Stop(ctx context.Context, req *runtimev2.WorkloadStopRequest) (*runtimev2.WorkloadStopResponse, error) {
	var resp runtimev2.WorkloadStopResponse
	if err := roundTrip(ctx, c.Bus, c.subject("workload.stop"), req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// roundTrip sends a protojson-encoded request and decodes a protojson reply,
// exactly matching the wire format wash-host's washlet::handle_command
// expects and produces.
func roundTrip[Req proto.Message, Resp proto.Message](ctx context.Context, bus wasmbus.Bus, subject string, req Req, resp Resp) error {
	ctx, cancel := context.WithTimeout(ctx, RoundtripTimeout)
	defer cancel()

	data, err := protojson.Marshal(req)
	if err != nil {
		return err
	}

	msg := wasmbus.NewMessage(subject)
	msg.Data = data

	reply, err := bus.Request(ctx, msg)
	if err != nil {
		return err
	}

	return protojson.Unmarshal(reply.Data, resp)
}
