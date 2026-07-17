# runtime-apiserver

Manages wasmCloud `WorkloadDeployment`s directly against one or more
`wash-host`s over NATS — no Kubernetes, no CRDs, no cluster required.

It speaks the same `wasmcloud.runtime.v2` wire protocol
(`runtime.host.<id>.workload.{start,status,stop}`,
`runtime.operator.heartbeat.<id>`) that `runtime-operator` uses, so any
`wash-host` that works with the Kubernetes operator works here unmodified.
State — which `WorkloadDeployment`s exist, and where their instances are
placed — lives in memory in the `apiserver` process; see
[Design notes](#design-notes--current-limitations) below.

## Running it

```
go run ./cmd/apiserver -nats-url nats://127.0.0.1:4222
```

Flags:

| Flag | Default | Description |
| --- | --- | --- |
| `-listen-addr` | `:8080` | HTTP API bind address |
| `-nats-url` | `nats://127.0.0.1:4222` | NATS server to connect to |
| `-nats-creds` | _(none)_ | Path to a NATS credentials file |
| `-reconcile-interval` | `5s` | How often to reconcile deployments against host state |
| `-host-heartbeat-ttl` | `45s` | How long a host stays eligible for new placements after its last heartbeat |
| `-json-log` | `false` | Emit logs as JSON |

See [`deploy/apiserver`](../deploy/apiserver) for a docker-compose stack that
runs this alongside `nats` and a `wash-host`, with a full walkthrough.

## API

All bodies are JSON. `WorkloadDeployment.spec.template` mirrors
`runtime-operator`'s `WorkloadSpec` (components/service/hostInterfaces/volumes)
closely enough to feel familiar, minus anything that only makes sense with
Kubernetes (ConfigMap/Secret references, image pull secrets, Service/EndpointSlice
management). `config`/`environment` on a component or service take literal
key/value maps instead of ConfigMap/Secret references.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/workloaddeployments` | Create a WorkloadDeployment |
| `GET` | `/v1/workloaddeployments` | List all WorkloadDeployments |
| `GET` | `/v1/workloaddeployments/{name}` | Get one WorkloadDeployment |
| `PUT` | `/v1/workloaddeployments/{name}` | Replace its labels/annotations/spec |
| `DELETE` | `/v1/workloaddeployments/{name}` | Drain and remove it |
| `GET` | `/v1/hosts` | List every host known from heartbeats, sorted by ID |
| `GET` | `/v1/hosts/{id}` | Get one host |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness (NATS connected) |

### WorkloadDeployment shape

```jsonc
{
  "name": "my-app",
  "labels": { "team": "platform" },
  "spec": {
    "replicas": 2, // defaults to 1; 0 stops all instances but keeps the record
    "hostId": "", // optional: pin every instance to one host's heartbeat-reported ID
    "template": {
      "components": [
        {
          "name": "api",
          "image": "ghcr.io/example/api:0.1.0",
          "poolSize": 4,
          "localResources": {
            "environment": { "LOG_LEVEL": "info" },
            "allowedHosts": ["*.example.com"]
          }
        }
      ],
      "service": null, // optional long-running sidecar
      "hostInterfaces": [], // host-provided WIT interfaces this workload imports
      "volumes": []
    }
  },
  "status": {
    "phase": "Running", // Pending | Progressing | Running | Stopping | Stopped | Error | Unknown
    "replicas": { "desired": 2, "ready": 2 },
    "instances": [
      { "slot": "<uid>-0", "hostId": "...", "workloadId": "...", "phase": "Running" }
    ]
  }
}
```

## Design notes / current limitations

- **No persistence.** State lives in memory; restarting `apiserver` forgets
  every WorkloadDeployment (the workloads themselves keep running on their
  hosts, but nothing manages them until they're re-created). Add a durable
  store behind `internal/store` if that's needed.
- **Deploy policy is always "Recreate".** Changing `spec.template` stops all
  of a deployment's instances and starts fresh ones; there's no rolling
  update. For a tool aimed at small/dev/edge deployments without a
  multi-host scheduler, overlapping old/new instances didn't seem worth the
  complexity yet.
- **Placement is round-robin, not bin-packed.** Each new instance goes to
  the next available host in ID order (or the pinned `spec.hostId`). There's
  no CPU/memory-aware scheduling like `runtime-operator`'s backpressure
  thresholds.
- **Host discovery is heartbeat-only**, exactly like `runtime-operator`: a
  host is "known" once it's published one `HostHeartbeat`
  (`runtime.operator.heartbeat.<id>`), and stays "available" for new
  placements until `-host-heartbeat-ttl` elapses without another one.
