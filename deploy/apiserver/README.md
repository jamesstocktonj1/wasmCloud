# apiserver setup

Runs `wash-host` without Kubernetes: just NATS and `runtime-apiserver`, the
Go service that manages `WorkloadDeployment`s directly over NATS (no CRDs,
no cluster). See [`runtime-apiserver/README.md`](../../runtime-apiserver/README.md)
for the full API reference.

Start everything:

```
docker compose up --build
```

`--build` is only needed the first time (or after changing
`runtime-apiserver` source) since its image isn't published yet; `nats` and
`wash-host` pull published images.

This brings up:

- `nats` — the message bus, on `localhost:4222`
- `wash-host` — a single host, with its HTTP trigger port on `localhost:8081`
- `apiserver` — the API, on `localhost:8080`

## Deploy a workload

`wash-host` needs a few seconds to connect and send its first heartbeat
before the apiserver knows about it. Check `GET /v1/hosts` until `available`
is `true`:

```
curl -s localhost:8080/v1/hosts | jq
```

Then create a `WorkloadDeployment` running a published example component
([`examples/qrcode`](../../examples/qrcode)). `wash-host`'s HTTP router
dispatches purely by `Host` header, so reaching the component over HTTP
requires declaring a `wasi:http/incoming-handler` host interface with the
hostname you'll send requests as:

```
curl -s -X POST localhost:8080/v1/workloaddeployments \
  -H 'content-type: application/json' \
  -d '{
    "name": "qrcode",
    "spec": {
      "replicas": 1,
      "template": {
        "components": [
          {
            "name": "qrcode",
            "image": "ghcr.io/wasmcloud/components/qrcode:0.1.0"
          }
        ],
        "hostInterfaces": [
          {
            "namespace": "wasi",
            "package": "http",
            "interfaces": ["incoming-handler"],
            "config": {"host": "qrcode.local"}
          }
        ]
      }
    }
  }' | jq
```

Watch it come up:

```
curl -s localhost:8080/v1/workloaddeployments/qrcode | jq .status
```

Once `status.phase` is `"Running"`, the component is reachable through the
host's own HTTP port, using the `host` you declared above as the `Host` header:

```
curl -s -H 'Host: qrcode.local' localhost:8081/
curl -s -H 'Host: qrcode.local' -H 'content-type: application/json' \
  -d '{"payload": "hello"}' localhost:8081/qrcode -o qrcode.png
```

Scale it down without deleting it (`PUT` replaces the whole spec, so the
template needs to be repeated):

```
curl -s -X PUT localhost:8080/v1/workloaddeployments/qrcode \
  -H 'content-type: application/json' \
  -d '{
    "spec": {
      "replicas": 0,
      "template": {
        "components": [
          {"name": "qrcode", "image": "ghcr.io/wasmcloud/components/qrcode:0.1.0"}
        ],
        "hostInterfaces": [
          {
            "namespace": "wasi",
            "package": "http",
            "interfaces": ["incoming-handler"],
            "config": {"host": "qrcode.local"}
          }
        ]
      }
    }
  }' | jq
```

Delete it entirely:

```
curl -s -X DELETE localhost:8080/v1/workloaddeployments/qrcode -i
```
