# syntax=docker/dockerfile:1.4

FROM golang:1.26.4 AS builder
WORKDIR /workspace/runtime-apiserver

# dependencies cache
COPY go.mod go.sum ./
RUN go mod download

# source code
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/runtime-apiserver ./cmd/apiserver

FROM cgr.dev/chainguard/static:latest AS release
COPY --from=builder /go/bin/runtime-apiserver /usr/local/bin/runtime-apiserver
ENTRYPOINT ["/usr/local/bin/runtime-apiserver"]
