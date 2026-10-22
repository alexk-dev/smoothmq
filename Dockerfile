FROM redis:7.4.11-bookworm@sha256:44960b3100c48159a66d28c18b0588c193492e25f7551d60e51dd464fc3eca5b AS redis-test-tools

FROM golang:1.27.3-bookworm@sha256:972b4110ba0f47539a975c31e52c64765e1ff19f2d74469ae4a595c6676e80a5 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /smoothmq .

FROM builder AS test
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 python3 python3-venv && rm -rf /var/lib/apt/lists/*
COPY --from=redis-test-tools /usr/local/bin/redis-server /usr/local/bin/redis-cli /usr/local/bin/
RUN redis-server --version | grep 'v=7.4.11 ' && redis-cli --version
RUN python3 -m venv /opt/tests && /opt/tests/bin/pip install --no-cache-dir boto3==1.43.116 botocore==1.43.116
RUN go install golang.org/x/vuln/cmd/govulncheck@v1.9.0 && govulncheck ./...
RUN go vet ./... && go test -race ./... && go build -race -o /tmp/smoothmq-race .
RUN /opt/tests/bin/python integration/critic_acceptance.py --binary /tmp/smoothmq-race --slow

FROM gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88 AS runtime
COPY --from=builder /smoothmq /smoothmq
EXPOSE 3001 2112
ENTRYPOINT ["/smoothmq"]
CMD ["server"]
