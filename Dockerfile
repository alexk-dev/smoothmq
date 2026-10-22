FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /smoothmq .

FROM builder AS test
RUN apt-get update && apt-get install -y --no-install-recommends redis-server python3 python3-venv && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/tests && /opt/tests/bin/pip install --no-cache-dir boto3==1.43.116 botocore==1.43.116
RUN go vet ./... && go test -race ./... && go build -race -o /tmp/smoothmq-race .
RUN /opt/tests/bin/python integration/critic_acceptance.py --binary /tmp/smoothmq-race --slow

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /smoothmq /smoothmq
EXPOSE 3001 2112
ENTRYPOINT ["/smoothmq"]
CMD ["server"]
