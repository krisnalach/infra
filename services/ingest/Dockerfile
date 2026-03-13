# Stage 1
FROM --platform=linux/amd64 golang:1.25-bookworm AS builder

WORKDIR /app

COPY pkg/schema/go.mod ./pkg/schema/
COPY services/ingest/go.mod services/ingest/go.sum ./services/ingest/
RUN cd services/ingest && go mod download

COPY pkg/schema/ ./pkg/schema/
COPY services/ingest/ ./services/ingest/

RUN cd services/ingest && CGO_ENABLED=1 go build -o /ingest .

# Stage 2
FROM --platform=linux/amd64 debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /ingest /ingest
RUN chmod +x /ingest

ENTRYPOINT ["/ingest"]