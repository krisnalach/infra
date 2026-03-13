# Stage 1
FROM --platform=linux/amd64 golang:1.25-bookworm AS builder

WORKDIR /app

COPY pkg/schema/go.mod ./pkg/schema/
COPY services/consumer/go.mod services/consumer/go.sum ./services/consumer/
RUN cd services/consumer && go mod download

COPY pkg/schema/ ./pkg/schema/
COPY services/consumer/ ./services/consumer/

RUN cd services/consumer && CGO_ENABLED=1 go build -o /consumer .

# Stage 2
FROM --platform=linux/amd64 debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /consumer /consumer
RUN chmod +x /consumer

ENTRYPOINT ["/consumer"]