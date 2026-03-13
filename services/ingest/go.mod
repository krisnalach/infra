module github.com/krisnalach/infra/services/ingest

go 1.25.0

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.13.3
	github.com/gorilla/websocket v1.5.3
	github.com/krisnalach/infra/pkg/schema v0.0.0
)

replace github.com/krisnalach/infra/pkg/schema => ../../pkg/schema
