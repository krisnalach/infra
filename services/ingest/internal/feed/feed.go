package feed

import (
	"github.com/krisnalach/infra/pkg/schema"
)

type Feed interface {
	Subscribe(products []string) (<-chan schema.Trade, error)
	Close() error
}
