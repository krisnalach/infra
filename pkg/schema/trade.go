package schema

import "time"

type Trade struct {
	Exchange   string
	ProductID  string
	Price      float64
	Size       float64
	Side       string // "BUY" or "SELL"
	TradeTime  time.Time
	ReceivedAt int64 // nanoseconds
}
