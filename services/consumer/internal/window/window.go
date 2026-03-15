package window

import (
	"fmt"
	"time"

	"github.com/krisnalach/infra/pkg/schema"
)

type Stats struct {
	symbol     string
	tradeCount int
	buyVolume  float64
	sellVolume float64
	startTime  time.Time
	endTime    time.Time
	vwap       float64
	imbalance  float64
}

type Window struct {
	Symbol    string
	Trades    []schema.Trade
	StartTime time.Time
	Duration  int64
	Emit      chan Stats // Stats delivered through here
}

func (w *Window) Add(trade schema.Trade) {
	if w.StartTime.IsZero() {
		w.StartTime = trade.TradeTime
	}
	endTime := w.StartTime.Add(time.Duration(w.Duration) * time.Second)
	if trade.TradeTime.After(endTime) {
		snapshot := w.Trades
		snapshotStart := w.StartTime
		// Outside, compute stats and make new
		go func() {
			stats := compute(snapshot, w.Symbol, snapshotStart, w.Duration)
			w.Emit <- stats
		}()
		w.Trades = make([]schema.Trade, 0)
		w.StartTime = endTime
		w.Trades = append(w.Trades, trade)

	} else {
		w.Trades = append(w.Trades, trade)
	}
}

func (w *Window) Flush(trade schema.Trade) {
	go func() {
		stats := compute(w.Trades, w.Symbol, w.StartTime, w.Duration)
		w.Emit <- stats
	}()
	w.Trades = make([]schema.Trade, 0)
	w.StartTime = time.Now()
}

// Emits imbalance, VWAP, trade count, and volume for a set of trades through provided chan
func compute(trades []schema.Trade, symbol string, startTime time.Time, duration int64) Stats {
	var pq float64 = 0
	var q float64 = 0
	var sell float64 = 0
	var buy float64 = 0
	var vwap float64 = 0
	var imbalance float64 = 0
	tradeCount := 0

	for _, trade := range trades {
		pq += trade.Size * trade.Price
		q += trade.Size

		if trade.Side == "BUY" {
			buy += trade.Size
		} else {
			sell += trade.Size
		}
		tradeCount += 1

	}
	if q != 0 {
		vwap = pq / q
	}
	if (buy + sell) != 0 {
		imbalance = (buy - sell) / (buy + sell)
	}

	return Stats{symbol: symbol,
		tradeCount: tradeCount,
		buyVolume:  buy,
		sellVolume: sell,
		startTime:  startTime,
		endTime:    startTime.Add(time.Duration(duration) * time.Second),
		vwap:       vwap,
		imbalance:  imbalance}

}

func (s Stats) String() string {
	return fmt.Sprintf(
		"Symbol: %s\nTrades: %d\nBuy Volume: %.2f\nSell Volume: %.2f\nVWAP: %.2f\nImbalance: %.2f\nStart: %s\nEnd: %s",
		s.symbol,
		s.tradeCount,
		s.buyVolume,
		s.sellVolume,
		s.vwap,
		s.imbalance,
		s.startTime.Format(time.RFC3339),
		s.endTime.Format(time.RFC3339),
	)
}
