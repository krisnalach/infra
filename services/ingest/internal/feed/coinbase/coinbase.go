package coinbase

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/krisnalach/infra/pkg/schema"
	"github.com/krisnalach/infra/services/ingest/internal/feed"
)

const url = "wss://advanced-trade-ws.coinbase.com"

type Envelope struct {
	Channel     string          `json:"channel"`
	Timestamp   string          `json:"timestamp"`
	SequenceNum int64           `json:"sequence_num"`
	Events      json.RawMessage `json:"events"`
}

type SubscribeMsg struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channel    string   `json:"channel"`
}

// event structure for market_trades
type tradeEvent struct {
	Type   string     `json:"type"`
	Trades []rawTrade `json:"trades"`
}

type rawTrade struct {
	ProductID string `json:"product_id"`
	TradeID   string `json:"trade_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"` // RFC3339
	Side      string `json:"side"`
}

type client struct {
	conn *websocket.Conn
}

func New() feed.Feed {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil
	}

	c := &client{conn: conn}

	conn.SetPingHandler(func(data string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})

	return c
}

func (c *client) Subscribe(products []string) (<-chan schema.Trade, error) {
	sub := SubscribeMsg{
		Type:       "subscribe",
		ProductIDs: products,
		Channel:    "market_trades",
	}

	if err := c.conn.WriteJSON(sub); err != nil {
		return nil, err
	}

	ch := make(chan schema.Trade)

	go func() {
		defer close(ch)

		for {
			_, raw, err := c.conn.ReadMessage()
			if err != nil {
				return
			}

			var env Envelope
			if err := json.Unmarshal(raw, &env); err != nil {
				continue
			}

			if env.Channel != "market_trades" {
				continue
			}

			var events []tradeEvent
			if err := json.Unmarshal(env.Events, &events); err != nil {
				continue
			}

			receivedAt := time.Now().UnixNano()

			for _, event := range events {
				for _, t := range event.Trades {
					price, err := strconv.ParseFloat(t.Price, 64)
					if err != nil {
						continue
					}
					size, err := strconv.ParseFloat(t.Size, 64)
					if err != nil {
						continue
					}
					tradeTime, err := time.Parse(time.RFC3339Nano, t.Time)
					if err != nil {
						continue
					}

					ch <- schema.Trade{
						Exchange:   "coinbase",
						ProductID:  t.ProductID,
						Price:      price,
						Size:       size,
						Side:       t.Side,
						TradeTime:  tradeTime,
						ReceivedAt: receivedAt,
					}
				}
			}
		}
	}()

	return ch, nil
}

func (c *client) Close() error {
	return c.conn.Close()
}
