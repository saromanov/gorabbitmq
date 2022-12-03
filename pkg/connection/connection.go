package connection

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Connection defines structure for connection to rabbitmq
type Connection struct {
	address          string
	consumerID       string
	prefetchCount    int
	prefetchSize     int
	reconnectTimeout time.Duration
	conn             *amqp.Connection
	cancelFunc       context.CancelFunc
	connError        error
	connErrorM       sync.Mutex
}

// Run provides running of connection
func (c *Connection) Run(ctx context.Context) error {
	return c.maintainConnection(ctx)
}

// maintainConnection supports maintaining of connection
func (c *Connection) maintainConnection(ctx context.Context) error {
	for {
		err := c.open(ctx)
		if err != nil {
			c.connErrorM.Lock()
			c.connError = err
			c.connErrorM.Unlock()
		}

		select {
		case <- ctx.Done():
			return ctx.Err()
		}

	}
}

// private method for open connection
func (c *Connection) open(ctx context.Context) error {
	conn, err := amqp.Dial(r.address)
	if err != nil {
		return err
	}

	r.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	ctx, r.cancelFunc = context.WithCancel(ctx)
	errorChan := make(chan *amqp.Error)
	ch.NotifyClose(errorChan)
	return nil
}
