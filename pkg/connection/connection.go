package connection

import (
	"context"
	"fmt"
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
	subscribersM     sync.RWMutex
	subscribers      map[string]subscriber
	lastErr          error
	lastErrM         sync.Mutex
}

// Run provides running of connection
func (c *Connection) Run(ctx context.Context) error {
	return c.maintainConnection(ctx)
}
// AddSubscription provides adding of the new subscription
func (c *Connection) AddSubscribtion(ctx context.Context, queueName string, s subscription) error {
	err := c.addSubscription(ctx, queueName, s)
	c.lastErrM.Lock()
	c.lastErr = err
	c.lastErrM.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) addSubscription(ctx context.Context, queueName string, s subscription) error {
	c.subscribersM.Lock()
	defer c.subscribersM.Unlock()
	if _, exists := c.subscribers[queueName]; exists {
		return fmt.Errorf("subscriber already exists")
	}
	c.subscribers[queueName] = s
	return nil
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
		case <-ctx.Done():
			return ctx.Err()
		}

	}
}

// private method for open connection
func (c *Connection) open(ctx context.Context) error {
	conn, err := amqp.Dial(c.address)
	if err != nil {
		return err
	}

	c.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	ctx, c.cancelFunc = context.WithCancel(ctx)
	errorChan := make(chan *amqp.Error)
	ch.NotifyClose(errorChan)
	return nil
}
