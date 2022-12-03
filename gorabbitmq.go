package gorabbitmq

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	address          string
	consumerID       string
	prefetchCount    int
	prefetchSize     int
	reconnectTimeout time.Duration
	conn             *amqp.Connection
	cancelFunc       context.CancelFunc
}

type Config struct {
	Address          string
	ConsumerID       string
	PrefetchCount    int
	PrefetchSize     int
	ReconnectTimeout time.Duration
}

func New(cfg Config) *RabbitMQ {
	return &RabbitMQ{
		address:          cfg.Address,
		consumerID:       cfg.ConsumerID,
		prefetchCount:    cfg.PrefetchCount,
		prefetchSize:     cfg.PrefetchSize,
		reconnectTimeout: cfg.ReconnectTimeout,
	}
}

// Open provides opening of connection
func (r *RabbitMQ) Open(ctx context.Context) error {
	return r.open(ctx)
}

// private method for open connection
func (r *RabbitMQ) open(ctx context.Context) error {
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

	return nil
}
