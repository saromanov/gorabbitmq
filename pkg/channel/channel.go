package channel

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

type Channel interface {
	Publish(ctx context.Context) error
}

type channel struct {
	pubCh    *amqp.Channel
	exchange string
}

func (c *channel) Publish(ctx context.Context) error {
	if c.pubCh == nil {
		return fmt.Errorf("unable to publish to channel")
	}

	err := c.pubCh.Publish(c.exchange, "", false, false, amqp.Publishing{})
	if err != nil {
		return err
	}

	return nil
}
