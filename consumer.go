package jackrabbit

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitConsumer is a struct that handles all
//
//	rabbitmq consuming needs
type RabbitConsumer struct {
	rc      *RabbitConnection
	AutoAck bool
	Handler func(*amqp.Delivery)
	Queue   QueueDetails
	done    chan interface{}
}

// NewRabbitConsumer creates a new RabbitBatchConsumer
func NewRabbitConsumer(rabbitConnection *RabbitConnection, queue QueueDetails) (*RabbitConsumer, error) {
	rb := &RabbitConsumer{
		rc:    rabbitConnection,
		Queue: queue,
		done:  make(chan interface{}),
	}
	return rb, nil
}

// StartConsumer starts the consumer
func (c *RabbitConsumer) StartConsumer(ctx context.Context) error {
	go func() {
		for {
			deliveries, err := c.rc.channel.Consume(c.Queue.Name, "", c.AutoAck, c.Queue.Exclusive, false, c.Queue.NoWait, nil)
			if err != nil {
				return
			}
		DEL_LOOP:
			for {
				select {
				case d, ok := <-deliveries:
					if ok {
						c.Handler(&d)
					} else {
						break DEL_LOOP
					}
				case <-ctx.Done():
					close(c.done)
					return
				case <-c.done:
					return
				}
			}
			t := time.NewTicker(time.Second * 30)
			select {
			case <-t.C:
				log.Debug("Reconnect timer expired, exiting")
			case <-c.rc.reconnectSignal:
				// reconnected, continue
			}
		}
	}()
	return nil
}

func (c *RabbitConsumer) SetMaxUnacked(unackedCount int) error {
	if c.rc.channel != nil {
		return c.rc.channel.Qos(unackedCount, 0, false)
	} else {
		return fmt.Errorf("channel was nil, unable to set unacked max")
	}
}

func (c *RabbitConsumer) StopConsumer() {
	close(c.done)
}
