package jackrabbit

import (
	"github.com/streadway/amqp"
)

// RabbitConsumer is a struct that handles all
//   rabbitmq consuming needs
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
func (c *RabbitConsumer) StartConsumer() error {
	deliveries, err := c.rc.channel.Consume(c.Queue.Name, "", c.AutoAck, c.Queue.Exclusive, false, c.Queue.NoWait, nil)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case d, ok := <-deliveries:
				if ok {
					c.Handler(&d)
				} else {
					close(c.done)
					return
				}
			}
		}
	}()
	return nil
}
