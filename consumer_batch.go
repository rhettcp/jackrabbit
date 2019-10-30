package jackrabbit

import (
	"github.com/streadway/amqp"
)

// RabbitBatchConsumer is a struct that handles all
//   rabbitmq consuming needs
type RabbitBatchConsumer struct {
	rc        *RabbitConnection
	AutoAck   bool
	BatchSize int
	Queue     QueueDetails
	close     chan interface{}
}

// RabbitMessage wraps the message with the channel to allow
//   for acking and nacking
type RabbitMessage struct {
	AckFunc  func() error
	NackFunc func(bool) error
	Delivery *amqp.Delivery
}

// NewRabbitBatchConsumer creates a new RabbitPublisher
func NewRabbitBatchConsumer(rabbitConnection *RabbitConnection, autoAck bool, batchSize int, queue QueueDetails) (*RabbitBatchConsumer, error) {
	rb := &RabbitBatchConsumer{
		rc:        rabbitConnection,
		AutoAck:   autoAck,
		BatchSize: batchSize,
		Queue:     queue,
		close:     make(chan interface{}),
	}

	return rb, nil
}

// GetBatch gets the batch size
func (c *RabbitBatchConsumer) GetBatch() ([]RabbitMessage, error) {
	c.rc.connChanLock.Lock()
	defer c.rc.connChanLock.Unlock()
	var batch []RabbitMessage
	for i := 0; i < c.BatchSize; i++ {
		d, ok, err := c.rc.channel.Get(c.Queue.Name, c.AutoAck)
		if err != nil {
			return batch, err
		}
		if !ok {
			return batch, nil
		}
		ackFunc := func() error {
			return c.rc.channel.Ack(d.DeliveryTag, false)
		}
		nackFunc := func(requeue bool) error {
			return c.rc.channel.Nack(d.DeliveryTag, false, requeue)
		}
		batch = append(batch, RabbitMessage{AckFunc: ackFunc, NackFunc: nackFunc, Delivery: &d})
	}
	return batch, nil
}
