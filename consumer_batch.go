package jackrabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitBatchConsumer is a struct that handles all
//
//	rabbitmq consuming needs
type RabbitBatchConsumer struct {
	rc        *RabbitConnection
	BatchSize int
	Queue     QueueDetails
	close     chan interface{}
}

// NewRabbitBatchConsumer creates a new RabbitBatchConsumer
func NewRabbitBatchConsumer(rabbitConnection *RabbitConnection, batchSize int, queue QueueDetails) (*RabbitBatchConsumer, error) {
	rb := &RabbitBatchConsumer{
		rc:        rabbitConnection,
		BatchSize: batchSize,
		Queue:     queue,
		close:     make(chan interface{}),
	}
	return rb, nil
}

// GetBatch gets the batch size
func (c *RabbitBatchConsumer) GetBatch() ([]*amqp.Delivery, error) {
	c.rc.connChanLock.Lock()
	defer c.rc.connChanLock.Unlock()
	var batch []*amqp.Delivery
	for i := 0; i < c.BatchSize; i++ {
		d, ok, err := c.rc.channel.Get(c.Queue.Name, true)
		if err != nil {
			return batch, err
		}
		if !ok {
			return batch, nil
		}
		batch = append(batch, &d)
	}
	return batch, nil
}
