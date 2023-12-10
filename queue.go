package jackrabbit

import (
	"github.com/streadway/amqp"
)

// QueueDetails carries the specifics of a queue
type QueueDetails struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
	Bindings   []Binding
	IfUsed     bool
	IfEmpty    bool
}

type Binding struct {
	Name     string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

// Create creates the underlying queue with bindings
//
//	If any binding fails, queue is deleted
func (q *QueueDetails) Create(channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	if err != nil {
		return err
	}
	for _, b := range q.Bindings {
		err := channel.QueueBind(
			b.Name,
			b.Key,
			b.Exchange,
			b.NoWait,
			b.Args,
		)
		if err != nil {
			channel.QueueDelete(
				q.Name,
				false,
				false,
				true,
			)
			return err
		}
	}
	return nil
}

// Delete deletes the underlying queue
func (q *QueueDetails) Delete(channel *amqp.Channel) error {
	_, err := channel.QueueDelete(
		q.Name,
		q.IfUsed,
		q.IfEmpty,
		q.NoWait,
	)
	return err
}

// Inspect inspects the underlying queue
func (q *QueueDetails) Inspect(channel *amqp.Channel) (amqp.Queue, error) {
	return channel.QueueInspect(q.Name)
}
