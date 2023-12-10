package jackrabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeDetails carries the specifics of an exchange
type ExchangeDetails struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	NoWait     bool
	Args       amqp.Table
	IfUsed     bool
}

// Create creates the underlying exchange
func (e *ExchangeDetails) Create(channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		e.Name,
		e.Kind,
		e.Durable,
		e.AutoDelete,
		false,
		e.NoWait,
		e.Args,
	)
}

// Delete deletes the underlying exchange
func (e *ExchangeDetails) Delete(channel *amqp.Channel) error {
	return channel.ExchangeDelete(
		e.Name,
		e.IfUsed,
		e.NoWait,
	)
}
