package jackrabbit

import (
	"github.com/streadway/amqp"
)

// RabbitPublisher is a struct that handles all
//   rabbitmq publishing needs
type RabbitPublisher struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

// NewRabbitPublisher creates a new RabbitPublisher
func NewRabbitPublisher(rabbitURL string) (*RabbitPublisher, error) {
	return nil, nil
}
