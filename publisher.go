package jackrabbit

import (
	"github.com/streadway/amqp"
)

// RabbitPublisher is a struct that handles all
//   rabbitmq publishing needs
type RabbitPublisher struct {
	rc *RabbitConnection
}

// NewRabbitPublisher creates a new RabbitPublisher
func NewRabbitPublisher(rabbitConnection *RabbitConnection) (*RabbitPublisher, error) {
	return &RabbitPublisher{rc: rabbitConnection}, nil
}

//Publish publishes data to the exchange with the routingKey
func (p *RabbitPublisher) Publish(exchange ExchangeDetails, routingKey string, data amqp.Publishing) error {
	return p.rc.channel.Publish(
		exchange.Name,
		routingKey,
		false,
		false,
		data,
	)
}

//PublishSimple publishes data to the exchange with the routingKey
func (p *RabbitPublisher) PublishSimple(exchange ExchangeDetails, routingKey string, data string) error {
	return p.rc.channel.Publish(
		exchange.Name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
		},
	)
}
