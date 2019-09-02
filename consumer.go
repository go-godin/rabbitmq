package rabbitmq

import (
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

type ConsumeHandler func(delivery amqp.Delivery)

type Consumer struct {
	Consumer   *cony.Consumer
	Handler    ConsumeHandler
	RoutingKey string
}

func NewConsumer(client *cony.Client, exchangeName string, queueName string, routingKey string, handler ConsumeHandler, opt ...cony.ConsumerOpt) *Consumer {
	exchange := cony.Exchange{
		Name:       exchangeName,
		Kind:       "topic",
		Durable:    true,
		AutoDelete: false,
		Args:       nil,
	}

	queue := &cony.Queue{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	}

	binding := cony.Binding{
		Queue:    queue,
		Exchange: exchange,
		Key:      routingKey,
	}

	client.Declare([]cony.Declaration{
		cony.DeclareExchange(exchange),
		cony.DeclareQueue(queue),
		cony.DeclareBinding(binding),
	})

	opt = append(opt, cony.Tag(routingKey))

	consumer := cony.NewConsumer(queue, opt...)
	client.Consume(consumer)

	return &Consumer{
		Consumer:   consumer,
		Handler:    handler,
		RoutingKey: routingKey,
	}
}

// Handle will filter the passed amqp.Delivery based on the routing key
// Only the configured routing key is handled, all others are NACKed and requeued.
// If the delivery has the correct routing key, the handler is called.
func (c *Consumer) Handle(msg amqp.Delivery) {
	if msg.RoutingKey != c.RoutingKey {
		_ = msg.Nack(false, true)
	}
	c.Handler(msg)
}
