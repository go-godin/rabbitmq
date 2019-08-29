package rabbitmq

import (
	"github.com/assembla/cony"
)

const DefaultConsumePrefetchCount = 10

type Consumer struct {
	Consumer *cony.Consumer
}

func NewConsumer(client *cony.Client, exchangeName string, queueName string, topic string, opt ...cony.ConsumerOpt) *Consumer {
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
		Key:      topic,
	}

	client.Declare([]cony.Declaration{
		cony.DeclareExchange(exchange),
		cony.DeclareQueue(queue),
		cony.DeclareBinding(binding),
	})

	opt = append(opt, cony.Tag(topic))

	consumer := cony.NewConsumer(queue, opt...)
	client.Consume(consumer)

	return &Consumer{Consumer:consumer}
}
