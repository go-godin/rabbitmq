package rabbitmq

import (
	"github.com/assembla/cony"
	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
)

type Producer struct {
	Publisher *cony.Publisher
}

func NewProducer(client *cony.Client, exchangeName string, topic string, opts ...cony.PublisherOpt) *Producer {
	exchange := cony.Exchange{
		Name:       exchangeName,
		Kind:       "topic",
		Durable:    true,
		AutoDelete: false,
		Args:       nil,
	}

	opts = append(opts)

	client.Declare([]cony.Declaration{
		cony.DeclareExchange(exchange),
	})

	publisher := cony.NewPublisher(exchange.Name, topic, opts...)

	return &Producer{Publisher:publisher}
}

func (p *Producer) Publish(event interface{}) error  {
	protobuf := event.(proto.Message)
	bodyBytes, err := proto.Marshal(protobuf)
	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		Headers:      amqp.Table{},
		ContentType:  "application/octet-stream",
		DeliveryMode: amqp.Transient,
		Priority:     0,
		Body:         bodyBytes,
	}

	return p.Publisher.Publish(publishing)
}
