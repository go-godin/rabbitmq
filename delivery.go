package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Delivery struct {
	amqp.Delivery
}

func (d Delivery) NackDelivery(multiple, requeue bool) error {
	var requeueVal string
	if requeue {
		requeueVal = "1"
	} else {
		requeueVal = "0"
	}

	nackCounter.With("routing_key", d.RoutingKey, "requeue", requeueVal).Add(1)
	return d.Nack(multiple, requeue)
}
