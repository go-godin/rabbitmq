package rabbitmq

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const EnvironmentVariableName = "AMQP_ADDRESS"

type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	URI        string
}

func NewRabbitMQ(connectionString string) *RabbitMQ {
	return &RabbitMQ{
		URI: connectionString,
	}
}

func NewRabbitMQFromEnv() (*RabbitMQ, error) {
	connection := os.Getenv(EnvironmentVariableName)
	if connection == "" {
		return nil, fmt.Errorf("missing AMQP connection string, set %s env variable", EnvironmentVariableName)
	}
	return NewRabbitMQ(connection), nil
}

func (r *RabbitMQ) Connect() (err error) {
	r.Connection, err = amqp.Dial(r.URI)
	if err != nil {
		return errors.Wrap(err, "failed to connect to RabbitMQ")
	}
	return nil
}

func (r *RabbitMQ) NewChannel() (err error) {
	if r.Connection == nil {
		return fmt.Errorf("cannot create channel without a connection")
	}
	r.Channel, err = r.Connection.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open AMQP channel")
	}
	return nil
}

func (r *RabbitMQ) DeclareExchange(name, typ string, durable, autoDelete, internal, noWait bool) (err error) {
	if err := r.Channel.ExchangeDeclare(name, typ, durable, autoDelete, internal, noWait, nil); err != nil {
		return errors.Wrap(err, "failed to delcare RabbitMQ exchange")
	}
	return nil
}
