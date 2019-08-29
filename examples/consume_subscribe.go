package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-godin/log"

	amqp "github.com/assembla/cony"
	"github.com/go-godin/rabbitmq"
)

const SomeExchangeName = "example-exchange"
const ServiceQueueName = "service-name-queue"
const SomeTopic = "some.topic"
const DSN = "amqp://user:pass@host/vhost"

var logger log.Logger
var someTopicConsumer *rabbitmq.Consumer
var someTopicProducer *rabbitmq.Producer

func main() {

	logger = log.NewLoggerFromEnv()

	// AMQP consumers
	{
		consumeConnection := amqp.NewClient(amqp.URL(DSN))
		defer consumeConnection.Close()

		// some.topic
		someTopicConsumer = rabbitmq.NewConsumer(consumeConnection, SomeExchangeName, ServiceQueueName, SomeTopic)
		logger.Info(fmt.Sprintf("subscribed to topic '%s'", SomeTopic))

		go consumeAMQP(consumeConnection)
	}

	// AMQP producers
	{
		produceConnection := amqp.NewClient(amqp.URL(DSN))
		defer produceConnection.Close()

		someTopicProducer = rabbitmq.NewProducer(produceConnection, SomeExchangeName, SomeTopic)
		produceConnection.Publish(someTopicProducer.Publisher)

		go produceAMQP(produceConnection)
	}

	// demo producer
	go func() {
		for range time.Tick(1000 * time.Millisecond) {
			logger.Info("PRODUCE")
			err := someTopicProducer.Publish("foo")
			logger.Info("PRODUCED")
			if err != nil {
				logger.Error("FAILED TO PUBLISH", "err", err)
			}
		}
	}()

	ctx, _ := context.WithCancel(context.Background())
	<-ctx.Done()
}

// consumeAMQP runs the main connection loop for the consuming connection, handles errors and sends
// the consumed messages to the target handlers of your domain logic.
func consumeAMQP(connection *amqp.Client) {
	for connection.Loop() {
		select {

		// some.topic deliveries
		case msg := <-someTopicConsumer.Consumer.Deliveries():
			// TODO call subscription handler
			_ = msg.Ack(false)

		// some.topic errors
		case err := <-someTopicConsumer.Consumer.Errors():
			logger.Error("consumer error", "err", err, "topic", SomeTopic)

		// connection errors
		case err := <-connection.Errors():
			logger.Error("AMQP consumer connection error", "err", err)
			break
		}
	}
}

// produceAMQP runs the main connection loop for the producing connection and handles errors
func produceAMQP(connection *amqp.Client) {
	for connection.Loop() {
		select {

		// connection errors
		case err := <-connection.Errors():
			logger.Error("AMQP producer connection error", "err", err)
		}
	}
}
