package main

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"time"

	pb "bitbucket.org/jdbergmann/protobuf-go/contact/contact"

	"github.com/go-godin/log"

	"github.com/assembla/cony"
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

	// setup AMQP connections
	produceConnection := cony.NewClient(cony.URL(DSN))
	consumeConnection := cony.NewClient(cony.URL(DSN))
	defer produceConnection.Close()
	defer consumeConnection.Close()

	// setup consumers
	someTopicConsumer = rabbitmq.NewConsumer(consumeConnection, SomeExchangeName, ServiceQueueName, SomeTopic)
	logger.Info(fmt.Sprintf("subscribed to topic '%s'", SomeTopic))
	go consumeAMQP(consumeConnection)

	// setup producers
	someTopicProducer = rabbitmq.NewProducer(produceConnection, SomeExchangeName, SomeTopic)
	produceConnection.Publish(someTopicProducer.Publisher)
	go produceAMQP(produceConnection)

	// demo producer
	go func() {
		for range time.Tick(1000 * time.Millisecond) {
			logger.Info("PRODUCE")
			err := someTopicProducer.Publish(&pb.Contact{Name:"HANS PETER"})
			logger.Info("PRODUCED")
			if err != nil {
				logger.Error("FAILED TO PUBLISH", "err", err)
			}
		}
	}()

	ctx, _ := context.WithCancel(context.Background())
	<-ctx.Done()
}

func consumeAMQP(connection *cony.Client) {
	for connection.Loop() {
		select {
		// some.topic
		case msg := <-someTopicConsumer.Consumer.Deliveries():
			evt := &pb.Contact{}

			if err := proto.Unmarshal(msg.Body, evt); err != nil {
				logger.Error("", "during", "proto.Unmarshal", "err", err)
				msg.Ack(false) // drop event
			}

			logger.Info("unmarshalled", "evt", evt)

			msg.Ack(false)
		case err := <-someTopicConsumer.Consumer.Errors():
			logger.Error("consumer error", "err", err, "topic", SomeTopic)

		case err := <-connection.Errors():
			logger.Error("AMQP consumer connection error", "err", err)
		}
	}
}

func produceAMQP(connection *cony.Client) {
	for connection.Loop() {
		select {
		case err := <-connection.Errors():
			logger.Error("AMQP producer connection error", "err", err)
		}
	}
}
