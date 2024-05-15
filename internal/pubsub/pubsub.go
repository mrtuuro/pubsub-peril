package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	ctx := context.Background()
	data, err := json.Marshal(val)
	if err != nil {
		log.Fatalf("error encoding val to byte: %b", err)
	}

	amqpPublishData := amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}
	if err := ch.PublishWithContext(ctx, exchange, key, false, false, amqpPublishData); err != nil {
		log.Fatalf("error publishing content: %v", err)
	}

	return nil
}

type SimpleQueueType int

const (
	DURABLE SimpleQueueType = iota
	TRANSIENT
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("error creating ch: %v", err)
		return nil, amqp.Queue{}, err
	}

	durable := false
	autoDelete := false
	exclusive := false
	noWait := false
	if simpleQueueType == DURABLE {
		durable = true
	}

	if simpleQueueType == TRANSIENT {
		autoDelete = true
		exclusive = true
	}

	que, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		log.Fatalf("error creating queue: %v", err)
		return nil, amqp.Queue{}, err
	}
	if err = ch.QueueBind(queueName, key, exchange, noWait, nil); err != nil {
		log.Fatalf("error bindin queue to queue: %v", err)
		return nil, amqp.Queue{}, err
	}

	return ch, que, nil

}
