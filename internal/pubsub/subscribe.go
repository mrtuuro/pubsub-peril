package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType, handler func(T)) error {

    channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
    if err != nil {
        log.Fatalf("error declarin and binding: %v", err)
        return err
        
    }

    msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
    if err != nil {
        log.Fatalf("error consuming: %v", err)
        return err
    }

    unmarshaller := func(data []byte) (T, error ) {
        var target T
        err := json.Unmarshal(data, &target)
        return target, err
    }

    go func() {
        defer channel.Close()
        for msg := range msgs {

            target, err := unmarshaller(msg.Body)
            if err != nil {
                log.Fatalf("could not unmarshal messages: %v\n", err)
                continue
            }
            handler(target)
            msg.Ack(false)
        }
    }()
    
    return nil
}
