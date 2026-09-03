package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := queueType == Transient

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}