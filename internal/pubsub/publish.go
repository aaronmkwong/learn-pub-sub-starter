package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// SimpleQueueType identifies whether a queue should be durable or transient.
type SimpleQueueType string

const (
	// Durable queues survive RabbitMQ restarts.
	Durable SimpleQueueType = "durable"

	// Transient queues are temporary and are deleted when the connection closes.
	Transient SimpleQueueType = "transient"
)

// AckType determines how a consumed message should be acknowledged.
type AckType string

const (
	// Ack acknowledges the message as successfully processed.
	Ack AckType = "ack"

	// NackRequeue negatively acknowledges the message and puts it back
	// on the queue so it can be processed again.
	NackRequeue AckType = "nack_requeue"

	// NackDiscard negatively acknowledges the message and discards it.
	NackDiscard AckType = "nack_discard"
)

// PublishJSON converts a Go value to JSON and publishes it to RabbitMQ.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// Convert the Go value into JSON bytes.
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// Publish the JSON bytes to the specified exchange using the routing key.
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory: don't return the message if no queue matches
		false, // immediate: not supported by RabbitMQ
		amqp.Publishing{
			// Tell the consumer that the message body contains JSON.
			ContentType: "application/json",

			// The JSON-encoded message.
			Body: body,
		},
	)
}

// PublishGob converts a Go value to gob-encoded bytes and publishes it
// to RabbitMQ.
func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// Create an in-memory buffer to hold the gob-encoded data.
	var buf bytes.Buffer

	// Create a gob encoder that writes into the buffer.
	encoder := gob.NewEncoder(&buf)

	// Encode the Go value into gob format.
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	// Publish the gob-encoded bytes to RabbitMQ.
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory: don't return the message if no queue matches
		false, // immediate: not supported by RabbitMQ
		amqp.Publishing{
			// Tell the consumer that the message body contains gob data.
			ContentType: "application/gob",

			// The gob-encoded message.
			Body: buf.Bytes(),
		},
	)
}

// DeclareAndBind creates a RabbitMQ channel, declares a queue,
// and binds the queue to an exchange using the provided routing key.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	// Create a new channel on the RabbitMQ connection.
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// Determine the queue settings from the queue type.
	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := queueType == Transient

	// Configure the queue to send rejected/discarded messages
	// to the dead letter exchange.
	queueArgs := amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
	}

	// Declare the queue with the appropriate durability/lifetime settings
	// and the dead letter exchange configuration.
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,     // no-wait: wait for RabbitMQ's response
		queueArgs, // queue arguments, including the dead letter exchange
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, err
	}

	// Bind the queue to the exchange.
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

// SubscribeJSON creates a queue subscription and handles incoming
// JSON messages by passing the decoded value to the provided handler.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		defer ch.Close()

		for msg := range deliveries {
			var val T

			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				log.Println("Malformed JSON; discarding message:", err)

				if err := msg.Nack(false, false); err != nil {
					log.Println("Failed to nack malformed message:", err)
				}

				continue
			}

			ack := handler(val)

			switch ack {
			case Ack:
				log.Println("Message acknowledged")

				if err := msg.Ack(false); err != nil {
					log.Println("Failed to acknowledge message:", err)
				}

			case NackRequeue:
				log.Println("Message negatively acknowledged and requeued")

				if err := msg.Nack(false, true); err != nil {
					log.Println("Failed to nack and requeue message:", err)
				}

			case NackDiscard:
				log.Println("Message negatively acknowledged and discarded")

				if err := msg.Nack(false, false); err != nil {
					log.Println("Failed to nack and discard message:", err)
				}

			default:
				log.Printf(
					"Unexpected acktype %q; discarding message",
					ack,
				)

				if err := msg.Nack(false, false); err != nil {
					log.Println("Failed to nack unexpected acktype message:", err)
				}
			}
		}
	}()

	return nil
}