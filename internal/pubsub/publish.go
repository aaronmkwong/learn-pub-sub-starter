package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType identifies whether a queue should be durable or transient.
type SimpleQueueType string

const (
	// Durable queues survive RabbitMQ restarts.
	Durable SimpleQueueType = "durable"

	// Transient queues are temporary and are deleted when the connection closes.
	Transient SimpleQueueType = "transient"
)

// acktype determines how a consumed message should be acknowledged.
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
	// RabbitMQ messages are sent as []byte.
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

	// Declare the queue with the appropriate durability/lifetime settings.
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false, // no-wait: wait for RabbitMQ's response
		nil,   // no additional arguments
	)
	if err != nil {
		// The channel is no longer needed because queue creation failed.
		ch.Close()

		return nil, amqp.Queue{}, err
	}

	// Bind the queue to the exchange so messages with the routing key
	// will be delivered to this queue.
	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // no-wait: wait for RabbitMQ's response
		nil,   // no additional arguments
	)
	if err != nil {
		// Clean up the channel if binding fails.
		ch.Close()

		return nil, amqp.Queue{}, err
	}

	// Return the channel and queue so the caller can use them.
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
	// Make sure the queue exists and is bound to the exchange.
	// DeclareAndBind also gives us the channel we'll use to consume messages.
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

	// Start consuming messages from the queue.
	deliveries, err := ch.Consume(
		queue.Name, // queue to consume from
		"",         // empty consumer name lets RabbitMQ generate one
		false,      // auto-ack: false, so we manually acknowledge messages
		false,      // exclusive: false
		false,      // no-local: false
		false,      // no-wait: false
		nil,        // no additional arguments
	)
	if err != nil {
		// Consumption failed, so the channel is no longer needed.
		ch.Close()

		return err
	}

	// Process messages in the background so SubscribeJSON can return
	// without blocking the rest of the application.
	go func() {
		// Once the delivery loop ends, close the channel to clean it up.
		defer ch.Close()

		// Continue receiving messages until the deliveries channel closes.
		for msg := range deliveries {
			var val T

			// Convert the raw message body from JSON bytes back into
			// the generic Go type T.
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				// Malformed JSON cannot be processed, so discard it
				// instead of leaving it unacknowledged and allowing
				// RabbitMQ to redeliver it later.
				log.Println("Malformed JSON; discarding message:", err)

				err := msg.Nack(false, false)
				if err != nil {
					log.Println("Failed to nack malformed message:", err)
				}

				continue
			}

			// Let the handler process the message and determine
			// how RabbitMQ should handle the message afterward.
			ack := handler(val)

			switch ack {
			case Ack:
				// Acknowledge successful processing.
				log.Println("Message acknowledged")

				err := msg.Ack(false)
				if err != nil {
					log.Println("Failed to acknowledge message:", err)
				}

			case NackRequeue:
				// Reject the message and put it back on the queue
				// so it can be processed again.
				log.Println("Message negatively acknowledged and requeued")

				err := msg.Nack(false, true)
				if err != nil {
					log.Println("Failed to nack and requeue message:", err)
				}

			case NackDiscard:
				// Reject the message and discard it instead of requeuing.
				log.Println("Message negatively acknowledged and discarded")

				err := msg.Nack(false, false)
				if err != nil {
					log.Println("Failed to nack and discard message:", err)
				}

			default:
				// An unexpected acktype is treated as unsafe to retry.
				// Discard the message rather than leaving it unacknowledged.
				log.Printf(
					"Unexpected acktype %q; discarding message",
					ack,
				)

				err := msg.Nack(false, false)
				if err != nil {
					log.Println("Failed to nack unexpected acktype message:", err)
				}
			}
		}
	}()

	return nil
}
