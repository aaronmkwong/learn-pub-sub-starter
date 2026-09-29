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

	// Publish the JSON bytes to RabbitMQ using the routing key.
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory: don't return the message if no queue matches
		false, // immediate: not supported by RabbitMQ
		amqp.Publishing{
			// Identify the message body as JSON.
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
			// Identify the message body as gob data.
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
	// and dead-letter exchange configuration.
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,     // no-wait: wait for RabbitMQ's response
		queueArgs, // queue arguments
	)
	if err != nil {
		// Close the channel because queue creation failed.
		ch.Close()

		return nil, amqp.Queue{}, err
	}

	// Bind the queue to the exchange using the supplied routing key.
	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // no-wait: wait for RabbitMQ's response
		nil,   // no additional arguments
	)
	if err != nil {
		// Close the channel because binding failed.
		ch.Close()

		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

// subscribe contains the shared subscription logic used by both
// SubscribeJSON and SubscribeGob.
func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	// Declare and bind the queue, then use the returned channel
	// to consume messages.
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
		"",         // let RabbitMQ generate the consumer name
		false,      // auto-ack: false so we manually acknowledge messages
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

	// Process messages in the background so the subscription does not
	// block the rest of the application.
	go func() {
		// Close the channel when the delivery loop ends.
		defer ch.Close()

		// Continue processing messages until RabbitMQ closes
		// the deliveries channel.
		for msg := range deliveries {
			// Decode the message body using the unmarshalling function
			// supplied by SubscribeJSON or SubscribeGob.
			val, err := unmarshaller(msg.Body)
			if err != nil {
				// The message cannot be processed, so discard it rather
				// than leaving it unacknowledged and causing redelivery.
				log.Println("Failed to decode message; discarding:", err)

				if err := msg.Nack(false, false); err != nil {
					log.Println("Failed to nack malformed message:", err)
				}

				continue
			}

			// Let the handler process the decoded message and decide
			// how RabbitMQ should handle it afterward.
			ack := handler(val)

			switch ack {
			case Ack:
				// The message was successfully processed.
				log.Println("Message acknowledged")

				if err := msg.Ack(false); err != nil {
					log.Println("Failed to acknowledge message:", err)
				}

			case NackRequeue:
				// The message should be returned to the queue so
				// another attempt can be made.
				log.Println("Message negatively acknowledged and requeued")

				if err := msg.Nack(false, true); err != nil {
					log.Println("Failed to nack and requeue message:", err)
				}

			case NackDiscard:
				// The message should not be processed again.
				// Because the queue has a dead-letter exchange configured,
				// RabbitMQ can route the rejected message there.
				log.Println("Message negatively acknowledged and discarded")

				if err := msg.Nack(false, false); err != nil {
					log.Println("Failed to nack and discard message:", err)
				}

			default:
				// An unexpected AckType should never leave the message
				// unacknowledged, so safely discard it.
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

// SubscribeJSON creates a queue subscription and decodes incoming
// messages from JSON before passing them to the handler.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	// Use the shared subscriber with JSON unmarshalling.
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var val T

			// Convert the JSON message body into the requested Go type.
			err := json.Unmarshal(data, &val)

			return val, err
		},
	)
}

// SubscribeGob creates a queue subscription and decodes incoming
// messages from gob before passing them to the handler.
func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	// Use the shared subscriber with gob decoding.
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var val T

			// Create a buffer containing the gob-encoded message body.
			buf := bytes.NewBuffer(data)

			// Decode the gob data into the requested Go type.
			decoder := gob.NewDecoder(buf)
			err := decoder.Decode(&val)

			return val, err
		},
	)
}