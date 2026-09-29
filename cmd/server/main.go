package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// handlerGameLog returns a handler function that processes game log messages.
// Each received log is written to disk.
func handlerGameLog(gameLog routing.GameLog) pubsub.AckType {
	// Display a new prompt when the handler finishes.
	defer fmt.Print("> ")

	// Write the received game log to disk.
	err := gamelogic.WriteLog(gameLog)
	if err != nil {
		fmt.Println("Failed to write game log:", err)

		// Requeue the message so the server can try again.
		return pubsub.NackRequeue
	}

	// The log was successfully written.
	return pubsub.Ack
}

func main() {
	fmt.Println("Starting Peril server...")

	// Show the available server commands.
	gamelogic.PrintServerHelp()

	// Connection string for the local RabbitMQ server.
	connString := "amqp://guest:guest@localhost:5672/"

	// Connect to RabbitMQ.
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	// Subscribe to game log messages.
	// The durable game_logs queue is shared by the server and uses a
	// wildcard routing key so it receives logs from every client.
	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handlerGameLog,
	)
	if err != nil {
		fmt.Println("Failed to subscribe to game logs:", err)
		return
	}

	// Create a RabbitMQ channel for publishing pause/resume messages.
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	defer ch.Close()

	// Start the server REPL.
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
			}

		case "resume":
			fmt.Println("Sending resume message...")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
			}

		case "quit":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("I don't understand that command.")
		}
	}
}