package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// handlerPause returns a handler function that processes pause messages
// received from RabbitMQ.
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		// Display a new prompt when the pause handler finishes.
		defer fmt.Print("> ")

		// Update the game state based on the pause/resume message.
		gs.HandlePause(state)
	}
}

func main() {
	fmt.Println("Starting Peril client...")

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

	// Get the client's username.
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to get username:", err)
		return
	}

	// Create a new game state for this user.
	gamestate := gamelogic.NewGameState(username)

	// Subscribe to pause messages for this client.
	// SubscribeJSON creates and binds the transient queue and calls
	// handlerPause whenever a new PlayingState message is received.
	queueName := routing.PauseKey + "." + username

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gamestate),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to pause messages:", err)
		return
	}

	// Start the client REPL.
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gamestate.CommandSpawn(words)
			if err != nil {
				fmt.Println("Error spawning unit:", err)
				continue
			}

		case "move":
			result, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Println("Error moving unit:", err)
				continue
			}

			fmt.Println(result)

		case "status":
			gamestate.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command:", words[0])
		}
	}
}
