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
		// Display a new prompt when the handler finishes.
		defer fmt.Print("> ")

		// Update the game state based on the pause/resume message.
		gs.HandlePause(state)
	}
}

// handlerMove returns a handler function that processes move messages
// received from RabbitMQ.
func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		// Display a new prompt when the handler finishes.
		defer fmt.Print("> ")

		// Update the game state based on the received move.
		gs.HandleMove(move)
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

	// Create one channel for publishing moves.
	// This channel is reused for every publish instead of creating
	// a new channel each time the player moves.
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open RabbitMQ channel:", err)
		return
	}
	defer ch.Close()

	// Get the client's username.
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to get username:", err)
		return
	}

	// Create a new game state for this user.
	gamestate := gamelogic.NewGameState(username)

	// Subscribe to pause messages for this client.
	// The transient queue is bound to the pause routing key.
	pauseQueueName := routing.PauseKey + "." + username

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		pauseQueueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gamestate),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to pause messages:", err)
		return
	}

	// Subscribe to move messages from all players.
	// The queue has a username-specific name, but the binding key
	// uses a wildcard so it receives moves from any player.
	moveQueueName := routing.ArmyMovesPrefix + "." + username

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		moveQueueName,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gamestate),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to move messages:", err)
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

			// Build the concrete routing key for this player's move.
			// Unlike the subscription binding key, this does not use
			// a wildcard because the message belongs to this username.
			moveKey := routing.ArmyMovesPrefix + "." + username

			// Publish the move using the shared channel created above.
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				moveKey,
				result,
			)
			if err != nil {
				fmt.Println("Failed to publish move:", err)
				continue
			}

			fmt.Println("Move published successfully")

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