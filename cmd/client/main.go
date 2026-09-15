package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// handlerPause returns a handler function that processes pause messages.
// Pause messages should always be acknowledged.
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(state)

		return pubsub.Ack
	}
}

// handlerMove returns a handler function that processes move messages.
// Safe moves and moves that make war are acknowledged.
// Moves involving the same player or any other outcome are discarded.
func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)

		if outcome == gamelogic.MoveOutComeSafe ||
			outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	// Create one channel for publishing moves.
	// Reuse it for every move instead of creating a new channel each time.
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open RabbitMQ channel:", err)
		return
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to get username:", err)
		return
	}

	gamestate := gamelogic.NewGameState(username)

	// Subscribe to pause/resume messages for this client.
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

	// Subscribe to move messages for this client.
	// The wildcard binding allows the client to receive moves
	// published to army_moves.<any-username>.
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

			// Publish the move using the concrete username routing key.
			moveKey := routing.ArmyMovesPrefix + "." + username

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
