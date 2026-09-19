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
// The AMQP channel is used to publish a war recognition message when
// a move results in war.
func handlerMove(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
	username string,
) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)

		// A safe move has been handled successfully.
		if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		}

		// A move that makes war needs to publish a war recognition
		// message so another client can handle the war.
		if outcome == gamelogic.MoveOutcomeMakeWar {
			warKey := routing.WarRecognitionsPrefix + "." + username

			war := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				warKey,
				war,
			)
			if err != nil {
				fmt.Println("Failed to publish war recognition:", err)

				// Requeue the move because the war declaration
				// was not successfully published.
				return pubsub.NackRequeue
			}

			// The move was successfully processed and the war
			// declaration was successfully published.
			return pubsub.Ack
		}

		// Moves involving the same player or any other outcome
		// should be discarded.
		return pubsub.NackDiscard
	}
}

// handlerWar returns a handler function that processes war recognition
// messages. All clients consume from the shared "war" queue.
// A client not involved in the war requeues the message so another
// client can try to process it.
func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		// HandleWar returns the outcome as well as the winner and loser.
		// The winner and loser are not needed by this handler.
		outcome, _, _ := gs.HandleWar(war)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			// This client is not involved in the war, so put the
			// message back on the shared queue for another client.
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			// The war cannot be processed because there are no units.
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon:
			// The war was successfully resolved.
			return pubsub.Ack

		case gamelogic.WarOutcomeYouWon:
			// The war was successfully resolved.
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			// The war was successfully resolved.
			return pubsub.Ack

		default:
			// An unexpected outcome should not be retried.
			fmt.Println("Error: unexpected war outcome:", outcome)
			return pubsub.NackDiscard
		}
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

	// Create one channel for publishing moves and war recognitions.
	// Reuse it instead of creating a new channel for every publish.
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
		handlerMove(gamestate, ch, username),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to move messages:", err)
		return
	}

	// Subscribe to war recognition messages.
	// All clients share the durable "war" queue, so only one client
	// consumes each war message at a time.
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gamestate),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to war messages:", err)
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
