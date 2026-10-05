package main

import (
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// publishGameLog creates and publishes a GameLog using gob encoding.
func publishGameLog(
	ch *amqp.Channel,
	username string,
	message string,
) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	}

	// Game logs use the game_logs.<username> routing key.
	logKey := routing.GameLogSlug + "." + username

	return pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		logKey,
		gameLog,
	)
}

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

			return pubsub.Ack
		}

		// Moves involving the same player or any other outcome
		// should be discarded.
		return pubsub.NackDiscard
	}
}

// handlerWar processes war recognition messages.
// All clients consume from the shared "war" queue.
func handlerWar(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		// HandleWar returns the outcome, winner, and loser.
		// Winner and loser are already strings containing player names.
		outcome, winner, loser := gs.HandleWar(war)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			// This client is not involved in the war, so let another
			// client try to process the message.
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			// The war cannot be processed because there are no units.
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon,
			gamelogic.WarOutcomeYouWon:

			message := fmt.Sprintf(
				"%s won a war against %s",
				winner,
				loser,
			)

			// Use the attacker's username because that player
			// initiated the war.
			err := publishGameLog(
				ch,
				war.Attacker.Username,
				message,
			)
			if err != nil {
				fmt.Println("Failed to publish game log:", err)

				// Requeue the war because the game log was not
				// successfully published.
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf(
				"A war between %s and %s resulted in a draw",
				winner,
				loser,
			)

			// Use the attacker's username because that player
			// initiated the war.
			err := publishGameLog(
				ch,
				war.Attacker.Username,
				message,
			)
			if err != nil {
				fmt.Println("Failed to publish game log:", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		default:
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

	// Create one channel for publishing moves, war recognitions,
	// and game logs.
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
	// All clients share the durable "war" queue.
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gamestate, ch),
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
			if len(words) < 2 {
				fmt.Println("Usage: spam <count>")
				continue
			}

			count, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("Invalid spam count:", words[1])
				continue
			}

			if count < 1 {
				fmt.Println("Spam count must be greater than 0")
				continue
			}

			successful := 0

			for i := 0; i < count; i++ {
				message := gamelogic.GetMaliciousLog()

				err := publishGameLog(
					ch,
					username,
					message,
				)
				if err != nil {
					fmt.Println("Failed to publish spam log:", err)
					break
				}

				successful++
			}

			if successful == count {
				fmt.Printf(
					"Successfully published %d malicious logs\n",
					successful,
				)
			} else {
				fmt.Printf(
					"Successfully published %d of %d malicious logs\n",
					successful,
					count,
				)
			}

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command:", words[0])
		}
	}
}
