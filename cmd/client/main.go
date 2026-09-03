package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

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

	// Declare and bind a transient queue for this client.
	queueName := routing.PauseKey + "." + username

	ch, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	defer ch.Close()

	// Wait for Ctrl+C (SIGINT) or another termination signal.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	fmt.Println("Shutting down Peril client...")
}