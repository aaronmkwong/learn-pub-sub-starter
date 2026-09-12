package routing

const (
	// ArmyMovesPrefix is the prefix used for player move routing keys.
	ArmyMovesPrefix = "army_moves"

	// WarRecognitionsPrefix is the prefix used for war recognition messages.
	WarRecognitionsPrefix = "war"

	// PauseKey is the routing key used for pause/resume messages.
	PauseKey = "pause"

	// GameLogSlug is the routing key/queue name used for game logs.
	GameLogSlug = "game_logs"
)

const (
	// ExchangePerilDirect is the direct exchange used for pause/resume messages.
	ExchangePerilDirect = "peril_direct"

	// ExchangePerilTopic is the topic exchange used for player move messages.
	ExchangePerilTopic = "peril_topic"
)
