package kafka

import (
	"time"
)

type Config struct {
	BootstrapServers  string        `env:"BOOTSTRAP_SERVERS"`
	CloseTimeout      time.Duration `env:"CLOSE_TIMEOUT" envDefault:"10s"`
	EnableIdempotence bool          `env:"ENABLE_IDEMPOTENCE" envDefault:"false"`
}
