package config

import (
	"github.com/Karzoug/meower-common-go/metric/prom"
	"github.com/Karzoug/meower-common-go/mongo"
	"github.com/rs/zerolog"

	"github.com/Karzoug/meower-post-outbox/internal/kafka"
)

type Config struct {
	LogLevel zerolog.Level     `env:"LOG_LEVEL" envDefault:"info"`
	PromHTTP prom.ServerConfig `envPrefix:"PROM_"`
	Mongo    mongo.Config      `envPrefix:"MONGO_"`
	Kafka    kafka.Config      `envPrefix:"KAFKA_"`
}
