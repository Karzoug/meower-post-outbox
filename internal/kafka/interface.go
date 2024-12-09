package kafka

import "context"

type kafkaRecorder interface {
	IncKafkaClientProduceCounter(ctx context.Context, topic string)
	IncKafkaProducerSuccessCounter(ctx context.Context, topic string)
	IncKafkaProducerErrorCounter(ctx context.Context, topic string)
}
