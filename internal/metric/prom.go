package metric

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type kafkaRecorder struct {
	clientProduceCounter   metric.Int64Counter
	producerSuccessCounter metric.Int64Counter
	producerErrorCounter   metric.Int64Counter
}

// NewKafkaRecorder returns a kafka recorder that is used to send metrics.
func NewKafkaRecorder(meter metric.Meter) (kafkaRecorder, error) {
	kr := kafkaRecorder{}

	var err error
	kr.clientProduceCounter, err = meter.Int64Counter(
		"client_produce_total",
		metric.WithDescription("This represent the number of messages pushed by Kafka client"),
	)
	if err != nil {
		return kafkaRecorder{}, err
	}

	kr.producerSuccessCounter, err = meter.Int64Counter(
		"producer_event_success_total",
		metric.WithDescription("This represent the number of successful messages pushed into Kafka"),
	)
	if err != nil {
		return kafkaRecorder{}, err
	}

	kr.producerErrorCounter, err = meter.Int64Counter(
		"producer_event_error_total",
		metric.WithDescription("This represent the number of error messages handled by Kafka producer"),
	)
	if err != nil {
		return kafkaRecorder{}, err
	}

	return kr, nil
}

// IncKafkaClientProduceCounter increments the client produce counter.
func (r kafkaRecorder) IncKafkaClientProduceCounter(ctx context.Context, topic string) {
	r.clientProduceCounter.Add(ctx,
		1,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// IncKafkaProducerSuccessCounter increments the producer success counter.
func (r kafkaRecorder) IncKafkaProducerSuccessCounter(ctx context.Context, topic string) {
	r.producerSuccessCounter.Add(ctx,
		1,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// IncKafkaProducerErrorCounter increments the producer error counter.
func (r kafkaRecorder) IncKafkaProducerErrorCounter(ctx context.Context, topic string) {
	r.producerErrorCounter.Add(ctx,
		1,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}
