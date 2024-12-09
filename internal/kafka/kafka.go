package kafka

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/Karzoug/meower-post-outbox/internal/entity"
)

var topicName = "posts"

type eventProducer struct {
	p           *kafka.Producer
	rec         kafkaRecorder
	cfg         Config
	logger      zerolog.Logger
	lastEventID atomic.Pointer[bson.Raw]
}

func NewProducer(cfg Config, rec kafkaRecorder, logger zerolog.Logger) (*eventProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"client.id":                cfg.ClientID,
		"bootstrap.servers":        cfg.BootstrapServers,
		"enable.idempotence":       cfg.EnableIdempotence,
		"allow.auto.create.topics": true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	logger = logger.With().
		Str("component", "kafka producer").
		Logger()

	ep := &eventProducer{
		p:      p,
		cfg:    cfg,
		rec:    rec,
		logger: logger,
	}

	return ep, nil
}

func (ep *eventProducer) Run(ctx context.Context) error {
	for {
		select {
		case event := <-ep.p.Events():
			if err := ep.handleKafkaEvent(event); err != nil {
				return err
			}
			continue
		default:
		}

		select {
		case event := <-ep.p.Events():
			if err := ep.handleKafkaEvent(event); err != nil {
				return err
			}
		case <-ctx.Done():
			return ep.close()
		}
	}
}

func (ep *eventProducer) close() error {
	ep.logger.Info().
		Msg("flushing outstanding messages")

	ep.p.Flush(int(ep.cfg.CloseTimeout.Milliseconds()))

	ep.p.Close()

	return nil
}

func (ep *eventProducer) handleKafkaEvent(e kafka.Event) error {
	switch ev := e.(type) {
	case *kafka.Message:
		m := ev
		if m.TopicPartition.Error != nil {
			ep.rec.IncKafkaProducerErrorCounter(context.TODO(), *m.TopicPartition.Topic)
			ep.logger.Error().
				Err(m.TopicPartition.Error).
				Msg("message delivery failed")
			// TODO: should we return?
		} else {
			ep.rec.IncKafkaProducerSuccessCounter(context.TODO(), *m.TopicPartition.Topic)
			postEventIDptr := m.Opaque.(*bson.Raw)
			ep.lastEventID.Store(postEventIDptr)
			ep.logger.Info().
				Str("topic", *m.TopicPartition.Topic).
				Int("partition", int(m.TopicPartition.Partition)).
				Int64("offset", int64(m.TopicPartition.Offset)).
				Hex("event_id_hash", entity.HashPostEventID(*postEventIDptr)).
				Msg("message delivered")
		}

	case kafka.Error:
		// Generic client instance-level errors, such as
		// broker connection failures, authentication issues, etc.
		//
		// These errors should generally be considered informational
		// as the underlying client will automatically try to
		// recover from any errors encountered, the application
		// does not need to take action on them.
		//
		// But with idempotence enabled, truly fatal errors can
		// be raised when the idempotence guarantees can't be
		// satisfied, these errors are identified by
		// `e.IsFatal()`.

		e := ev
		if e.IsFatal() {
			// Fatal error handling.
			//
			// When a fatal error is detected by the producer
			// instance, it will emit kafka.Error event (with
			// IsFatal()) set on the Events channel.
			//
			// Note:
			//   After a fatal error has been raised, any
			//   subsequent Produce*() calls will fail with
			//   the original error code.
			return fmt.Errorf("fatal error is detected by the producer instance: %w", e)
		}
		ep.logger.Error().
			Err(e).
			Msg("producer instance error")
	}

	return nil
}
