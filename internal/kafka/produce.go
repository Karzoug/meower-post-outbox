package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	ck "github.com/Karzoug/meower-common-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"

	"github.com/Karzoug/meower-post-outbox/internal/entity"
	postApi "github.com/Karzoug/meower-post-outbox/internal/kafka/gen/post/v1"
)

func (ep *eventProducer) Produce(ctx context.Context, event entity.PostEvent) error {
	var (
		fngpnt string
		val    []byte
		err    error
	)
	switch event.Type {
	case entity.Insert:
		e := &postApi.ChangedEvent{
			Id:         event.PostID.String(),
			AuthorId:   event.AuthorID.String(),
			ChangeType: postApi.ChangeType_CHANGE_TYPE_CREATED,
		}
		fngpnt = ck.MessageTypeHeaderValue(e)
		val, err = proto.Marshal(e)
	case entity.Delete:
		e := &postApi.ChangedEvent{
			Id:         event.PostID.String(),
			AuthorId:   event.AuthorID.String(),
			ChangeType: postApi.ChangeType_CHANGE_TYPE_DELETED,
		}
		fngpnt = ck.MessageTypeHeaderValue(e)
		val, err = proto.Marshal(e)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	key, _ := event.AuthorID.MarshalText()

	for range 5 { // <-- only if producer queue is full

		// Produce message is an asynchronous call, on success it will only
		// enqueue the message on the internal producer queue.
		// The actual delivery attempts to the broker are handled
		// by background threads.
		// Per-message delivery reports are emitted on the Events() channel.
		err = ep.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Headers: []kafka.Header{
				{
					Key:   ck.MessageTypeHeaderKey,
					Value: []byte(fngpnt),
				},
			},
			Opaque: &event.ID,
			Key:    key,
			Value:  val,
		}, nil)
		if nil == err {
			ep.rec.IncKafkaClientProduceCounter(ctx, topicName)
			return nil
		}

		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) {
			if kafkaErr.Code() == kafka.ErrQueueFull {
				time.Sleep(500 * time.Millisecond) // <-- only if producer queue is full
				continue
			}
		}

		return fmt.Errorf("failed to produce message: %w", err)
	}

	return fmt.Errorf("failed to produce message: %w", err)
}

func (ep *eventProducer) LastProccessedPostEventID() (*bson.Raw, bool) {
	return ep.lastEventID.Load(), ep.p.IsClosed()
}
