package mongo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/Karzoug/meower-post-outbox/internal/entity"
)

const (
	dbName                       = "meower"
	collectionPostName           = "posts"
	collectionOutboxName         = "outbox"
	lastEventIDCollectionID      = "last_event_id"
	lastEventIDCollectionKey     = "event_id"
	maxAttemptsSaveStartEventID  = 5
	maxTryGetStartEventIDTimeout = 5 * time.Second
	insertOperation              = "insert"
	replaceOperation             = "replace"
)

type postEventsConsumer struct {
	client   *mongo.Client
	lastID   bson.Raw
	producer producer
	logger   zerolog.Logger
}

func New(client *mongo.Client, producer producer, logger zerolog.Logger) postEventsConsumer {
	logger = logger.With().
		Str("component", "mongo post events consumer").
		Logger()

	return postEventsConsumer{
		client:   client,
		producer: producer,
		logger:   logger,
	}
}

func (er postEventsConsumer) Run(ctx context.Context) error {
	coll := er.client.Database(dbName).Collection(collectionPostName)

	operation := func() error {
		lastPostEventID, err := er.getStartID(ctx)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil
			}
			return fmt.Errorf("get start id error: %w", err)
		}
		er.lastID = lastPostEventID
		return nil
	}

	err := backoff.Retry(operation,
		backoff.NewExponentialBackOff(
			backoff.WithMaxInterval(maxTryGetStartEventIDTimeout)))
	if err != nil {
		return err
	}

	pipeline := mongo.Pipeline{
		{{
			Key: "$match",
			Value: bson.D{{
				Key:   "operationType",
				Value: bson.M{"$in": []string{"insert", "replace"}},
			}},
		}},
		{{
			Key: "$project",
			Value: bson.D{{
				Key:   "operationType",
				Value: 1,
			}, {
				Key:   "fullDocument.author_id",
				Value: 1,
			}, {
				Key:   "fullDocument._id",
				Value: 1,
			}},
		}},
	}

	var opts []options.Lister[options.ChangeStreamOptions]
	if er.lastID != nil {
		opts = append(opts, options.ChangeStream().SetStartAfter(er.lastID))
	}

	cs, err := coll.Watch(ctx, pipeline, opts...)
	if err != nil {
		return err
	}
	defer cs.Close(context.TODO())

	er.logger.Info().
		Str("start id", er.lastID.String()).
		Msg("listen to mongo change events")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for cs.Next(ctx) {
			var mongoEvent mongoEvent
			if err := cs.Decode(&mongoEvent); err != nil {
				return fmt.Errorf("failed to decode event: %w", err)
			}

			postEvent := entity.PostEvent{
				ID:       cs.ResumeToken(),
				AuthorID: mongoEvent.Document.AuthorID,
				PostID:   mongoEvent.Document.ID,
			}

			switch mongoEvent.Operation {
			case insertOperation:
				postEvent.Type = entity.Insert
			case replaceOperation:
				postEvent.Type = entity.Delete
			default:
				continue
			}

			er.logger.Info().
				Str("operation", mongoEvent.Operation).
				Hex("event_id_hash", entity.HashPostEventID(postEvent.ID)).
				Str("postID", postEvent.PostID.String()).
				Str("authorID", postEvent.AuthorID.String()).
				Msg("received event")

			if err := er.producer.Produce(ctx, postEvent); err != nil {
				return fmt.Errorf("failed to produce event: %w", err)
			}
		}
		if err := cs.Err(); !errors.Is(err, context.Canceled) {
			return err
		}

		return nil
	})

	// update last event id, returned only if producer is closed
	eg.Go(func() error {
		t := time.NewTicker(500 * time.Millisecond)

		counter := 0
		for {
			<-t.C

			lastID, closed := er.producer.LastProccessedPostEventID()
			if lastID == nil {
				if closed {
					return nil
				}
				continue
			}

			if bytes.Equal(*lastID, er.lastID) {
				continue
			}

			if err := er.setStartID(ctx, *lastID); err != nil {
				counter++
				er.logger.Error().
					Err(err).
					Int("attempts", counter).
					Str("last event id", lastID.String()).
					Msg("error saving last event id to repo")
				if counter >= maxAttemptsSaveStartEventID {
					return fmt.Errorf("error saving start event id to repo: %w", err)
				}
			}
		}
	})

	return eg.Wait()
}

type mongoEvent struct {
	Operation string `bson:"operationType"`
	Document  struct {
		AuthorID xid.ID `bson:"author_id"`
		ID       xid.ID `bson:"_id"`
	} `bson:"fullDocument"`
}

func (er postEventsConsumer) setStartID(ctx context.Context, eventID bson.Raw) error {
	coll := er.client.Database(dbName).Collection(collectionOutboxName)

	opts := options.Update().SetUpsert(true)
	filter := bson.D{{
		Key:   "_id",
		Value: lastEventIDCollectionID,
	}}
	update := bson.D{{
		Key: "$set",
		Value: bson.M{
			"raw_data": eventID,
		},
	}}

	_, err := coll.UpdateOne(ctx, filter, update, opts)

	return err
}

func (er postEventsConsumer) getStartID(ctx context.Context) (bson.Raw, error) {
	coll := er.client.Database(dbName).Collection(collectionOutboxName)

	filter := bson.D{{
		Key:   "_id",
		Value: lastEventIDCollectionID,
	}}

	var result struct {
		RawData bson.Raw `bson:"raw_data"`
	}
	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.RawData, nil
}
