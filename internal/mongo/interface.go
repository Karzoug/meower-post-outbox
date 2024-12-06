package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/Karzoug/meower-post-outbox/internal/entity"
)

type producer interface {
	Produce(ctx context.Context, event entity.PostEvent) error
	LastProccessedPostEventID() (lastID *bson.Raw, closed bool)
}
