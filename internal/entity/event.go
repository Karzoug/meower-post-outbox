package entity

import (
	"crypto/md5" //nolint:gosec // use only for hash

	"github.com/rs/xid"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type eventType int8

const (
	Insert eventType = 1
	Delete eventType = 3
)

type PostEvent struct {
	ID       bson.Raw
	Type     eventType
	AuthorID xid.ID
	PostID   xid.ID
}

func HashPostEventID(id bson.Raw) []byte {
	hash := md5.Sum(id) //nolint:gosec // use only for hash
	return hash[:]
}
